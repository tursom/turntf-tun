package tun

import (
	"strings"
	"testing"
)

func TestConfigDefaultsToHighThroughputTunSettings(t *testing.T) {
	cfg := Config{}
	cfg.ApplyDefaults()
	if cfg.Transport.Mode != "auto" {
		t.Fatalf("transport mode default: %q", cfg.Transport.Mode)
	}
	if cfg.Tun.Name != "turntf0" || cfg.Tun.MTU != 1400 {
		t.Fatalf("tun defaults: %+v", cfg.Tun)
	}
	if cfg.Transport.SendQueueSize != 4096 || cfg.Transport.MaxPacketBytes != 65535 {
		t.Fatalf("transport defaults: %+v", cfg.Transport)
	}
}

func TestConfigRejectsUnknownTransportMode(t *testing.T) {
	cfg := validTestConfig()
	cfg.Transport.Mode = "unknown"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "transport.mode") {
		t.Fatalf("Validate() error = %v, want transport.mode error", err)
	}
}

func TestConfigRejectsUnknownPeerTransportMode(t *testing.T) {
	cfg := validTestConfig()
	cfg.Peers[0].TransportMode = "unknown"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "peers[0].transport_mode") {
		t.Fatalf("Validate() error = %v, want peer transport_mode error", err)
	}
}

func TestPeerTransportModeInheritanceAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		name       string
		globalMode string
		peerMode   string
		want       string
	}{
		{name: "empty inherits stream", globalMode: "stream", want: "stream"},
		{name: "empty inherits relay", globalMode: "relay", want: "relay"},
		{name: "stream override", globalMode: "relay", peerMode: "stream", want: "stream"},
		{name: "relay override", globalMode: "stream", peerMode: "relay", want: "relay"},
		{name: "auto override", globalMode: "stream", peerMode: "auto", want: "auto"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := PeerConfig{TransportMode: tc.peerMode}
			if got := peer.effectiveTransportMode(tc.globalMode); got != tc.want {
				t.Fatalf("effectiveTransportMode(%q) = %q, want %q", tc.globalMode, got, tc.want)
			}
		})
	}
}

func TestConfigRejectsMissingPeerRoutes(t *testing.T) {
	cfg := validTestConfig()
	cfg.Peers[0].Routes = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing routes error")
	}
}

func validTestConfig() Config {
	cfg := Config{
		Turntf: TurntfConfig{
			BaseURL: "http://localhost",
			Credentials: CredentialsConfig{
				LoginName: "x",
				Password:  PasswordConfig{Source: "plain", Value: "y"},
			},
		},
		Tun: TunConfig{MTU: 1400},
		Peers: []PeerConfig{{
			Name:   "peer",
			User:   UserRefConfig{NodeID: 1, UserID: 2},
			Routes: []string{"10.0.0.2/32"},
		}},
	}
	cfg.ApplyDefaults()
	return cfg
}
