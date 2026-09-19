package tun

import "testing"

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
	cfg := Config{}
	cfg.ApplyDefaults()
	cfg.Transport.Mode = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid transport mode error")
	}
}
func TestConfigRejectsMissingPeerRoutes(t *testing.T) {
	cfg := Config{Turntf: TurntfConfig{BaseURL: "http://localhost", Credentials: CredentialsConfig{LoginName: "x", Password: PasswordConfig{Source: "plain", Value: "y"}}}, Tun: TunConfig{MTU: 1400}, Peers: []PeerConfig{{Name: "peer", User: UserRefConfig{NodeID: 1, UserID: 2}}}}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected missing routes error")
	}
}
