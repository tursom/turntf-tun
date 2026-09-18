package tun

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	turntf "github.com/tursom/turntf-go"
	"gopkg.in/yaml.v3"
)

const ExampleConfig = `turntf:
  base_url: "http://127.0.0.1:8080"
  request_timeout: "10s"
  ping_interval: "30s"
  credentials:
    login_name: "tun-kr.login"
    password:
      source: "plain"
      value: "tun-password"

tun:
  name: "turntf0"
  mtu: 1400
  addresses:
    - "10.250.0.1/32"
  bring_up: true

transport:
  send_queue_size: 4096
  max_packet_bytes: 65535
  dial_retry_interval: "3s"

peers:
  - name: "cc"
    user:
      node_id: 4096
      user_id: 1026
    routes:
      - "10.250.0.2/32"
    dial_policy: "auto"
  - name: "kiwi"
    user:
      node_id: 4096
      user_id: 1027
    routes:
      - "10.250.0.3/32"
    dial_policy: "auto"
`

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return errors.New("duration must be a scalar")
	}
	if strings.TrimSpace(value.Value) == "" {
		d.Duration = 0
		return nil
	}
	parsed, err := time.ParseDuration(value.Value)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type Config struct {
	Turntf    TurntfConfig    `yaml:"turntf"`
	Tun       TunConfig       `yaml:"tun"`
	Transport TransportConfig `yaml:"transport"`
	Overlay   OverlayConfig   `yaml:"overlay"`
	Peers     []PeerConfig    `yaml:"peers"`
}
type TurntfConfig struct {
	BaseURL        string            `yaml:"base_url"`
	Credentials    CredentialsConfig `yaml:"credentials"`
	RequestTimeout Duration          `yaml:"request_timeout"`
	PingInterval   Duration          `yaml:"ping_interval"`
}
type OverlayConfig struct {
	Enabled       bool     `yaml:"enabled"`
	Database      string   `yaml:"database"`
	StaticAddress string   `yaml:"static_address"`
	PoolStart     string   `yaml:"pool_start"`
	PoolEnd       string   `yaml:"pool_end"`
	LeaseDuration Duration `yaml:"lease_duration"`
}
type CredentialsConfig struct {
	NodeID    int64          `yaml:"node_id"`
	UserID    int64          `yaml:"user_id"`
	LoginName string         `yaml:"login_name"`
	Password  PasswordConfig `yaml:"password"`
}
type PasswordConfig struct {
	Source string `yaml:"source"`
	Value  string `yaml:"value"`
}
type TunConfig struct {
	Name      string   `yaml:"name"`
	MTU       int      `yaml:"mtu"`
	Addresses []string `yaml:"addresses"`
	BringUp   bool     `yaml:"bring_up"`
}
type TransportConfig struct {
	SendQueueSize     int      `yaml:"send_queue_size"`
	MaxPacketBytes    int      `yaml:"max_packet_bytes"`
	DialRetryInterval Duration `yaml:"dial_retry_interval"`
}
type PeerConfig struct {
	Name       string        `yaml:"name"`
	User       UserRefConfig `yaml:"user"`
	Routes     []string      `yaml:"routes"`
	DialPolicy string        `yaml:"dial_policy"`
}
type UserRefConfig struct {
	NodeID int64 `yaml:"node_id"`
	UserID int64 `yaml:"user_id"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Config{}, err
	}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}
func (c *Config) ApplyDefaults() {
	if c.Turntf.RequestTimeout.Duration == 0 {
		c.Turntf.RequestTimeout.Duration = 10 * time.Second
	}
	if c.Turntf.PingInterval.Duration == 0 {
		c.Turntf.PingInterval.Duration = 30 * time.Second
	}
	if c.Overlay.LeaseDuration.Duration == 0 {
		c.Overlay.LeaseDuration.Duration = 10 * time.Minute
	}
	if c.Tun.Name == "" {
		c.Tun.Name = "turntf0"
	}
	if c.Tun.MTU == 0 {
		c.Tun.MTU = 1400
	}
	if c.Transport.SendQueueSize == 0 {
		c.Transport.SendQueueSize = 4096
	}
	if c.Transport.MaxPacketBytes == 0 {
		c.Transport.MaxPacketBytes = 65535
	}
	if c.Transport.DialRetryInterval.Duration == 0 {
		c.Transport.DialRetryInterval.Duration = 3 * time.Second
	}
	for i := range c.Peers {
		if c.Peers[i].DialPolicy == "" {
			c.Peers[i].DialPolicy = "auto"
		}
	}
}
func (c Config) Validate() error {
	if strings.TrimSpace(c.Turntf.BaseURL) == "" {
		return errors.New("turntf.base_url is required")
	}
	if _, err := c.Turntf.Credentials.ToTurntf(); err != nil {
		return fmt.Errorf("turntf.credentials: %w", err)
	}
	if c.Overlay.Enabled {
		if strings.TrimSpace(c.Overlay.Database) == "" {
			return errors.New("overlay.database is required")
		}
		if c.Overlay.StaticAddress == "" && (c.Overlay.PoolStart == "" || c.Overlay.PoolEnd == "") {
			return errors.New("overlay requires static_address or pool_start/pool_end")
		}
		if c.Overlay.LeaseDuration.Duration < time.Minute {
			return errors.New("overlay.lease_duration must be at least 1m")
		}
	}

	if c.Tun.MTU < 576 || c.Tun.MTU > 65535 {
		return errors.New("tun.mtu must be between 576 and 65535")
	}
	if c.Transport.SendQueueSize < 1 {
		return errors.New("transport.send_queue_size must be positive")
	}
	if c.Transport.MaxPacketBytes < c.Tun.MTU || c.Transport.MaxPacketBytes > 65535 {
		return errors.New("transport.max_packet_bytes must cover mtu and be at most 65535")
	}
	if c.Transport.DialRetryInterval.Duration <= 0 {
		return errors.New("transport.dial_retry_interval must be positive")
	}
	for _, a := range c.Tun.Addresses {
		if _, _, err := net.ParseCIDR(a); err != nil {
			return fmt.Errorf("tun.addresses %q: %w", a, err)
		}
	}
	seen := map[turntf.UserRef]bool{}
	for i, p := range c.Peers {
		if strings.TrimSpace(p.Name) == "" {
			return fmt.Errorf("peers[%d].name is required", i)
		}
		u := p.User.ToTurntf()
		if u.NodeID == 0 || u.UserID == 0 {
			return fmt.Errorf("peers[%d].user is required", i)
		}
		if seen[u] {
			return fmt.Errorf("duplicate peer user %d:%d", u.NodeID, u.UserID)
		}
		seen[u] = true
		if p.DialPolicy != "auto" && p.DialPolicy != "always" && p.DialPolicy != "never" {
			return fmt.Errorf("peers[%d].dial_policy must be auto, always or never", i)
		}
		if len(p.Routes) == 0 {
			return fmt.Errorf("peers[%d].routes must not be empty", i)
		}
		for _, r := range p.Routes {
			if _, n, err := net.ParseCIDR(r); err != nil || n == nil {
				return fmt.Errorf("peers[%d].routes contains invalid CIDR %q", i, r)
			}
		}
	}
	return nil
}
func (c CredentialsConfig) ToTurntf() (turntf.Credentials, error) {
	p, err := c.Password.ToTurntf()
	if err != nil {
		return turntf.Credentials{}, err
	}
	out := turntf.Credentials{NodeID: c.NodeID, UserID: c.UserID, LoginName: strings.TrimSpace(c.LoginName), Password: p}
	if err := out.Password.Validate(); err != nil {
		return turntf.Credentials{}, err
	}
	byID := out.NodeID != 0 || out.UserID != 0
	byName := out.LoginName != ""
	if byID == byName {
		return turntf.Credentials{}, errors.New("set exactly one of node_id/user_id or login_name")
	}
	if byID && (out.NodeID == 0 || out.UserID == 0) {
		return turntf.Credentials{}, errors.New("node_id and user_id must be set together")
	}
	return out, nil
}
func (p PasswordConfig) ToTurntf() (turntf.PasswordInput, error) {
	switch p.Source {
	case "plain":
		return turntf.PlainPassword(p.Value)
	case "hashed":
		if strings.TrimSpace(p.Value) == "" {
			return turntf.PasswordInput{}, errors.New("password.value is required")
		}
		return turntf.HashedPassword(p.Value), nil
	default:
		return turntf.PasswordInput{}, errors.New("password.source must be plain or hashed")
	}
}
func (u UserRefConfig) ToTurntf() turntf.UserRef {
	return turntf.UserRef{NodeID: u.NodeID, UserID: u.UserID}
}
