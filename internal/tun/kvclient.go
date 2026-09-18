package tun

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	turntf "github.com/tursom/turntf-go"
)

type kvHTTPClient struct {
	base  string
	http  *http.Client
	token string
}
type kvEntryResponse struct {
	Revision uint64 `json:"revision"`
	Entry    struct {
		Value       []byte `json:"value"`
		ModRevision uint64 `json:"mod_revision"`
	} `json:"entry"`
}
type kvListResponse struct {
	Items map[string]struct {
		Value       []byte `json:"value"`
		ModRevision uint64 `json:"mod_revision"`
	} `json:"items"`
}
type kvTxnResponse struct {
	Revision  uint64 `json:"Revision"`
	Succeeded bool   `json:"Succeeded"`
}

func newKVHTTPClient(cfg TurntfConfig) *kvHTTPClient {
	return &kvHTTPClient{base: strings.TrimRight(cfg.BaseURL, "/"), http: &http.Client{Timeout: cfg.RequestTimeout.Duration}}
}
func (c *kvHTTPClient) login(ctx context.Context, creds CredentialsConfig) error {
	body, _ := json.Marshal(map[string]any{"login_name": creds.LoginName, "password": creds.Password.Value})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("kv login: %s", resp.Status)
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	c.token = out.Token
	return nil
}
func (c *kvHTTPClient) do(ctx context.Context, method, path string, in any, out any) error {
	var body *bytes.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	} else {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == 404 {
			return kvNotFound
		}
		return fmt.Errorf("kv api: %s", resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

var kvNotFound = errors.New("kv key not found")

func (c *kvHTTPClient) get(ctx context.Context, db, key string) (kvEntryResponse, error) {
	var out kvEntryResponse
	err := c.do(ctx, http.MethodGet, "/kv/"+url.PathEscape(db)+"/keys/"+url.PathEscape(key), nil, &out)
	return out, err
}
func (c *kvHTTPClient) txn(ctx context.Context, db string, cmp []map[string]any, puts []map[string]any) error {
	var out kvTxnResponse
	err := c.do(ctx, http.MethodPost, "/kv/"+url.PathEscape(db)+"/txn", map[string]any{"compare": cmp, "puts": puts}, &out)
	if err != nil {
		return err
	}
	if !out.Succeeded {
		return errors.New("kv transaction compare failed")
	}
	return nil
}
func (c *kvHTTPClient) acquire(ctx context.Context, cfg OverlayConfig, node turntf.UserRef) (string, error) {
	if cfg.StaticAddress != "" {
		return cfg.StaticAddress, nil
	}
	start, err := parseIPv4(cfg.PoolStart)
	if err != nil {
		return "", err
	}
	end, err := parseIPv4(cfg.PoolEnd)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	expires := now.Add(cfg.LeaseDuration.Duration).Format(time.RFC3339)
	for ip := start; ip <= end; ip++ {
		key := fmt.Sprintf("leases/by-ip/%d.%d.%d.%d", byte(ip>>24), byte(ip>>16), byte(ip>>8), byte(ip))
		entry, err := c.get(ctx, cfg.Database, key)
		cmp := []map[string]any{{"key": key, "exists": false}}
		if err == nil {
			var lease struct {
				ExpiresAt time.Time `json:"expires_at"`
			}
			if json.Unmarshal(entry.Entry.Value, &lease) == nil && lease.ExpiresAt.After(now) {
				continue
			}
			cmp = []map[string]any{{"key": key, "revision": entry.Entry.ModRevision}}
		} else if !errors.Is(err, kvNotFound) {
			return "", err
		}
		value, _ := json.Marshal(map[string]any{"node_id": node.NodeID, "user_id": node.UserID, "address": key[len("leases/by-ip/"):], "expires_at": expires})
		if c.txn(ctx, cfg.Database, cmp, []map[string]any{{"key": key, "value": value}}) == nil {
			return key[len("leases/by-ip/"):] + "/32", nil
		}
	}
	return "", errors.New("kv address pool exhausted")
}
func parseIPv4(raw string) (uint32, error) {
	var a, b, c, d int
	if _, err := fmt.Sscanf(raw, "%d.%d.%d.%d", &a, &b, &c, &d); err != nil || a < 1 || a > 254 || b < 0 || b > 255 || c < 0 || c > 255 || d < 1 || d > 254 {
		return 0, fmt.Errorf("invalid IPv4 pool address %q", raw)
	}
	return uint32(a)<<24 | uint32(b)<<16 | uint32(c)<<8 | uint32(d), nil
}
