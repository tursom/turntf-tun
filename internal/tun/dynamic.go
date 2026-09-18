package tun

import (
	"context"
	"encoding/json"
	"errors"
	turntf "github.com/tursom/turntf-go"
	"net/http"
	"net/url"
	"time"
)

type dynamicLeaseValue struct {
	NodeID    int64     `json:"node_id"`
	UserID    int64     `json:"user_id"`
	Address   string    `json:"address"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (c *kvHTTPClient) listLeaseValues(ctx context.Context, database string) (map[string]struct {
	Value       []byte `json:"value"`
	ModRevision uint64 `json:"mod_revision"`
}, error) {
	var out kvListResponse
	err := c.do(ctx, http.MethodGet, "/kv/"+url.PathEscape(database)+"/list?prefix=leases/by-ip/", nil, &out)
	return out.Items, err
}
func (c *kvHTTPClient) releaseLease(ctx context.Context, database string, lease overlayLease) error {
	if lease.Key == "" {
		return nil
	}
	entry, err := c.get(ctx, database, lease.Key)
	if err != nil {
		return err
	}
	var out kvTxnResponse
	err = c.do(ctx, http.MethodPost, "/kv/"+url.PathEscape(database)+"/txn", map[string]any{"compare": []map[string]any{{"key": lease.Key, "revision": entry.Entry.ModRevision}}, "deletes": []string{lease.Key}}, &out)
	if err != nil {
		return err
	}
	if !out.Succeeded {
		return errors.New("lease release compare failed")
	}
	return nil
}
func discoverOverlayPeers(ctx context.Context, c *kvHTTPClient, cfg OverlayConfig, self turntf.UserRef, existing []PeerConfig) ([]PeerConfig, error) {
	items, err := c.listLeaseValues(ctx, cfg.Database)
	if err != nil {
		return nil, err
	}
	seen := map[turntf.UserRef]bool{}
	for _, p := range existing {
		seen[p.User.ToTurntf()] = true
	}
	out := append([]PeerConfig(nil), existing...)
	now := time.Now().UTC()
	for _, item := range items {
		var v dynamicLeaseValue
		if json.Unmarshal(item.Value, &v) != nil || v.NodeID <= 0 || v.UserID <= 0 || v.Address == "" || v.ExpiresAt.Before(now) || (v.NodeID == self.NodeID && v.UserID == self.UserID) {
			continue
		}
		u := turntf.UserRef{NodeID: v.NodeID, UserID: v.UserID}
		if seen[u] {
			continue
		}
		seen[u] = true
		out = append(out, PeerConfig{Name: v.Address, User: UserRefConfig{NodeID: v.NodeID, UserID: v.UserID}, Routes: []string{v.Address + "/32"}, DialPolicy: "auto"})
	}
	return out, nil
}
