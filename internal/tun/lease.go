package tun

import (
	"context"
	turntf "github.com/tursom/turntf-go"
	"time"
)

func renewOverlayLease(ctx context.Context, client *kvHTTPClient, cfg OverlayConfig, lease overlayLease, node turntf.UserRef) {
	interval := cfg.LeaseDuration.Duration / 3
	if interval < time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next, err := client.renew(ctx, cfg, lease, node)
			if err == nil {
				lease = next
			}
		}
	}
}
