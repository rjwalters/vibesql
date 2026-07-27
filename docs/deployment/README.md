# Deployment Documentation

This directory contains guides for deploying and configuring the VibeSQL web demo.

## Current Setup

The web demo is deployed to Cloudflare Pages at:
- **URL**: https://vibesql.org/
- **Config**: `web-demo/wrangler.toml` (project `vibesql`, output dir `dist`)
- **Trigger**: Manual — run `wrangler deploy` from `web-demo/` on the `main` branch.
  There is no automatic deploy workflow (see `.github/workflows/ci-main.yml`).

## Guides

- [Cloudflare CDN Setup](./CLOUDFLARE_CDN.md) - Enable Brotli compression via Cloudflare CDN

## Compression Comparison

| Format | WASM Size | Savings |
|--------|-----------|---------|
| Raw | 5.3 MB | - |
| Gzip | 1.5 MB | 72% |
| Brotli (Cloudflare default) | 1.0 MB | 81% |

## Quick Reference

### Manual Redeploy

```bash
# Build and deploy to Cloudflare Pages
cd web-demo && pnpm build && wrangler deploy
```

### Check Deployment Status

```bash
# List recent Cloudflare Pages deployments
cd web-demo && wrangler pages deployment list
```

### Verify Compression

```bash
# Check current compression (should show br)
curl -I -H "Accept-Encoding: br, gzip" https://vibesql.org/pkg/vibesql_wasm_bg.wasm 2>/dev/null | grep -i content-encoding
```
