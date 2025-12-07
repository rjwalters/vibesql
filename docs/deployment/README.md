# Deployment Documentation

This directory contains guides for deploying and configuring the VibeSQL web demo.

## Current Setup

The web demo is deployed to GitHub Pages at:
- **URL**: https://rjwalters.github.io/vibesql/
- **Workflow**: `.github/workflows/deploy-pages.yml`
- **Trigger**: Automatic on push to `main`, or manual dispatch

## Guides

- [Cloudflare CDN Setup](./CLOUDFLARE_CDN.md) - Enable Brotli compression via Cloudflare CDN

## Compression Comparison

| Format | WASM Size | Savings |
|--------|-----------|---------|
| Raw | 5.3 MB | - |
| Gzip (GitHub Pages default) | 1.5 MB | 72% |
| Brotli (with Cloudflare) | 1.0 MB | 81% |

## Quick Reference

### Manual Redeploy

```bash
# Trigger manual deployment via GitHub CLI
gh workflow run deploy-pages.yml
```

### Check Deployment Status

```bash
# View recent deployments
gh run list --workflow=deploy-pages.yml
```

### Verify Compression

```bash
# Check current compression (should show gzip or br)
curl -I -H "Accept-Encoding: br, gzip" https://rjwalters.github.io/vibesql/pkg/vibesql_wasm_bg.wasm 2>/dev/null | grep -i content-encoding
```
