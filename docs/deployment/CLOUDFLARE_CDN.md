# Cloudflare CDN Setup for VibeSQL Web Demo

This guide explains how to configure Cloudflare as a CDN in front of GitHub Pages to enable Brotli compression for WASM files, reducing the initial load size by ~30%.

## Why Cloudflare?

GitHub Pages automatically serves files with gzip compression, but doesn't support Brotli. For our WASM module:

| Compression | Size | Savings |
|-------------|------|---------|
| Raw | 5.3 MB | - |
| Gzip | 1.5 MB | 72% |
| Brotli | 1.0 MB | 81% |

Brotli saves an additional ~500KB over gzip, improving initial page load time.

## Setup Instructions

### 1. Create Cloudflare Account

1. Sign up at [cloudflare.com](https://dash.cloudflare.com/sign-up) (free tier is sufficient)
2. Add a new site/domain

### 2. Option A: Use Custom Domain (Recommended)

If you have a custom domain (e.g., `vibesql.dev`):

#### DNS Configuration

1. In Cloudflare, add your domain
2. Update nameservers at your registrar to point to Cloudflare
3. Add a CNAME record:
   ```
   Type: CNAME
   Name: @ (or subdomain like 'demo')
   Target: rjwalters.github.io
   Proxy status: Proxied (orange cloud)
   ```

#### GitHub Pages Configuration

1. In the GitHub repo, go to Settings → Pages
2. Add your custom domain under "Custom domain"
3. Enable "Enforce HTTPS"

#### Add CNAME File

Create `web-demo/public/CNAME`:
```
vibesql.dev
```

This file will be copied to the build output and tells GitHub Pages about the custom domain.

### 2. Option B: Proxy GitHub Pages Subdomain

If you want to keep using `rjwalters.github.io/vibesql/`:

1. Create a Cloudflare Worker to proxy requests:

```javascript
// Cloudflare Worker script
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Rewrite to GitHub Pages
    const githubUrl = `https://rjwalters.github.io${url.pathname}${url.search}`;

    // Fetch from origin
    const response = await fetch(githubUrl, {
      headers: request.headers,
      cf: {
        // Enable Brotli compression
        minify: { javascript: false, css: false, html: false },
      }
    });

    // Clone response and add cache headers
    const newResponse = new Response(response.body, response);

    // Cache WASM files aggressively
    if (url.pathname.endsWith('.wasm')) {
      newResponse.headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    }

    return newResponse;
  }
}
```

2. Create a Worker Route to handle `/vibesql/*` requests

### 3. Configure Cloudflare Settings

#### Speed → Optimization

1. **Auto Minify**: Enable for JavaScript, CSS, HTML
2. **Brotli**: Enable (this is the key setting!)
3. **Early Hints**: Enable
4. **Rocket Loader**: Disable (can interfere with WASM loading)

#### Caching → Configuration

1. **Browser Cache TTL**: Respect Existing Headers
2. **Crawler Hints**: Enable

#### Caching → Cache Rules

Create a rule for WASM files:
```
Expression: (http.request.uri.path contains ".wasm")
Cache eligibility: Eligible for cache
Edge TTL: 1 year
Browser TTL: 1 year
```

#### Security → Settings

1. **SSL/TLS**: Full (strict)
2. **Always Use HTTPS**: Enable
3. **Automatic HTTPS Rewrites**: Enable

### 4. Verify Brotli is Working

After setup, verify compression:

```bash
# Check response headers
curl -I -H "Accept-Encoding: br, gzip" https://your-domain.com/vibesql/pkg/vibesql_wasm_bg.wasm

# Should see:
# content-encoding: br
# cf-cache-status: HIT
```

Or use browser DevTools:
1. Open Network tab
2. Load the page
3. Find the `.wasm` request
4. Check "Content-Encoding" header should be `br`

## Performance Monitoring

### Cloudflare Analytics

Monitor in Cloudflare Dashboard → Analytics:
- Cache hit ratio (aim for >90%)
- Bandwidth saved
- Request volume

### Web Vitals

Track impact on Core Web Vitals:
- **LCP** (Largest Contentful Paint): Should improve with smaller WASM
- **TTFB** (Time to First Byte): Should improve with edge caching

## Troubleshooting

### WASM not loading

1. Check CORS headers are preserved
2. Verify `Content-Type: application/wasm` is set
3. Check browser console for errors

### Cache not working

1. Purge Cloudflare cache: Caching → Configuration → Purge Everything
2. Check Cache Rules are applied
3. Verify origin is returning cacheable headers

### SSL/Certificate issues

1. Ensure GitHub Pages has HTTPS enabled
2. Check Cloudflare SSL mode is "Full (strict)"
3. Wait for certificate provisioning (can take up to 24h for new domains)

## Cost Considerations

Cloudflare Free tier includes:
- Unlimited bandwidth
- Brotli compression
- Global CDN
- Basic DDoS protection
- SSL certificates

Pro tier ($20/month) adds:
- Image optimization
- Cache Analytics
- WAF rulesets

For a static WASM demo site, the free tier is sufficient.

## Alternative: Pre-compressed Assets

If Cloudflare isn't feasible, you can pre-compress assets and serve them directly:

1. Add brotli compression to the build:
```bash
# In deploy-pages.yml, after build
brotli -k -q 11 web-demo/dist/pkg/*.wasm
gzip -k -9 web-demo/dist/pkg/*.wasm
```

2. Configure the web server to serve `.wasm.br` when `Accept-Encoding: br` is present

However, GitHub Pages doesn't support this content negotiation, so you'd need to switch to Netlify or Vercel instead.
