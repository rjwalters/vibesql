# Web Demo Testing Guide

This document describes how to test the VibeSQL web demo.

## Manual Testing

**Development server:**
```bash
pnpm run dev
# Open http://localhost:5173/vibesql/
```

**Production build:**
```bash
pnpm run build
pnpm run preview
# Open http://localhost:4173/vibesql/
```

## Unit Tests

Run unit tests with Vitest:

```bash
# Run all tests
pnpm test

# Watch mode
pnpm test:watch

# Coverage report
pnpm test:coverage

# UI mode
pnpm test:ui
```

## Troubleshooting

### Port Already in Use

If the dev server won't start:

```bash
# Kill processes on port 5173
lsof -ti:5173 | xargs kill -9

# Or use a different port
pnpm run dev --port 5174
```

## Best Practices

1. **Add console logs** with prefixes like `[Bootstrap]` for easy filtering
2. **Use the loading progress component** for all async initialization steps
3. **Test both dev and production builds** before deploying

## Resources

- [Vitest Documentation](https://vitest.dev/)
- [Web Demo Source](./src/)
