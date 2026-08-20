# Packages

TypeScript/JavaScript packages published to npm. The workspace uses **pnpm**
(see `pnpm-workspace.yaml` at the repo root) — run `pnpm install` from the
repo root, then build within each package.

| Directory | npm package | Description |
|-----------|-------------|-------------|
| [vibesql-client/](vibesql-client/) | `@vibesql/client` (0.1.0) | TypeScript SDK for VibeSQL with real-time subscription support |
| [vibesql-client-ts/](vibesql-client-ts/) | `@vibesql/client` (0.1.3) | TypeScript/JavaScript client library for VibeSQL with real-time subscription support |
| [vibesql-drizzle/](vibesql-drizzle/) | `@vibesql/drizzle` (0.1.3) | Drizzle ORM adapter for VibeSQL using the sqlite-proxy driver |

Note: `vibesql-client/` and `vibesql-client-ts/` both declare the npm name
`@vibesql/client`; `vibesql-client-ts/` carries the newer version.

See each package's own README for installation and usage details.
