# vibesql-l10n

Localization support for VibeSQL using [Project Fluent](https://projectfluent.org/).

## Overview

This crate provides internationalization (i18n) and localization (l10n) support for the VibeSQL CLI and related tools. Translation catalogs are authored in Fluent (`.ftl`) files, embedded into the binary at compile time, and resolved at runtime with system locale detection and English fallback.

## Features

- **Fluent message catalogs**: translations authored in `.ftl` files and embedded via `rust-embed`
- **Locale detection**: detects the system locale from the environment, with explicit override support
- **Simple API**: `init()`, `format()`, and the `vibe_msg!` macro for messages with arguments
- **Translation tooling**: `check-ftl` and `check-translations` binaries validate catalog syntax and cross-locale coverage

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
vibesql-l10n = "0.2"
```

Basic example:

```rust
use vibesql_l10n::{init, format, vibe_msg};

// Initialize with system locale detection...
init(None).unwrap();
// ...or specify a locale explicitly
init(Some("es")).unwrap();

// Format a simple message
let banner = format("cli-banner", None);

// Format a message with arguments using the macro
let rows = vibe_msg!("rows-with-time", count = 42, time = "0.123");
```

## Documentation

- [API Documentation](https://docs.rs/vibesql-l10n)
- [Main VibeSQL Repository](https://github.com/rjwalters/vibesql)

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))

at your option.
