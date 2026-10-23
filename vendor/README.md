# Vendored, patched crates

These crates are applied through `[patch.crates-io]` in the root `Cargo.toml`.
They are excluded from the workspace. Drop each one once upstream ships an
equivalent release.

## reqsign-core, reqsign-google, reqsign-azure-storage (#6826)

**Why:** `rsa` 0.9 has RUSTSEC-2023-0071 (Marvin timing side channel), and no
release fixes it. reqsign uses `rsa` for one operation: RS256 (RSASSA-PKCS1-v1_5
with SHA-256) signing with a PKCS#8 PEM private key. That covers Google
service-account JWTs and GCS presigned URLs, and Azure client-certificate
assertions. VibeSQL reaches it through opendal's `services-gcs` and
`services-azblob` (`storage-all`).

**Change:** the patched crates sign with `aws-lc-rs`, which is already in the
graph through rustls. The lockfile loses `rsa` and 20 RustCrypto support crates,
and gains one new crate, `untrusted 0.7.1`, which the signing API of the
already-present `aws-lc-rs` needs. `reqsign-core::jwt` gains `Rs256Key`, a wrapper around
`aws_lc_rs::signature::RsaKeyPair`:

- It decodes the PEM with the same `pem-rfc7468` decoder `rsa` used and checks
  the `PRIVATE KEY` label.
- The DER is held in `Zeroizing`.
- `Debug` shows only the key size.

`encode_rs256` takes `&Rs256Key`; `encode_rs256_pem` is unchanged.

**Error layers** (each unchanged from the unpatched crates):

| Path | Bad key error kind | Message |
|---|---|---|
| `reqsign-core` `encode_rs256_pem` / `Rs256Key` | `CredentialInvalid` | `failed to parse PKCS#8 RSA private key PEM` |
| Google service account → OAuth token exchange | `CredentialInvalid` (core's error passes through) | same as core |
| Google GCS presign (`sign_with_service_account`) | `Unexpected` (the crate wraps the parse failure) | `failed to parse private key` |
| Azure client certificate | `CredentialInvalid` | `Failed to parse private key from PEM: …` |

Only the Azure detail after `Failed to parse private key from PEM:` changes; it
now comes from aws-lc instead of the `rsa` crate's parser.

**Behaviour change, accepted on #6826:** RSA keys below 2048 bits or above 8192
bits now fail with `credential_invalid` instead of signing. For keys from 2048
to 8192 bits, signatures are byte-identical to `rsa` 0.9, tested with public
exponents 3, 17, 65537 and 2^32+1.

| Crate | Upstream version | Upstream git sha | Files changed |
|---|---|---|---|
| reqsign-core | 3.3.1 | `6818b17e` | `Cargo.toml`, `src/jwt.rs` |
| reqsign-google | 3.0.3 | `b1f72c17` | `Cargo.toml`, `src/sign_request.rs`, new `src/sign_request_provider_tests.rs` |
| reqsign-azure-storage | 3.1.1 | `b1f72c17` | `Cargo.toml`, `src/provide_credential/client_certificate.rs`, new `src/provide_credential/client_certificate_provider_tests.rs` |

Everything else in each directory is the published `.crate` content. Each
crate's `.crate` sha256 matches the crates.io index checksum. The registry's
`.cargo-ok`, `Cargo.lock` and `Cargo.toml.orig` files are left out. Also
omitted is upstream's unused fixture `reqsign-google/testdata/testbucket_credential.json`:
it is a fake service-account key that nothing references, and GitHub push
protection blocks it.

`testdata/` holds throwaway test keys and certificates, generated only for these
tests, plus golden outputs from the **unpatched** crates.

**Tests.** `vendor/Cargo.toml` is a test-only workspace for these three crates,
with its own `vendor/Cargo.lock`. That lock pins the dev-dependencies
`reqsign-http-send-reqwest` and `reqsign-command-execute-tokio` to versions that
resolve to the patched `reqsign-core` 3.3.1, so there is one reqsign-core and no
`rsa`. CI (`storage-features-check`) runs:

    cargo test --manifest-path vendor/Cargo.toml --workspace --lib --all-features --locked

That runs the crates' own unit tests plus the patch's tests:

- `reqsign-core/src/jwt.rs`: JWTs byte-identical to goldens from the `rsa`-backed
  crate. Also covers the same rejections the `rsa` crate made, the e=3 key, and
  the <2048-bit refusal.
- `reqsign-google/src/sign_request.rs`: GCS presign signatures byte-identical to
  the `rsa`-backed goldens.
- `reqsign-google/src/sign_request_provider_tests.rs`: presign and token
  exchange through the real signer with a mocked token endpoint. Checks the
  request shape and claims. Every captured signature is re-signed over its
  captured input and must match byte for byte. Also checks both error layers.
- `reqsign-azure-storage/src/provide_credential/client_certificate{,_provider_tests}.rs`:
  the provider end to end, from certificate file to form POST, with the
  assertion re-signed and compared. PKCS#1 and 1024-bit keys are rejected with
  no token request.

Integration tests and examples (`--lib` excludes them) need live cloud
credentials. Without credentials they return early, so they would exercise
nothing.

**Keeping this alive:** CI (`storage-features-check`) also fails if `rsa` reappears in
`Cargo.lock`. That happens when a reqsign bump no longer matches these versions
and the patch silently stops applying. In that case, re-apply the same change to
the new versions, or drop the patch once upstream has fixed the advisory
([apache/reqsign#437](https://github.com/apache/reqsign/issues/437)).
