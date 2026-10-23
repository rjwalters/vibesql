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
graph through rustls. `reqsign-core::jwt` gains `Rs256Key`, a wrapper around
`aws_lc_rs::signature::RsaKeyPair`:

- It decodes the PEM with the same `pem-rfc7468` decoder `rsa` used and checks
  the `PRIVATE KEY` label.
- The DER is held in `Zeroizing`.
- `Debug` shows only the key size.

`encode_rs256` takes `&Rs256Key`; `encode_rs256_pem` is unchanged. Google and
Azure keep their error kinds and message prefixes.

**Behaviour change, accepted on #6826:** RSA keys below 2048 bits or above 8192
bits now fail with `credential_invalid` instead of signing. For keys from 2048
to 8192 bits, signatures are byte-identical to `rsa` 0.9, tested with public
exponents 3, 17, 65537 and 2^32+1.

| Crate | Upstream version | Upstream git sha | Files changed |
|---|---|---|---|
| reqsign-core | 3.3.1 | `6818b17e` | `Cargo.toml`, `src/jwt.rs` |
| reqsign-google | 3.0.3 | `b1f72c17` | `Cargo.toml`, `src/sign_request.rs` |
| reqsign-azure-storage | 3.1.1 | `b1f72c17` | `Cargo.toml`, `src/provide_credential/client_certificate.rs` |

Everything else in each directory is the published `.crate` content. Each
crate's `.crate` sha256 matches the crates.io index checksum. The registry's
`.cargo-ok`, `Cargo.lock` and `Cargo.toml.orig` files are left out. Also
omitted is upstream's unused fixture `reqsign-google/testdata/testbucket_credential.json`:
it is a fake service-account key that nothing references, and GitHub push
protection blocks it.

`testdata/` holds throwaway test keys and certificates, generated only for these
tests, plus golden outputs from the **unpatched** crates. The new tests in
`jwt.rs`, `sign_request.rs` and `client_certificate.rs` check the patched output
byte for byte against those goldens. These crates are not workspace members,
so VibeSQL CI does not run their tests. They were run in a scratch workspace,
with results on #6826:

- `reqsign-core` here: `cargo test -p reqsign-core --all-features --lib`,
  46 passed.
- All three crates together: 174 passed (control: 166). In that run, core came
  from 3.3.2 with the identical `jwt.rs`, because the google and azure examples
  and dev-dependencies (`reqsign-http-send-reqwest`) require reqsign-core 3.3.2.

The upstream live-credential integration tests return early without
credentials, so their "pass" exercises nothing.

**Keeping this alive:** CI (`storage-features-check`) fails if `rsa` reappears in
`Cargo.lock`. That happens when a reqsign bump no longer matches these versions
and the patch silently stops applying. In that case, re-apply the same change to
the new versions, or drop the patch once upstream has fixed the advisory
([apache/reqsign#437](https://github.com/apache/reqsign/issues/437)).
