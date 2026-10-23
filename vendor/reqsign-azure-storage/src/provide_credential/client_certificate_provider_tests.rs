//! Provider-level checks for the aws-lc-rs RS256 patch (VibeSQL #6826).
//!
//! These run the client-certificate provider end to end: file read, then
//! assertion, then a form POST to a mocked token endpoint. The request shape and
//! error kinds match the unpatched, `rsa`-backed 3.1.1. The assertion is
//! re-signed over its captured input and compared byte for byte (RS256 is
//! deterministic). `reqsign-core`'s jwt goldens tie `Rs256Key` to `rsa` 0.9.
use super::*;
use bytes::Bytes;
use reqsign_core::{ErrorKind, HttpSend};
use std::sync::{Arc, Mutex};

fn testdata(name: &str) -> String {
    format!("{}/../testdata/{name}", env!("CARGO_MANIFEST_DIR"))
}

type Seen = (String, String, Option<String>, Vec<(String, String)>);

#[derive(Clone, Debug, Default)]
struct TokenEndpoint {
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl HttpSend for TokenEndpoint {
    async fn http_send(
        &self,
        req: http::Request<Bytes>,
    ) -> Result<http::Response<Bytes>, reqsign_core::Error> {
        let mut form: Vec<(String, String)> = form_urlencoded::parse(req.body())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        form.sort();
        self.seen.lock().unwrap().push((
            req.method().to_string(),
            req.uri().to_string(),
            req.headers()
                .get("Content-Type")
                .map(|v| v.to_str().unwrap().to_string()),
            form,
        ));
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(Bytes::from_static(
                br#"{"access_token":"provider-token","expires_in":3600,"token_type":"Bearer"}"#,
            ))
            .unwrap())
    }
}

async fn provide(
    cert: &str,
    endpoint: &TokenEndpoint,
) -> Result<Option<Credential>, reqsign_core::Error> {
    let ctx = Context::new()
        .with_file_read(reqsign_file_read_tokio::TokioFileRead)
        .with_http_send(endpoint.clone());
    ClientCertificateCredentialProvider::new()
        .with_tenant_id("provider-tenant")
        .with_client_id("provider-client")
        .with_certificate_path(&testdata(cert))
        .provide_credential(&ctx)
        .await
}

#[tokio::test]
async fn provider_posts_a_signed_client_assertion() {
    let endpoint = TokenEndpoint::default();
    let credential = provide("azure-cert-pkcs8.pem", &endpoint).await.unwrap();
    assert!(credential.is_some());

    let seen = endpoint.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let (method, uri, content_type, form) = &seen[0];
    assert_eq!(method, "POST");
    assert_eq!(
        uri,
        "https://login.microsoftonline.com/provider-tenant/oauth2/v2.0/token"
    );
    assert_eq!(
        content_type.as_deref(),
        Some("application/x-www-form-urlencoded")
    );
    let keys: Vec<&str> = form.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(
        keys,
        [
            "client_assertion",
            "client_assertion_type",
            "client_id",
            "grant_type",
            "scope"
        ]
    );

    let assertion = &form[0].1;
    let segments: Vec<&str> = assertion.split('.').collect();
    assert_eq!(segments.len(), 3);
    let key = Rs256Key::from_pkcs8_pem(&std::fs::read(testdata("k2048.pem")).unwrap()).unwrap();
    let expected = key
        .sign(format!("{}.{}", segments[0], segments[1]).as_bytes())
        .unwrap();
    assert_eq!(segments[2], URL_SAFE_NO_PAD.encode(expected));
}

/// A PKCS#1 block keeps the unpatched kind and message prefix; only the detail
/// after the prefix comes from the new parser. A 1024-bit key, which `rsa` 0.9
/// signed with, is the accepted narrowing: it now fails the same way, and
/// no token request is made.
#[tokio::test]
async fn rejected_keys_keep_the_error_kind_and_prefix() {
    for cert in ["azure-cert-pkcs1.pem", "azure-cert-1024.pem"] {
        let endpoint = TokenEndpoint::default();
        let err = provide(cert, &endpoint).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CredentialInvalid, "{cert}: {err}");
        assert_eq!(
            err.to_string(),
            "Failed to parse private key from PEM: failed to parse PKCS#8 RSA private key PEM",
            "{cert}"
        );
        assert!(endpoint.seen.lock().unwrap().is_empty(), "{cert}");
    }
}
