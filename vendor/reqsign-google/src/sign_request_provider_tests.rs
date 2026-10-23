//! Provider-level checks for the aws-lc-rs RS256 patch (VibeSQL #6826).
//!
//! These drive the real signing paths with a mocked token endpoint. The
//! expected request shape and error kinds were recorded from the unpatched,
//! `rsa`-backed 3.0.3. RS256 is deterministic, so every captured signature is
//! re-signed over its captured input and compared byte for byte. That shows the
//! provider signed exactly that input with that key. `reqsign-core`'s jwt
//! goldens tie `Rs256Key` itself to `rsa` 0.9 output.
use super::*;
use bytes::Bytes;
use reqsign_core::jwt::Rs256Key;
use reqsign_core::{ErrorKind, HttpSend};
use std::sync::{Arc, Mutex};

fn testdata(name: &str) -> String {
    std::fs::read_to_string(format!("{}/../testdata/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
}

/// JWS segments are unpadded base64url; core's decoder takes standard base64.
fn b64url(segment: &str) -> Vec<u8> {
    let mut s = segment.replace('-', "+").replace('_', "/");
    while s.len() % 4 != 0 {
        s.push('=');
    }
    reqsign_core::hash::base64_decode(&s).unwrap()
}

fn rs256(pem: &str, message: &[u8]) -> Vec<u8> {
    Rs256Key::from_pkcs8_pem(pem.as_bytes()).unwrap().sign(message).unwrap()
}

#[derive(Clone, Debug, Default)]
struct TokenEndpoint {
    seen: Arc<Mutex<Vec<(String, String, Option<String>, String)>>>,
}

impl HttpSend for TokenEndpoint {
    async fn http_send(&self, req: http::Request<Bytes>) -> Result<http::Response<Bytes>> {
        let content_type = req
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_string());
        self.seen.lock().unwrap().push((
            req.method().to_string(),
            req.uri().to_string(),
            content_type,
            String::from_utf8(req.body().to_vec()).unwrap(),
        ));
        Ok(http::Response::builder()
            .status(http::StatusCode::OK)
            .body(Bytes::from_static(
                br#"{"access_token":"provider-token","expires_in":3600,"token_type":"Bearer"}"#,
            ))
            .unwrap())
    }
}

fn parts(uri: &str) -> http::request::Parts {
    http::Request::builder()
        .method(http::Method::GET)
        .uri(uri)
        .body(Bytes::new())
        .unwrap()
        .into_parts()
        .0
}

fn service_account(key: &str) -> ServiceAccount {
    ServiceAccount {
        private_key: testdata(key),
        client_email: "provider@example.iam.gserviceaccount.com".to_string(),
    }
}

const URI: &str = "https://storage.googleapis.com/bucket/obj%2Fx?a=1&b=%20";

fn signer() -> RequestSigner {
    RequestSigner::new("storage").with_scope("https://www.googleapis.com/auth/devstorage.read_write")
}

/// GCS presign: the URL's X-Goog-Signature is RS256 over the string to sign,
/// rebuilt from the signed URL's own X-Goog-Date.
#[tokio::test]
async fn presign_signature_covers_the_string_to_sign() -> Result<()> {
    let sa = service_account("k2048.pem");
    let cred = Credential::with_service_account(sa.clone());
    let mut presigned = parts(URI);
    signer()
        .sign_request(&Context::new(), &mut presigned, Some(&cred), Some(Duration::from_secs(3600)))
        .await?;

    let query = presigned.uri.query().unwrap().to_string();
    let get = |k: &str| {
        query
            .split('&')
            .find_map(|kv| kv.split_once('=').filter(|(a, _)| *a == k).map(|(_, v)| v.to_string()))
            .unwrap()
    };
    let date = get("X-Goog-Date");
    let now: Timestamp = format!(
        "{}-{}-{}T{}:{}:{}Z",
        &date[0..4], &date[4..6], &date[6..8], &date[9..11], &date[11..13], &date[13..15]
    )
    .parse()
    .unwrap();
    let mut rebuilt = parts(URI);
    let mut signing_req = SigningRequest::build(&mut rebuilt)?;
    let (string_to_sign, _) =
        signer().build_string_to_sign(&mut signing_req, &sa.client_email, now, Duration::from_secs(3600))?;

    let expected: String = rs256(&sa.private_key, string_to_sign.as_bytes())
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect();
    assert_eq!(get("X-Goog-Signature"), expected);
    Ok(())
}

/// Service account → OAuth token: one form POST carrying an RS256 JWT
/// assertion, and the exchanged token is what authorizes the request.
#[tokio::test]
async fn token_exchange_sends_a_signed_assertion() -> Result<()> {
    let sa = service_account("k2048.pem");
    let cred = Credential::with_service_account(sa.clone());
    let endpoint = TokenEndpoint::default();
    let ctx = Context::new().with_http_send(endpoint.clone());
    let mut req = parts("https://storage.googleapis.com/bucket/obj");
    signer().sign_request(&ctx, &mut req, Some(&cred), None).await?;

    assert_eq!(req.headers[header::AUTHORIZATION], "Bearer provider-token");
    let seen = endpoint.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let (method, uri, content_type, body) = &seen[0];
    assert_eq!(method, "POST");
    assert_eq!(uri, "https://oauth2.googleapis.com/token");
    assert_eq!(content_type.as_deref(), Some("application/x-www-form-urlencoded"));
    let jwt = body
        .strip_prefix("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=")
        .expect("form body shape");

    let segments: Vec<&str> = jwt.split('.').collect();
    assert_eq!(segments.len(), 3);
    let header: serde_json::Value =
        serde_json::from_slice(&b64url(segments[0])).unwrap();
    assert_eq!(header["alg"], "RS256");
    let claims: serde_json::Value =
        serde_json::from_slice(&b64url(segments[1])).unwrap();
    let mut keys: Vec<&str> = claims.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, ["aud", "exp", "iat", "iss", "scope"]);
    assert_eq!(claims["iss"], sa.client_email);
    let signing_input = format!("{}.{}", segments[0], segments[1]);
    assert_eq!(
        b64url(segments[2]),
        rs256(&sa.private_key, signing_input.as_bytes())
    );
    Ok(())
}

/// Error layers, unchanged from the `rsa`-backed crate: presign wraps a key
/// parse failure as Unexpected; the token exchange surfaces core's
/// CredentialInvalid. A key below 2048 bits now fails the same way.
#[tokio::test]
async fn key_errors_keep_their_kinds() {
    for key in ["k2048pkcs1.pem", "k1024.pem"] {
        let bad = Credential::with_service_account(service_account(key));
        let presign = signer()
            .sign_request(&Context::new(), &mut parts(URI), Some(&bad), Some(Duration::from_secs(60)))
            .await
            .unwrap_err();
        assert_eq!(presign.kind(), ErrorKind::Unexpected, "{key}: {presign}");
        assert_eq!(presign.to_string(), "failed to parse private key", "{key}");

        let ctx = Context::new().with_http_send(TokenEndpoint::default());
        let exchange = signer()
            .sign_request(&ctx, &mut parts(URI), Some(&bad), None)
            .await
            .unwrap_err();
        assert_eq!(exchange.kind(), ErrorKind::CredentialInvalid, "{key}: {exchange}");
        assert_eq!(exchange.to_string(), "failed to parse PKCS#8 RSA private key PEM", "{key}");
    }
}
