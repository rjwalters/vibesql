// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! JWT encoding helpers.

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{RSA_PKCS1_SHA256, RsaKeyPair};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Serialize;
use zeroize::Zeroizing;

use crate::{Error, Result};

/// An RSA private key for RS256 signing.
///
/// The key is held by aws-lc, whose private-key operation is blinded and
/// constant-time; it replaces the `rsa` crate (RUSTSEC-2023-0071). aws-lc
/// accepts 2048- to 8192-bit moduli.
pub struct Rs256Key(RsaKeyPair);

impl std::fmt::Debug for Rs256Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rs256Key")
            .field("bits", &(self.0.public_modulus_len() * 8))
            .finish_non_exhaustive()
    }
}

impl Rs256Key {
    /// Parse an RSA private key in PKCS#8 PEM format (`PRIVATE KEY`).
    ///
    /// PEM decoding is the same RFC 7468 decoder the `rsa` crate used, so the
    /// set of accepted encodings is unchanged.
    pub fn from_pkcs8_pem(private_key_pem: &[u8]) -> Result<Self> {
        let invalid = || Error::credential_invalid("failed to parse PKCS#8 RSA private key PEM");
        std::str::from_utf8(private_key_pem).map_err(|e| {
            Error::credential_invalid("RSA private key PEM is not valid UTF-8").with_source(e)
        })?;
        let (label, der) =
            pem_rfc7468::decode_vec(private_key_pem).map_err(|e| invalid().with_source(e))?;
        let der = Zeroizing::new(der);
        if label != "PRIVATE KEY" {
            return Err(invalid());
        }
        RsaKeyPair::from_pkcs8(&der)
            .map(Self)
            .map_err(|e| invalid().with_source(e))
    }

    /// Sign `message` with RSASSA-PKCS1-v1_5 and SHA-256.
    pub fn sign(&self, message: &[u8]) -> Result<Vec<u8>> {
        let mut signature = vec![0; self.0.public_modulus_len()];
        self.0
            .sign(&RSA_PKCS1_SHA256, &SystemRandom::new(), message, &mut signature)
            .map_err(|e| Error::unexpected("failed to sign with RSA private key").with_source(e))?;
        Ok(signature)
    }
}

/// Encode a JWS compact JWT using the RS256 algorithm.
///
/// RS256 is RSA PKCS#1 v1.5 with SHA-256. The caller owns the JWT header and
/// claims shape so service-specific fields such as `x5t` stay service-local.
pub fn encode_rs256<H, C>(header: &H, claims: &C, private_key: &Rs256Key) -> Result<String>
where
    H: Serialize,
    C: Serialize,
{
    let encoded_header = encode_json(header)?;
    let encoded_claims = encode_json(claims)?;
    let signing_input = format!("{encoded_header}.{encoded_claims}");

    let signature = private_key.sign(signing_input.as_bytes())?;
    let encoded_signature = URL_SAFE_NO_PAD.encode(signature);

    Ok(format!("{signing_input}.{encoded_signature}"))
}

/// Encode a JWS compact JWT using an RSA private key in PKCS#8 PEM format.
pub fn encode_rs256_pem<H, C>(header: &H, claims: &C, private_key_pem: &[u8]) -> Result<String>
where
    H: Serialize,
    C: Serialize,
{
    let private_key = Rs256Key::from_pkcs8_pem(private_key_pem)?;

    encode_rs256(header, claims, &private_key)
}

fn encode_json<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let json = serde_json::to_vec(value)
        .map_err(|e| Error::unexpected("failed to serialize JWT JSON").with_source(e))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use aws_lc_rs::signature::{KeyPair, RSA_PKCS1_2048_8192_SHA256, UnparsedPublicKey};
    use serde_json::json;

    #[derive(Serialize)]
    struct Header<'a> {
        alg: &'a str,
        typ: &'a str,
        x5t: &'a str,
    }

    #[derive(Serialize)]
    struct Claims<'a> {
        iss: &'a str,
        sub: &'a str,
        aud: &'a str,
        exp: u64,
    }

    const HEADER: Header<'static> = Header {
        alg: "RS256",
        typ: "JWT",
        x5t: "thumbprint",
    };
    const CLAIMS: Claims<'static> = Claims {
        iss: "client",
        sub: "client",
        aud: "https://example.com/token",
        exp: 1,
    };

    fn testdata(name: &str) -> Vec<u8> {
        std::fs::read(format!("{}/../testdata/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
    }

    fn golden() -> serde_json::Value {
        serde_json::from_slice(&testdata("golden.json")).unwrap()
    }

    /// PKCS#1 v1.5 is deterministic: the JWT must be byte-identical to the one
    /// the `rsa`-backed 3.3.1 produced for the same key and input.
    #[test]
    fn encode_rs256_pem_matches_rsa_crate_golden() -> Result<()> {
        for key in ["k2048", "k4096"] {
            let jwt = encode_rs256_pem(&HEADER, &CLAIMS, &testdata(&format!("{key}.pem")))?;
            assert_eq!(jwt, golden()[key]["jwt"].as_str().unwrap(), "{key}");
        }
        Ok(())
    }

    #[test]
    fn encode_rs256_keeps_jws_shape_and_verifiable_signature() -> Result<()> {
        let private_key = Rs256Key::from_pkcs8_pem(&testdata("k2048.pem"))?;
        let jwt = encode_rs256(&HEADER, &CLAIMS, &private_key)?;

        let parts = jwt.split('.').collect::<Vec<_>>();
        assert_eq!(parts.len(), 3);
        let header: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[0]).unwrap()).unwrap();
        assert_eq!(
            header,
            json!({"alg": "RS256", "typ": "JWT", "x5t": "thumbprint"})
        );

        let signature = URL_SAFE_NO_PAD.decode(parts[2]).unwrap();
        UnparsedPublicKey::new(
            &RSA_PKCS1_2048_8192_SHA256,
            private_key.0.public_key().as_ref(),
        )
        .verify(format!("{}.{}", parts[0], parts[1]).as_bytes(), &signature)
        .expect("signature verifies");
        Ok(())
    }

    #[test]
    fn rejects_what_the_rsa_crate_rejected() {
        for (input, message) in [
            (
                testdata("k2048pkcs1.pem"),
                "failed to parse PKCS#8 RSA private key PEM",
            ),
            (
                b"not a pem".to_vec(),
                "failed to parse PKCS#8 RSA private key PEM",
            ),
            (vec![0xff, 0xfe], "RSA private key PEM is not valid UTF-8"),
        ] {
            let err = encode_rs256_pem(&HEADER, &CLAIMS, &input).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::CredentialInvalid);
            assert!(err.to_string().contains(message), "{err}");
        }
    }

    /// The one deliberate narrowing: aws-lc refuses moduli below 2048 bits
    /// (and above 8192), which the `rsa` crate signed with. Neither Google nor
    /// Entra documents a size bound, so this is an accepted behaviour change
    /// (VibeSQL #6826), not a proven no-op.
    #[test]
    fn refuses_keys_below_2048_bits() {
        let err = encode_rs256_pem(&HEADER, &CLAIMS, &testdata("k1024.pem")).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::CredentialInvalid);
    }

    #[test]
    fn still_signs_with_small_public_exponent() -> Result<()> {
        encode_rs256_pem(&HEADER, &CLAIMS, &testdata("k2048e3.pem"))?;
        Ok(())
    }
}
