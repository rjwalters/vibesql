//! HTTP API authentication middleware and handlers

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::auth::PasswordStore;
use crate::config::{HttpAuthConfig, HttpAuthMethod};

/// Authenticated user information extracted from request
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    /// Username or identifier
    pub identity: String,
    /// Authentication method used
    pub method: HttpAuthMethod,
}

/// JWT claims structure
#[derive(Debug, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (username)
    pub sub: String,
    /// Expiration time (Unix timestamp)
    pub exp: u64,
    /// Issued at time (Unix timestamp)
    pub iat: u64,
    /// Issuer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    /// Audience
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<String>,
}

/// Request body for token exchange
#[derive(Debug, Deserialize)]
pub struct TokenRequest {
    /// Username for authentication
    pub username: String,
    /// Password for authentication
    pub password: String,
}

/// Response body for token exchange
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    /// JWT access token
    pub access_token: String,
    /// Token type (always "Bearer")
    pub token_type: String,
    /// Expiration time in seconds
    pub expires_in: u64,
}

/// Error response for authentication failures
#[derive(Debug, Serialize)]
pub struct AuthError {
    pub error: String,
    pub message: String,
}

impl AuthError {
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}

/// State for authentication middleware
#[derive(Clone)]
pub struct AuthState {
    pub config: Arc<HttpAuthConfig>,
    pub password_store: Option<Arc<PasswordStore>>,
}

/// Authentication middleware for protected routes
pub async fn auth_middleware(
    State(state): State<AuthState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    // If auth is disabled, allow all requests
    if !state.config.enabled {
        return next.run(request).await;
    }

    // Extract Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    let auth_header = match auth_header {
        Some(h) => h,
        None => {
            debug!("No Authorization header provided");
            return unauthorized_response("missing_token", "Authorization header required");
        }
    };

    // Try each enabled authentication method
    let result = validate_auth(auth_header, &state.config, state.password_store.as_deref());

    match result {
        Ok(user) => {
            debug!("Authenticated user: {} via {:?}", user.identity, user.method);
            // Store authenticated user in request extensions
            request.extensions_mut().insert(user);
            next.run(request).await
        }
        Err(err) => {
            warn!("Authentication failed: {}", err);
            unauthorized_response("invalid_token", &err)
        }
    }
}

/// Validate authentication from Authorization header
fn validate_auth(
    auth_header: &str,
    config: &HttpAuthConfig,
    password_store: Option<&PasswordStore>,
) -> Result<AuthenticatedUser, String> {
    // Parse the authorization header
    let parts: Vec<&str> = auth_header.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Err("Invalid Authorization header format".to_string());
    }

    let (scheme, credentials) = (parts[0], parts[1]);

    match scheme.to_lowercase().as_str() {
        "bearer" => {
            // Try API key first
            if config.methods.contains(&HttpAuthMethod::ApiKey) {
                if let Some(user) = validate_api_key(credentials, config) {
                    return Ok(user);
                }
            }

            // Try JWT
            if config.methods.contains(&HttpAuthMethod::Jwt) {
                if let Some(user) = validate_jwt(credentials, config) {
                    return Ok(user);
                }
            }

            Err("Invalid bearer token".to_string())
        }
        "basic" => {
            if !config.methods.contains(&HttpAuthMethod::Basic) {
                return Err("Basic authentication not enabled".to_string());
            }

            validate_basic_auth(credentials, password_store)
        }
        _ => Err(format!("Unsupported authentication scheme: {}", scheme)),
    }
}

/// Validate API key authentication
fn validate_api_key(token: &str, config: &HttpAuthConfig) -> Option<AuthenticatedUser> {
    if config.api_keys.keys.contains(&token.to_string()) {
        Some(AuthenticatedUser {
            identity: "api_key_user".to_string(),
            method: HttpAuthMethod::ApiKey,
        })
    } else {
        None
    }
}

/// Validate JWT authentication
fn validate_jwt(token: &str, config: &HttpAuthConfig) -> Option<AuthenticatedUser> {
    if config.jwt.secret.is_empty() {
        debug!("JWT secret not configured");
        return None;
    }

    let mut validation = Validation::default();

    // Set issuer validation if configured
    if let Some(ref issuer) = config.jwt.issuer {
        validation.set_issuer(&[issuer]);
    }

    // Set audience validation if configured
    if let Some(ref audience) = config.jwt.audience {
        validation.set_audience(&[audience]);
    }

    let decoding_key = DecodingKey::from_secret(config.jwt.secret.as_bytes());

    match decode::<JwtClaims>(token, &decoding_key, &validation) {
        Ok(token_data) => Some(AuthenticatedUser {
            identity: token_data.claims.sub,
            method: HttpAuthMethod::Jwt,
        }),
        Err(e) => {
            debug!("JWT validation failed: {}", e);
            None
        }
    }
}

/// Validate Basic authentication
fn validate_basic_auth(
    credentials: &str,
    password_store: Option<&PasswordStore>,
) -> Result<AuthenticatedUser, String> {
    let decoded = BASE64
        .decode(credentials)
        .map_err(|_| "Invalid base64 encoding")?;

    let credentials_str =
        String::from_utf8(decoded).map_err(|_| "Invalid UTF-8 in credentials")?;

    let parts: Vec<&str> = credentials_str.splitn(2, ':').collect();
    if parts.len() != 2 {
        return Err("Invalid Basic auth format (expected username:password)".to_string());
    }

    let (username, password) = (parts[0], parts[1]);

    // Verify against password store
    let password_store = password_store.ok_or("Password store not configured for Basic auth")?;

    if password_store.verify_cleartext(username, password) {
        Ok(AuthenticatedUser {
            identity: username.to_string(),
            method: HttpAuthMethod::Basic,
        })
    } else {
        Err("Invalid username or password".to_string())
    }
}

/// Generate a JWT token for a user
pub fn generate_jwt(username: &str, config: &HttpAuthConfig) -> Result<String, String> {
    if config.jwt.secret.is_empty() {
        return Err("JWT secret not configured".to_string());
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("Time error: {}", e))?
        .as_secs();

    let claims = JwtClaims {
        sub: username.to_string(),
        exp: now + config.jwt.expiration_secs,
        iat: now,
        iss: config.jwt.issuer.clone(),
        aud: config.jwt.audience.clone(),
    };

    let encoding_key = EncodingKey::from_secret(config.jwt.secret.as_bytes());

    encode(&Header::default(), &claims, &encoding_key)
        .map_err(|e| format!("Failed to encode JWT: {}", e))
}

/// Handler for token exchange endpoint
pub async fn token_handler(
    State(state): State<AuthState>,
    Json(req): Json<TokenRequest>,
) -> impl IntoResponse {
    // Check if JWT is enabled
    if !state.config.methods.contains(&HttpAuthMethod::Jwt) {
        return (
            StatusCode::BAD_REQUEST,
            Json(AuthError::new(
                "unsupported_grant_type",
                "JWT authentication is not enabled",
            )),
        )
            .into_response();
    }

    // Verify credentials
    let password_store = match &state.password_store {
        Some(store) => store,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AuthError::new(
                    "server_error",
                    "Password store not configured",
                )),
            )
                .into_response();
        }
    };

    if !password_store.verify_cleartext(&req.username, &req.password) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(AuthError::new(
                "invalid_grant",
                "Invalid username or password",
            )),
        )
            .into_response();
    }

    // Generate JWT
    match generate_jwt(&req.username, &state.config) {
        Ok(token) => (
            StatusCode::OK,
            Json(TokenResponse {
                access_token: token,
                token_type: "Bearer".to_string(),
                expires_in: state.config.jwt.expiration_secs,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(AuthError::new("server_error", e)),
        )
            .into_response(),
    }
}

/// Create an unauthorized response with WWW-Authenticate header
fn unauthorized_response(error: &str, description: &str) -> Response {
    let body = Json(AuthError::new(error, description));

    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            "Bearer realm=\"vibesql\", error=\"invalid_token\"",
        )],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiKeyConfig, JwtConfig};

    fn test_config() -> HttpAuthConfig {
        HttpAuthConfig {
            enabled: true,
            methods: vec![HttpAuthMethod::ApiKey, HttpAuthMethod::Basic, HttpAuthMethod::Jwt],
            api_keys: ApiKeyConfig {
                keys: vec!["test-api-key-123".to_string()],
            },
            jwt: JwtConfig {
                secret: "test-secret-key-for-jwt".to_string(),
                issuer: Some("vibesql".to_string()),
                audience: Some("vibesql-api".to_string()),
                expiration_secs: 3600,
            },
        }
    }

    #[test]
    fn test_validate_api_key_success() {
        let config = test_config();
        let result = validate_api_key("test-api-key-123", &config);
        assert!(result.is_some());
        let user = result.unwrap();
        assert_eq!(user.method, HttpAuthMethod::ApiKey);
    }

    #[test]
    fn test_validate_api_key_failure() {
        let config = test_config();
        let result = validate_api_key("invalid-key", &config);
        assert!(result.is_none());
    }

    #[test]
    fn test_generate_and_validate_jwt() {
        let config = test_config();

        // Generate token
        let token = generate_jwt("testuser", &config).expect("Failed to generate JWT");

        // Validate token
        let result = validate_jwt(&token, &config);
        assert!(result.is_some());
        let user = result.unwrap();
        assert_eq!(user.identity, "testuser");
        assert_eq!(user.method, HttpAuthMethod::Jwt);
    }

    #[test]
    fn test_validate_jwt_invalid_token() {
        let config = test_config();
        let result = validate_jwt("invalid.jwt.token", &config);
        assert!(result.is_none());
    }

    #[test]
    fn test_validate_basic_auth_format() {
        // Valid format: base64(username:password)
        let credentials = BASE64.encode("testuser:testpass");
        let result = validate_basic_auth(&credentials, None);
        // Should fail because no password store is configured
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Password store not configured"));
    }

    #[test]
    fn test_validate_basic_auth_invalid_base64() {
        let result = validate_basic_auth("not-valid-base64!!!", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_auth_bearer_api_key() {
        let config = test_config();
        let result = validate_auth("Bearer test-api-key-123", &config, None);
        assert!(result.is_ok());
        let user = result.unwrap();
        assert_eq!(user.method, HttpAuthMethod::ApiKey);
    }

    #[test]
    fn test_validate_auth_bearer_jwt() {
        let config = test_config();
        let token = generate_jwt("jwtuser", &config).unwrap();
        let result = validate_auth(&format!("Bearer {}", token), &config, None);
        assert!(result.is_ok());
        let user = result.unwrap();
        assert_eq!(user.identity, "jwtuser");
        assert_eq!(user.method, HttpAuthMethod::Jwt);
    }

    #[test]
    fn test_validate_auth_unsupported_scheme() {
        let config = test_config();
        let result = validate_auth("Digest abc123", &config, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported authentication scheme"));
    }
}
