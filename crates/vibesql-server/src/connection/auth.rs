//! Authentication handling for PostgreSQL wire protocol
//!
//! This module implements various authentication methods supported by the PostgreSQL
//! wire protocol: trust, cleartext password, and MD5 password authentication.

use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tracing::{debug, error, info};

use crate::{
    auth::PasswordStore,
    config::Config,
    protocol::{BackendMessage, FrontendMessage},
};

use super::io::{flush_write_buffer, read_message};

/// Authenticate the user based on the configured authentication method
pub async fn authenticate(
    config: &Config,
    password_store: &Option<Arc<PasswordStore>>,
    read_half: &mut OwnedReadHalf,
    write_half: &mut OwnedWriteHalf,
    read_buf: &mut BytesMut,
    write_buf: &mut BytesMut,
    user: &str,
) -> Result<()> {
    match config.auth.method.as_str() {
        "trust" => {
            // Trust authentication - no password required
            debug!("Using trust authentication for user '{}'", user);
            send_authentication_ok(write_half, write_buf).await?;
            Ok(())
        }

        "password" => {
            // Cleartext password authentication
            debug!("Requesting cleartext password for user '{}'", user);
            send_cleartext_password_request(write_half, write_buf).await?;

            // Read password response
            read_message(read_half, read_buf).await?;
            let msg = FrontendMessage::decode(read_buf)?;

            match msg {
                Some(FrontendMessage::Password { password }) => {
                    debug!("Received password from user '{}'", user);

                    if let Some(ref store) = password_store {
                        if store.verify_cleartext(user, &password) {
                            info!("User '{}' authenticated successfully", user);
                            send_authentication_ok(write_half, write_buf).await?;
                            Ok(())
                        } else {
                            error!("Authentication failed for user '{}'", user);
                            Err(anyhow::anyhow!("Authentication failed"))
                        }
                    } else {
                        error!("No password store configured");
                        Err(anyhow::anyhow!("Authentication not configured"))
                    }
                }
                _ => {
                    error!("Expected password message, got: {:?}", msg);
                    Err(anyhow::anyhow!("Expected password message"))
                }
            }
        }

        "md5" => {
            // MD5 password authentication
            debug!("Requesting MD5 password for user '{}'", user);

            // Generate random salt
            use rand::Rng;
            let salt: [u8; 4] = rand::rng().random();

            send_md5_password_request(write_half, write_buf, &salt).await?;

            // Read password response
            read_message(read_half, read_buf).await?;
            let msg = FrontendMessage::decode(read_buf)?;

            match msg {
                Some(FrontendMessage::Password { password }) => {
                    debug!("Received MD5 password response from user '{}'", user);

                    if let Some(ref store) = password_store {
                        if store.verify_md5(user, &password, &salt) {
                            info!("User '{}' authenticated successfully (MD5)", user);
                            send_authentication_ok(write_half, write_buf).await?;
                            Ok(())
                        } else {
                            error!("MD5 authentication failed for user '{}'", user);
                            Err(anyhow::anyhow!("Authentication failed"))
                        }
                    } else {
                        error!("No password store configured");
                        Err(anyhow::anyhow!("Authentication not configured"))
                    }
                }
                _ => {
                    error!("Expected password message, got: {:?}", msg);
                    Err(anyhow::anyhow!("Expected password message"))
                }
            }
        }

        "scram-sha-256" => {
            // SCRAM-SHA-256 not yet implemented
            error!("SCRAM-SHA-256 authentication not yet implemented");
            Err(anyhow::anyhow!("SCRAM-SHA-256 not implemented"))
        }

        _ => {
            error!("Unsupported authentication method: {}", config.auth.method);
            Err(anyhow::anyhow!("Unsupported authentication method"))
        }
    }
}

/// Send authentication OK message
async fn send_authentication_ok(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> Result<()> {
    BackendMessage::AuthenticationOk.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send cleartext password request
async fn send_cleartext_password_request(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> Result<()> {
    BackendMessage::AuthenticationCleartextPassword.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send MD5 password request with salt
async fn send_md5_password_request(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    salt: &[u8; 4],
) -> Result<()> {
    BackendMessage::AuthenticationMD5Password { salt: *salt }.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}
