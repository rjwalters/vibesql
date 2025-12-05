use anyhow::{Context, Result};
use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use md5::{Digest, Md5};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tracing::warn;

/// Password store for managing user authentication
#[derive(Debug, Clone)]
pub struct PasswordStore {
    /// Map of username to stored password (either Argon2 hash or MD5 hash format)
    passwords: HashMap<String, String>,
}

impl PasswordStore {
    /// Create a new empty password store
    pub fn new() -> Self {
        Self { passwords: HashMap::new() }
    }

    /// Load passwords from a file
    ///
    /// File format: username:password (one per line)
    /// Password formats supported:
    /// - Argon2 PHC format: `username:$argon2id$v=19$m=...` (recommended, secure storage)
    /// - Cleartext: `username:mysecret` (will be hashed with Argon2 on load)
    /// - MD5 for wire protocol: `username:{MD5}hash` (for PostgreSQL MD5 wire protocol compatibility)
    ///
    /// Comments start with # and empty lines are ignored.
    ///
    /// Security note: Cleartext passwords will be automatically hashed with Argon2.
    /// For PostgreSQL MD5 wire protocol, use {MD5} prefix (less secure storage).
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read password file: {:?}", path.as_ref()))?;

        let mut passwords = HashMap::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid password file format at line {}: expected 'username:password'",
                    line_num + 1
                ));
            }

            let username = parts[0].trim().to_string();
            let password_value = parts[1].trim();

            if username.is_empty() {
                return Err(anyhow::anyhow!("Empty username at line {}", line_num + 1));
            }

            // Determine password storage format
            let stored_password = if password_value.starts_with("$argon2") {
                // Already an Argon2 hash
                password_value.to_string()
            } else if password_value.starts_with("{MD5}") {
                // MD5 format for PostgreSQL MD5 wire protocol
                password_value.to_string()
            } else {
                // Cleartext password - hash it with Argon2
                warn!(
                    "Password for user '{}' in cleartext, hashing with Argon2 (update your password file with pre-hashed passwords)",
                    username
                );
                hash_password_argon2(password_value)?
            };

            passwords.insert(username, stored_password);
        }

        Ok(Self { passwords })
    }

    /// Add a user with a password (will be hashed with Argon2)
    #[allow(dead_code)]
    pub fn add_user(&mut self, username: String, password: &str) -> Result<()> {
        let hashed = hash_password_argon2(password)?;
        self.passwords.insert(username, hashed);
        Ok(())
    }

    /// Add a user with an already-hashed password
    #[allow(dead_code)]
    pub fn add_user_hashed(&mut self, username: String, password_hash: String) {
        self.passwords.insert(username, password_hash);
    }

    /// Get the stored password for a user
    pub fn get_password(&self, username: &str) -> Option<&String> {
        self.passwords.get(username)
    }

    /// Verify a cleartext password for a user
    /// This works with Argon2 hashes (recommended) but not with {MD5} format
    pub fn verify_cleartext(&self, username: &str, password: &str) -> bool {
        if let Some(stored) = self.get_password(username) {
            if stored.starts_with("$argon2") {
                // Argon2 hash - use password_hash verification
                if let Ok(parsed_hash) = PasswordHash::new(stored) {
                    return Argon2::default()
                        .verify_password(password.as_bytes(), &parsed_hash)
                        .is_ok();
                }
            } else if stored.starts_with("{MD5}") {
                // MD5 format doesn't support cleartext verification
                warn!(
                    "Cannot verify cleartext password for user '{}': password stored in MD5 format",
                    username
                );
                return false;
            }
        }
        false
    }

    /// Verify an MD5 password for a user
    /// PostgreSQL MD5 format: md5(md5(password + username) + salt)
    /// This only works if the password is stored in {MD5}cleartext format
    pub fn verify_md5(&self, username: &str, password_hash: &str, salt: &[u8; 4]) -> bool {
        if let Some(stored) = self.get_password(username) {
            if let Some(md5_password) = stored.strip_prefix("{MD5}") {
                // MD5 format storage - we can verify MD5 wire protocol
                let expected = compute_md5_password(md5_password, username, salt);
                let hash_to_compare = password_hash.strip_prefix("md5").unwrap_or(password_hash);
                return expected == hash_to_compare;
            } else {
                // Argon2 format doesn't support MD5 wire protocol
                warn!(
                    "Cannot verify MD5 password for user '{}': password stored in Argon2 format (use cleartext wire protocol instead)",
                    username
                );
            }
        }
        false
    }
}

impl Default for PasswordStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Hash a password using Argon2id
pub fn hash_password_argon2(password: &str) -> Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("Failed to hash password: {}", e))?;
    Ok(password_hash.to_string())
}

/// Compute PostgreSQL MD5 password hash
/// Format: "md5" + md5(md5(password + username) + salt)
pub fn compute_md5_password(password: &str, username: &str, salt: &[u8; 4]) -> String {
    // Step 1: md5(password + username)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(username.as_bytes());
    let inner_hash = hasher.finalize();
    let inner_hex = format!("{:x}", inner_hash);

    // Step 2: md5(inner_hex + salt)
    let mut hasher = Md5::new();
    hasher.update(inner_hex.as_bytes());
    hasher.update(salt);
    let outer_hash = hasher.finalize();

    format!("{:x}", outer_hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_store_new() {
        let store = PasswordStore::new();
        assert_eq!(store.passwords.len(), 0);
    }

    #[test]
    fn test_add_user_with_argon2() {
        let mut store = PasswordStore::new();
        store.add_user("postgres".to_string(), "secret123").unwrap();

        // Password should be hashed
        let stored = store.get_password("postgres").unwrap();
        assert!(stored.starts_with("$argon2"));
        assert_ne!(stored, "secret123");

        // Should verify correctly
        assert!(store.verify_cleartext("postgres", "secret123"));
        assert!(!store.verify_cleartext("postgres", "wrong"));
    }

    #[test]
    fn test_verify_cleartext_with_argon2() {
        let mut store = PasswordStore::new();
        let hash = hash_password_argon2("secret123").unwrap();
        store.add_user_hashed("postgres".to_string(), hash);

        assert!(store.verify_cleartext("postgres", "secret123"));
        assert!(!store.verify_cleartext("postgres", "wrong"));
        assert!(!store.verify_cleartext("nonexistent", "secret123"));
    }

    #[test]
    fn test_hash_password_argon2() {
        let hash1 = hash_password_argon2("secret").unwrap();
        let hash2 = hash_password_argon2("secret").unwrap();

        // Should produce different hashes (different salts)
        assert_ne!(hash1, hash2);

        // Both should start with $argon2
        assert!(hash1.starts_with("$argon2"));
        assert!(hash2.starts_with("$argon2"));

        // Both should verify the same password
        let parsed1 = PasswordHash::new(&hash1).unwrap();
        let parsed2 = PasswordHash::new(&hash2).unwrap();

        assert!(Argon2::default().verify_password(b"secret", &parsed1).is_ok());
        assert!(Argon2::default().verify_password(b"secret", &parsed2).is_ok());
    }

    #[test]
    fn test_compute_md5_password() {
        let password = "secret";
        let username = "postgres";
        let salt: [u8; 4] = [1, 2, 3, 4];

        let hash1 = compute_md5_password(password, username, &salt);
        let hash2 = compute_md5_password(password, username, &salt);

        // Should be deterministic
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 32); // MD5 hex is 32 characters
    }

    #[test]
    fn test_verify_md5_with_md5_storage() {
        let mut store = PasswordStore::new();
        // Store password in MD5 format (for MD5 wire protocol)
        store.add_user_hashed("postgres".to_string(), "{MD5}secret".to_string());

        let salt: [u8; 4] = [1, 2, 3, 4];
        let hash = compute_md5_password("secret", "postgres", &salt);

        assert!(store.verify_md5("postgres", &hash, &salt));
        assert!(store.verify_md5("postgres", &format!("md5{}", hash), &salt));
        assert!(!store.verify_md5("postgres", "wronghash", &salt));
    }

    #[test]
    fn test_md5_wire_protocol_not_supported_with_argon2() {
        let mut store = PasswordStore::new();
        store.add_user("postgres".to_string(), "secret").unwrap();

        // Should be stored as Argon2
        assert!(store.get_password("postgres").unwrap().starts_with("$argon2"));

        // MD5 wire protocol should not work with Argon2 storage
        let salt: [u8; 4] = [1, 2, 3, 4];
        let hash = compute_md5_password("secret", "postgres", &salt);
        assert!(!store.verify_md5("postgres", &hash, &salt));
    }

    #[test]
    fn test_load_from_file_argon2() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        let hash = hash_password_argon2("secret123").unwrap();
        writeln!(file, "# Comment line").unwrap();
        writeln!(file).unwrap();
        writeln!(file, "postgres:{}", hash).unwrap();
        file.flush().unwrap();

        let store = PasswordStore::load_from_file(file.path()).unwrap();
        assert_eq!(store.passwords.len(), 1);
        assert!(store.verify_cleartext("postgres", "secret123"));
    }

    #[test]
    fn test_load_from_file_md5_format() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "postgres:{{MD5}}secret").unwrap();
        file.flush().unwrap();

        let store = PasswordStore::load_from_file(file.path()).unwrap();
        assert_eq!(store.passwords.len(), 1);
        assert_eq!(store.get_password("postgres"), Some(&"{MD5}secret".to_string()));

        // Should work with MD5 wire protocol
        let salt: [u8; 4] = [1, 2, 3, 4];
        let hash = compute_md5_password("secret", "postgres", &salt);
        assert!(store.verify_md5("postgres", &hash, &salt));
    }

    #[test]
    fn test_load_from_file_cleartext_gets_hashed() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "postgres:secret123").unwrap();
        file.flush().unwrap();

        let store = PasswordStore::load_from_file(file.path()).unwrap();
        assert_eq!(store.passwords.len(), 1);

        // Should be hashed with Argon2
        let stored = store.get_password("postgres").unwrap();
        assert!(stored.starts_with("$argon2"));

        // Should verify
        assert!(store.verify_cleartext("postgres", "secret123"));
    }

    #[test]
    fn test_load_from_file_invalid_format() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "invalid_line_without_colon").unwrap();
        file.flush().unwrap();

        let result = PasswordStore::load_from_file(file.path());
        assert!(result.is_err());
    }
}
