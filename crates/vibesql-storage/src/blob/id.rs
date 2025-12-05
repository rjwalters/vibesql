/// Blob storage identifier
///
/// Unique identifier for stored blobs, based on UUID v4.
/// Can be converted to/from string paths for storage backends.
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a blob
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BlobId(Uuid);

impl BlobId {
    /// Create a new random blob ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from UUID
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Create from string representation
    pub fn parse(s: &str) -> Option<Self> {
        Uuid::parse_str(s).ok().map(Self)
    }

    /// Get the inner UUID
    pub fn inner(&self) -> Uuid {
        self.0
    }

    /// Convert to storage path
    ///
    /// Uses hierarchical directory structure to avoid too many files in one directory:
    /// - First 2 chars: directory 1
    /// - Next 2 chars: directory 2
    /// - Rest: filename
    ///
    /// Example: `abc1-2b3c-... → ab/c1/2b3c...`
    pub fn to_path(&self) -> String {
        let uuid_str = self.0.to_string().replace("-", "");
        if uuid_str.len() >= 4 {
            format!("{}/{}/{}", &uuid_str[0..2], &uuid_str[2..4], &uuid_str[4..])
        } else {
            uuid_str
        }
    }

    /// Get a URL-safe representation
    pub fn to_url_safe(&self) -> String {
        self.0.to_string()
    }
}

impl Default for BlobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for BlobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_id_creation() {
        let id1 = BlobId::new();
        let id2 = BlobId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_blob_id_to_path() {
        let uuid = Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap();
        let id = BlobId::from_uuid(uuid);
        let path = id.to_path();
        assert!(path.starts_with("12/34/"));
    }

    #[test]
    fn test_blob_id_roundtrip() {
        let id1 = BlobId::new();
        let str_repr = id1.to_string();
        let id2 = BlobId::parse(&str_repr).unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_blob_id_from_parse() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = uuid_str.parse::<BlobId>().unwrap();
        assert_eq!(id.to_string(), uuid_str);
    }
}
