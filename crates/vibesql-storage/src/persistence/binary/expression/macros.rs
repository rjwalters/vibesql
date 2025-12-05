// ============================================================================
// Serialization Macros
// ============================================================================
//
// Provides macros to reduce boilerplate for enum serialization.

/// Macro to generate write/read functions for simple tag-based enums.
///
/// This handles the common pattern where an enum variant maps to a u8 tag.
macro_rules! impl_simple_enum_serialization {
    ($enum_name:ty, $write_fn:ident, $read_fn:ident, $type_name:expr, {
        $($variant:path => $tag:expr),* $(,)?
    }) => {
        pub(super) fn $write_fn<W: std::io::Write>(
            writer: &mut W,
            value: &$enum_name,
        ) -> Result<(), $crate::StorageError> {
            let tag = match value {
                $($variant => $tag,)*
            };
            writer
                .write_all(&[tag])
                .map_err(|e| $crate::StorageError::NotImplemented(format!("Write error: {}", e)))
        }

        pub(super) fn $read_fn<R: std::io::Read>(
            reader: &mut R,
        ) -> Result<$enum_name, $crate::StorageError> {
            let tag = super::super::io::read_u8(reader)?;
            match tag {
                $($tag => Ok($variant),)*
                _ => Err($crate::StorageError::NotImplemented(format!(
                    "Unknown {} tag: {}",
                    $type_name,
                    tag
                ))),
            }
        }
    };
}
