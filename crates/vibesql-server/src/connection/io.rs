//! I/O operations for connection handling
//!
//! This module provides low-level I/O operations for reading from and writing
//! to the TCP connection.

use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

/// Read data from the connection into the buffer
pub async fn read_message(read_half: &mut OwnedReadHalf, read_buf: &mut BytesMut) -> Result<()> {
    let n = read_half.read_buf(read_buf).await?;
    if n == 0 {
        return Err(anyhow::anyhow!("Connection closed"));
    }
    Ok(())
}

/// Flush the write buffer to the connection
pub async fn flush_write_buffer(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> Result<()> {
    write_half.write_all(write_buf).await?;
    write_half.flush().await?;
    write_buf.clear();
    Ok(())
}
