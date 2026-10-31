//! The messages between the runner and the runtime under test, on the
//! runtime's stdin and stdout: each is its length in bytes, 4 bytes
//! big-endian, then that many bytes of the encoded protobuf message. Nothing
//! else is written to the runtime's stdout; its stderr is its own.

use std::io::{self, Read, Write};

/// The largest message either side may send. A length above it is not a
/// message: most likely something other than a frame was written.
pub const MAX_FRAME_BYTES: usize = 64 << 20;

/// Writes `message` as one frame and flushes it.
///
/// # Errors
///
/// The write's error, or [`io::ErrorKind::InvalidInput`] for a message
/// larger than [`MAX_FRAME_BYTES`].
pub fn write_frame(out: &mut impl Write, message: &[u8]) -> io::Result<()> {
    if message.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a message of {} bytes is larger than a frame may be ({MAX_FRAME_BYTES})",
                message.len()
            ),
        ));
    }
    out.write_all(&(message.len() as u32).to_be_bytes())?;
    out.write_all(message)?;
    out.flush()
}

/// Reads one frame's message; `None` when the stream ends before a frame
/// starts.
///
/// # Errors
///
/// The read's error; [`io::ErrorKind::UnexpectedEof`] for a stream that ends
/// inside a frame, and [`io::ErrorKind::InvalidData`] for a length larger
/// than [`MAX_FRAME_BYTES`].
pub fn read_frame(input: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    let mut filled = 0;
    while filled < length.len() {
        match input.read(&mut length[filled..]) {
            Ok(0) if filled == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the stream ended inside a frame's length",
                ))
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "a frame of {length} bytes is larger than a frame may be ({MAX_FRAME_BYTES}): \
                 is something other than frames written to stdout?"
            ),
        ));
    }
    let mut message = vec![0u8; length];
    input.read_exact(&mut message)?;
    Ok(Some(message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_read_back_as_written() {
        let mut stream = Vec::new();
        write_frame(&mut stream, b"one").unwrap();
        write_frame(&mut stream, b"").unwrap();
        assert_eq!(&stream[..4], &[0, 0, 0, 3]);

        let mut input = stream.as_slice();
        assert_eq!(
            read_frame(&mut input).unwrap().as_deref(),
            Some(&b"one"[..])
        );
        assert_eq!(read_frame(&mut input).unwrap().as_deref(), Some(&b""[..]));
        assert_eq!(read_frame(&mut input).unwrap(), None);
    }

    #[test]
    fn a_stream_cut_inside_a_frame_is_an_error() {
        let mut input: &[u8] = &[0, 0];
        let error = read_frame(&mut input).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);

        let mut input: &[u8] = &[0, 0, 0, 5, b'a'];
        let error = read_frame(&mut input).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// Text on stdout reads as an absurd length, not as a message.
    #[test]
    fn text_is_not_a_frame() {
        let mut input: &[u8] = b"hello, world\n";
        let error = read_frame(&mut input).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
