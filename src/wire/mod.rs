//! The wire: the only code that converts between the generated protocol
//! types (`orcher-proto`) and this crate's own types.
//!
//! Generated types are the wire format, not this crate's vocabulary. Keeping
//! every conversion here means a change to the protocol is a change in one
//! place, and the rest of the crate speaks its own types. `scripts/
//! check-wire-boundary.sh` keeps generated types from spreading elsewhere.

pub(crate) mod client;
pub(crate) mod engine;
pub(crate) mod host;
pub(crate) mod status;
