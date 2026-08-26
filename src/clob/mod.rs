//! Shared Polymarket CLOB machinery: the wire spec ([`decode`]) and the book
//! reconstruction on top of it ([`book`]).
//!
//! Deliberately **synchronous and transport-free**: everything here consumes
//! `&str` frames and owned events, never sockets or files. Recorded capture
//! files (replay, conversion) and the future live websocket adapter feed the
//! *same* code — the transport-specific parts (file iteration, reconnects)
//! live with their callers.

pub mod book;
pub mod decode;
