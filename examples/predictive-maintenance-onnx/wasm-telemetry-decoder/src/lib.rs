//! Proprietary binary sensor-telemetry frame → JSON, as a sandboxed Pulse WASM
//! operator.
//!
//! Industrial machines stream a vendor's **binary** frame (not text — contrast
//! with the COBOL example's fixed-width ASCII). Nobody wants to re-implement the
//! vendor decoder in the engine, and the frames are operational/sensitive — so
//! we run the decoder as a zero-import `wasm32-unknown-unknown` module the engine
//! sandboxes. `pulse-wasm-guest` supplies the `alloc`/`process`/`memory` ABI, the
//! bump allocator and the panic handler, so this stays pure compute.
//!
//! Transport: binary-over-JSON. The frame arrives **base64-encoded** in the event
//! value (the standard way binary telemetry crosses an MQTT/Kafka/HTTP hop). The
//! module base64-decodes, then parses the fixed binary layout below. (On the
//! native/cluster path the raw bytes can be sent binary-safe; base64 keeps the
//! HTTP-fed standalone demo lossless.)
//!
//! Frame layout — 30 bytes, little-endian:
//!
//! ```text
//! off  size  field          type
//!   0    4   magic "SFM1"   u8[4]
//!   4    8   machineId       ascii (space/nul padded)
//!  12    4   vibration_mm_s  f32 LE
//!  16    4   bearing_temp_c  f32 LE
//!  20    2   rpm             u16 LE
//!  22    4   motor_current_a f32 LE
//!  26    4   timestamp       u32 LE (epoch seconds)
//! ```
//!
//! Emits, e.g.:
//! `{"machineId":"MCH-0007","vibration":11,"bearingTemp":95,"rpm":1500,
//!   "motorCurrent":22,"ts":1718000000,"_source":"telemetry-binary"}`
//!
//! Those four numeric fields (in the order the model expects) are what the next
//! operator in the chain — ONNX `mlPredict` — scores for failure risk.
#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write;

const MAGIC: &[u8; 4] = b"SFM1";
const FRAME_LEN: usize = 30;

/// Standard base64 alphabet value, or -1 for padding / whitespace / invalid.
fn b64_val(c: u8) -> i16 {
    match c {
        b'A'..=b'Z' => (c - b'A') as i16,
        b'a'..=b'z' => (c - b'a' + 26) as i16,
        b'0'..=b'9' => (c - b'0' + 52) as i16,
        b'+' => 62,
        b'/' => 63,
        _ => -1, // '=', '\n', spaces — skipped by the streaming decoder
    }
}

/// Streaming base64 decode (RFC 4648). Skips non-alphabet bytes (padding,
/// newlines), so it tolerates wrapped/padded input without a separate pass.
fn b64_decode(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len() / 4 * 3 + 3);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for &c in input {
        let v = b64_val(c);
        if v < 0 {
            continue;
        }
        buf = (buf << 6) | (v as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    out
}

fn f32_le(b: &[u8], off: usize) -> f32 {
    f32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

/// Append a finite f32 as a JSON number; non-finite (NaN/Inf) becomes 0 so the
/// output is always valid JSON the downstream model can parse.
fn push_num(out: &mut String, x: f32) {
    if x.is_finite() {
        let _ = write!(out, "{}", x);
    } else {
        out.push('0');
    }
}

fn push_json_str(out: &mut String, b: &[u8]) {
    out.push('"');
    for &c in b {
        match c {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x00..=0x1f => {} // drop control chars (incl. nul padding)
            _ => out.push(c as char),
        }
    }
    out.push('"');
}

/// Trim trailing space / nul padding from the fixed-width machineId field.
fn rtrim<'a>(b: &'a [u8]) -> &'a [u8] {
    let mut e = b.len();
    while e > 0 && (b[e - 1] == b' ' || b[e - 1] == 0) {
        e -= 1;
    }
    &b[..e]
}

fn parse(event: &[u8]) -> Vec<u8> {
    let frame = b64_decode(event);
    if frame.len() < FRAME_LEN || &frame[0..4] != MAGIC {
        return Vec::new(); // out_len == 0 → the engine drops the event
    }

    let machine = rtrim(&frame[4..12]);
    let vibration = f32_le(&frame, 12);
    let bearing_temp = f32_le(&frame, 16);
    let rpm = u16::from_le_bytes([frame[20], frame[21]]);
    let motor_current = f32_le(&frame, 22);
    let ts = u32::from_le_bytes([frame[26], frame[27], frame[28], frame[29]]);

    let mut out = String::with_capacity(160);
    out.push_str("{\"machineId\":");
    push_json_str(&mut out, machine);
    out.push_str(",\"vibration\":");
    push_num(&mut out, vibration);
    out.push_str(",\"bearingTemp\":");
    push_num(&mut out, bearing_temp);
    out.push_str(",\"rpm\":");
    let _ = write!(out, "{}", rpm);
    out.push_str(",\"motorCurrent\":");
    push_num(&mut out, motor_current);
    out.push_str(",\"ts\":");
    let _ = write!(out, "{}", ts);
    out.push_str(",\"_source\":\"telemetry-binary\"}");
    out.into_bytes()
}

pulse_wasm_guest::operator!(|event: &[u8]| -> Vec<u8> { parse(event) });
