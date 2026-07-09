//! COBOL copybook (fixed-width) → JSON, as a sandboxed Pulse WASM operator.
//!
//! Wraps a legacy mainframe record layout so it can enter a modern event flow
//! **without rewriting the parser**. The whole module is pure compute: no host
//! imports, no syscalls, no network — `pulse-wasm-guest` generates the
//! `alloc`/`process`/`memory` ABI + the bump allocator + panic handler, so this
//! compiles to a zero-import `wasm32-unknown-unknown` cdylib that the engine's
//! `ChicoryWasmRunner` accepts as-is.
//!
//! Copybook handled (classic 80-column record, ASCII):
//!
//! ```text
//! 01 TXN-RECORD.
//!    05 ACCT-ID       PIC X(10).     pos  1-10
//!    05 TXN-CODE      PIC X(02).     pos 11-12
//!    05 AMOUNT        PIC 9(10)V99.  pos 13-24   (12 digits, last 2 = cents)
//!    05 PRODUCT-CODE  PIC X(04).     pos 25-28
//!    05 CURRENCY      PIC X(03).     pos 29-31
//!    05 POST-DATE     PIC X(08).     pos 32-39   (YYYYMMDD)
//!    05 MEMO          PIC X(40).     pos 40-79   (free text)
//!    05 FILLER        PIC X(01).     pos 80
//! ```
#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

// 0-based, end-exclusive field windows.
const ACCT: (usize, usize) = (0, 10);
const CODE: (usize, usize) = (10, 12);
const AMT: (usize, usize) = (12, 24); // PIC 9(10)V99
const PROD: (usize, usize) = (24, 28);
const CCY: (usize, usize) = (28, 31);
const DATE: (usize, usize) = (31, 39); // YYYYMMDD
const MEMO: (usize, usize) = (39, 79);
const MIN_LEN: usize = 24; // need at least ACCT + CODE + AMOUNT to be a real record

/// Slice a field window, clamped to the record length.
fn field<'a>(rec: &'a [u8], f: (usize, usize)) -> &'a [u8] {
    let end = if f.1 <= rec.len() { f.1 } else { rec.len() };
    if f.0 >= end {
        &[]
    } else {
        &rec[f.0..end]
    }
}

/// Strip only the record framing (CR/LF/NUL) — never the COBOL space padding.
fn strip_eol(b: &[u8]) -> &[u8] {
    let mut e = b.len();
    while e > 0 && (b[e - 1] == b'\n' || b[e - 1] == b'\r' || b[e - 1] == 0) {
        e -= 1;
    }
    &b[..e]
}

/// Trim trailing COBOL space padding from a single field.
fn rtrim(b: &[u8]) -> &[u8] {
    let mut e = b.len();
    while e > 0 && b[e - 1] == b' ' {
        e -= 1;
    }
    &b[..e]
}

fn push_json_string(out: &mut String, b: &[u8]) {
    out.push('"');
    for &c in b {
        match c {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x00..=0x1f => {} // drop other control chars
            _ => out.push(c as char),
        }
    }
    out.push('"');
}

fn push_str_field(out: &mut String, key: &str, rec: &[u8], f: (usize, usize)) {
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    push_json_string(out, rtrim(field(rec, f)));
}

/// AMOUNT is PIC 9(10)V99 — 12 zero-padded digits, last two are cents.
fn push_amount(out: &mut String, rec: &[u8]) {
    let raw = field(rec, AMT);
    let mut digits: Vec<u8> = Vec::with_capacity(12);
    for &c in raw {
        if c.is_ascii_digit() {
            digits.push(c);
        }
    }
    while digits.len() < 3 {
        digits.insert(0, b'0');
    }
    let n = digits.len();
    let whole = &digits[..n - 2];
    let cents = &digits[n - 2..];
    let mut ws = 0;
    while ws + 1 < whole.len() && whole[ws] == b'0' {
        ws += 1; // strip leading zeros, keep at least one digit
    }
    out.push_str("\"amount\":");
    for &c in &whole[ws..] {
        out.push(c as char);
    }
    out.push('.');
    for &c in cents {
        out.push(c as char);
    }
}

fn parse(event: &[u8]) -> Vec<u8> {
    let rec = strip_eol(event);
    if rec.len() < MIN_LEN {
        return Vec::new(); // out_len == 0 → the engine drops the event
    }

    let mut out = String::with_capacity(256);
    out.push('{');
    push_str_field(&mut out, "accountId", rec, ACCT);
    out.push(',');
    push_str_field(&mut out, "txnCode", rec, CODE);
    out.push(',');
    push_amount(&mut out, rec);
    out.push(',');
    push_str_field(&mut out, "productCode", rec, PROD);
    out.push(',');
    push_str_field(&mut out, "currency", rec, CCY);
    out.push(',');

    // POST-DATE YYYYMMDD → ISO YYYY-MM-DD when well-formed.
    let d = rtrim(field(rec, DATE));
    out.push_str("\"postDate\":");
    if d.len() == 8 && d.iter().all(u8::is_ascii_digit) {
        out.push('"');
        for &c in &d[0..4] {
            out.push(c as char);
        }
        out.push('-');
        for &c in &d[4..6] {
            out.push(c as char);
        }
        out.push('-');
        for &c in &d[6..8] {
            out.push(c as char);
        }
        out.push('"');
    } else {
        push_json_string(&mut out, d);
    }
    out.push(',');

    push_str_field(&mut out, "memo", rec, MEMO);
    out.push_str(",\"_source\":\"cobol-copybook\"}");
    out.into_bytes()
}

pulse_wasm_guest::operator!(|event: &[u8]| -> Vec<u8> { parse(event) });
