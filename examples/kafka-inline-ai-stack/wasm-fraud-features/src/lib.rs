//! Kafka transaction record → fraud feature vector, as a sandboxed Pulse WASM operator.
//!
//! The Kafka source connector wraps every consumed record in an envelope:
//! `{"topic":"transactions","key":"...","payload":"<the raw record value>","ts":...}`.
//! The original producer JSON lands, escaped, in the `payload` string. This module
//! pulls it out, derives the four risk signals a fraud model actually wants, and
//! emits a flat JSON the ONNX `mlPredict` operator scores in the same streaming pass.
//! Zero host imports, no syscalls — `pulse_wasm_guest` supplies the alloc/process/memory
//! ABI and the panic handler.
//!
//! In  (inner payload, e.g.):
//! `{"txId":"t-1009","account":"acc-77","amount":4200,"merchant":"SkyMart",
//!   "country":"FR","cardPresent":false,"hourOfDay":3,"velocity24h":9}`
//!
//! Out:
//! `{"txId":"t-1009","account":"acc-77","amount":4200,"merchant":"SkyMart","country":"FR",
//!   "amount_f":4200,"is_foreign":1,"card_not_present":1,"night":1,"velocity_24h":9,
//!   "_source":"fraud-features"}`
#![no_std]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write;

/// The home country — transactions outside it score `is_foreign = 1`.
const HOME_COUNTRY: &[u8] = b"US";

/// Index just past the closing quote of the literal token `"key"`.
fn find_key(j: &[u8], key: &str) -> Option<usize> {
    let k = key.as_bytes();
    if k.is_empty() || j.len() < k.len() + 2 {
        return None;
    }
    let mut i = 0;
    while i + k.len() + 2 <= j.len() {
        if j[i] == b'"' && j[i + 1 + k.len()] == b'"' && &j[i + 1..i + 1 + k.len()] == k {
            return Some(i + k.len() + 2);
        }
        i += 1;
    }
    None
}

/// Advance `i` to the first byte of the value after `"key"` (past the colon
/// and any whitespace). Returns None if the key is absent.
fn value_start(j: &[u8], key: &str) -> Option<usize> {
    let mut i = find_key(j, key)?;
    while i < j.len() && j[i] != b':' {
        i += 1;
    }
    i += 1;
    while i < j.len() && (j[i] == b' ' || j[i] == b'\t') {
        i += 1;
    }
    Some(i)
}

/// Integer value of the first `"key":N` in `j` (0 if absent / non-numeric).
fn json_int(j: &[u8], key: &str) -> i64 {
    let Some(mut i) = value_start(j, key) else { return 0 };
    let neg = i < j.len() && j[i] == b'-';
    if neg {
        i += 1;
    }
    let mut v: i64 = 0;
    let mut any = false;
    while i < j.len() && j[i].is_ascii_digit() {
        v = v * 10 + (j[i] - b'0') as i64;
        i += 1;
        any = true;
    }
    if !any {
        0
    } else if neg {
        -v
    } else {
        v
    }
}

/// Boolean value of the first `"key":true|false` in `j` (false if absent).
fn json_bool(j: &[u8], key: &str) -> bool {
    let Some(i) = value_start(j, key) else { return false };
    j.len() >= i + 4 && &j[i..i + 4] == b"true"
}

/// String value of the first `"key":"…"` in `j`, with `\"` and `\\`
/// unescaped (empty if absent). Also used to pull the escaped inner
/// payload back out of the Kafka envelope.
fn json_string(j: &[u8], key: &str) -> Vec<u8> {
    let Some(i) = value_start(j, key) else { return Vec::new() };
    let mut i = i;
    if i >= j.len() || j[i] != b'"' {
        return Vec::new();
    }
    i += 1;
    let mut out = Vec::new();
    while i < j.len() {
        let c = j[i];
        if c == b'\\' && i + 1 < j.len() {
            out.push(j[i + 1]);
            i += 2;
            continue;
        }
        if c == b'"' {
            break;
        }
        out.push(c);
        i += 1;
    }
    out
}

fn push_str(out: &mut String, key: &str, val: &[u8]) {
    out.push('"');
    out.push_str(key);
    out.push_str("\":\"");
    for &c in val {
        match c {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x00..=0x1f => {}
            _ => out.push(c as char),
        }
    }
    out.push('"');
}

fn push_int(out: &mut String, key: &str, v: i64) {
    out.push('"');
    out.push_str(key);
    out.push_str("\":");
    let _ = write!(out, "{}", v);
}

fn parse(event: &[u8]) -> Vec<u8> {
    // The Kafka source nests the original record under "payload" (escaped). Pull
    // it back out; if there's no envelope (a direct feed), score the event as-is.
    let payload = json_string(event, "payload");
    let j: &[u8] = if payload.is_empty() { event } else { &payload };

    // txId identifies the transaction; absent it, the event isn't scorable.
    let tx_id = json_string(j, "txId");
    if tx_id.is_empty() {
        return Vec::new(); // out_len == 0 → engine drops it
    }

    let amount = json_int(j, "amount");
    let country = json_string(j, "country");
    let is_foreign = if !country.is_empty() && country.as_slice() != HOME_COUNTRY { 1 } else { 0 };
    let card_not_present = if json_bool(j, "cardPresent") { 0 } else { 1 };
    let hour = json_int(j, "hourOfDay");
    let night = if hour < 6 || hour >= 23 { 1 } else { 0 };
    let velocity = json_int(j, "velocity24h");

    let mut out = String::with_capacity(256);
    out.push('{');
    // Identifying fields carried through so the enriched Kafka record stays useful.
    push_str(&mut out, "txId", &tx_id);
    out.push(',');
    push_str(&mut out, "account", &json_string(j, "account"));
    out.push(',');
    push_int(&mut out, "amount", amount);
    out.push(',');
    push_str(&mut out, "merchant", &json_string(j, "merchant"));
    out.push(',');
    push_str(&mut out, "country", &country);
    // The five features, flat, in the order the ONNX model expects.
    out.push(',');
    push_int(&mut out, "amount_f", amount);
    out.push(',');
    push_int(&mut out, "is_foreign", is_foreign);
    out.push(',');
    push_int(&mut out, "card_not_present", card_not_present);
    out.push(',');
    push_int(&mut out, "night", night);
    out.push(',');
    push_int(&mut out, "velocity_24h", velocity);
    out.push_str(",\"_source\":\"fraud-features\"}");
    out.into_bytes()
}

pulse_wasm_guest::operator!(|event: &[u8]| -> Vec<u8> { parse(event) });
