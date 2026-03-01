//! Cache abstraction for Rust server parity.
//!
//! Phase A uses in-memory/no-op behavior.
//! Future phases add Upstash REST cache parity with key prefixing.

use serde_json::Value;

#[allow(dead_code)]
pub trait Cache: Send + Sync {
    fn get_json(&self, key: &str) -> Option<Value>;
    fn set_json(&self, key: &str, value: Value, ttl_seconds: u64);
}

#[derive(Debug, Default)]
pub struct NoopCache;

impl Cache for NoopCache {
    fn get_json(&self, _key: &str) -> Option<Value> {
        None
    }

    fn set_json(&self, _key: &str, _value: Value, _ttl_seconds: u64) {}
}
