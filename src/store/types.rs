use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use super::sortedset::SortedSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
    Set(HashSet<String>),
    Integer(i64),
    SortedSet(SortedSet),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Hash(_) => "hash",
            Value::Set(_) => "set",
            Value::Integer(_) => "integer",
            Value::SortedSet(_) => "zset",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Value) -> Self {
        Entry {
            value,
            expires_at: None,
        }
    }

    pub fn with_expiry(value: Value, ttl: Duration) -> Self {
        Entry {
            value,
            expires_at: Some(Instant::now() + ttl),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.map(|e| Instant::now() > e).unwrap_or(false)
    }

    pub fn ttl_secs(&self) -> Option<i64> {
        self.expires_at.map(|e| {
            let now = Instant::now();
            if now > e {
                -2
            } else {
                (e - now).as_secs() as i64
            }
        })
    }
}

#[derive(Serialize, Deserialize)]
pub struct Snapshot {
    pub entries: Vec<(String, Value, Option<u64>)>,
}
