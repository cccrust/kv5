use dashmap::DashMap;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[allow(unused_imports)]
use super::helpers::{glob_match, matches_pattern};
use super::sortedset::SortedSet;
use super::types::{Entry, Snapshot, Value};
use crate::pubsub::PubSub;

pub struct Store {
    data: Arc<DashMap<String, Entry>>,
    persist_path: Option<String>,
    _lock: Arc<RwLock<()>>,
    pub pubsub: PubSub,
    key_version: Arc<DashMap<String, u64>>,
}

impl Store {
    pub fn new(persist_path: Option<String>) -> Self {
        let store = Store {
            data: Arc::new(DashMap::new()),
            persist_path: persist_path.clone(),
            _lock: Arc::new(RwLock::new(())),
            pubsub: PubSub::new(),
            key_version: Arc::new(DashMap::new()),
        };

        if let Some(ref path) = persist_path {
            store.load_from_disk(path);
        }

        store
    }

    pub fn get_key_version(&self, key: &str) -> u64 {
        self.key_version.get(key).map(|v| *v.value()).unwrap_or(0)
    }

    fn increment_key_version(&self, key: &str) {
        let mut version = self.key_version.entry(key.to_string()).or_insert(0);
        *version += 1;
    }

    pub fn get_keys_versions(&self, keys: &[&str]) -> Vec<(String, u64)> {
        keys.iter()
            .map(|k| (k.to_string(), self.get_key_version(k)))
            .collect()
    }

    fn load_from_disk(&self, path: &str) {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                if let Ok(snapshot) = serde_json::from_str::<Snapshot>(&contents) {
                    for (key, value, ttl_ms) in snapshot.entries {
                        let entry = match ttl_ms {
                            Some(ms) if ms > 0 => {
                                Entry::with_expiry(value, Duration::from_millis(ms))
                            }
                            _ => Entry::new(value),
                        };
                        self.data.insert(key, entry);
                    }
                    tracing::info!("Loaded snapshot from {}", path);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!("Failed to load snapshot: {}", e),
        }
    }

    pub fn save_to_disk(&self) -> anyhow::Result<()> {
        let path = match &self.persist_path {
            Some(p) => p.clone(),
            None => return Ok(()),
        };

        let entries: Vec<(String, Value, Option<u64>)> = self
            .data
            .iter()
            .filter(|e| !e.value().is_expired())
            .map(|e| {
                let ttl_ms = e.value().expires_at.map(|exp| {
                    let now = Instant::now();
                    if exp > now {
                        (exp - now).as_millis() as u64
                    } else {
                        0
                    }
                });
                (e.key().clone(), e.value().value.clone(), ttl_ms)
            })
            .collect();

        let snapshot = Snapshot { entries };
        let json = serde_json::to_string_pretty(&snapshot)?;
        std::fs::write(&path, json)?;
        tracing::info!("Saved snapshot to {}", path);
        Ok(())
    }

    pub fn start_expiry_task(store: Arc<Store>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let expired_keys: Vec<String> = store
                    .data
                    .iter()
                    .filter(|e| e.value().is_expired())
                    .map(|e| e.key().clone())
                    .collect();

                for key in expired_keys {
                    store.data.remove(&key);
                }
            }
        });
    }

    pub fn set(&self, key: String, value: String) -> &'static str {
        self.data
            .insert(key.clone(), Entry::new(Value::String(value)));
        self.increment_key_version(&key);
        "OK"
    }

    pub fn setex(&self, key: String, value: String, ttl_secs: u64) -> &'static str {
        self.data.insert(
            key.clone(),
            Entry::with_expiry(Value::String(value), Duration::from_secs(ttl_secs)),
        );
        self.increment_key_version(&key);
        "OK"
    }

    pub fn psetex(&self, key: String, value: String, ttl_ms: u64) -> &'static str {
        self.data.insert(
            key.clone(),
            Entry::with_expiry(Value::String(value), Duration::from_millis(ttl_ms)),
        );
        self.increment_key_version(&key);
        "OK"
    }

    pub fn setnx(&self, key: String, value: String) -> i64 {
        if self.exists(&key) {
            0
        } else {
            self.data
                .insert(key.clone(), Entry::new(Value::String(value)));
            self.increment_key_version(&key);
            1
        }
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let entry = self.data.get(key)?;
        if entry.is_expired() {
            drop(entry);
            self.data.remove(key);
            return None;
        }
        match &entry.value {
            Value::String(s) => Some(s.clone()),
            Value::Integer(i) => Some(i.to_string()),
            _ => Some("(error) WRONGTYPE".to_string()),
        }
    }

    pub fn getset(&self, key: String, value: String) -> Option<String> {
        let old = self.get(&key);
        self.set(key, value);
        old
    }

    pub fn mset(&self, pairs: Vec<(String, String)>) -> &'static str {
        for (k, v) in pairs {
            self.set(k, v);
        }
        "OK"
    }

    pub fn mget(&self, keys: Vec<&str>) -> Vec<Option<String>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    pub fn append(&self, key: &str, value: &str) -> i64 {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::String(String::new())));
        match &mut entry.value {
            Value::String(s) => {
                s.push_str(value);
                s.len() as i64
            }
            _ => -1,
        }
    }

    pub fn strlen(&self, key: &str) -> i64 {
        match self.get(key) {
            Some(s) => s.len() as i64,
            None => 0,
        }
    }

    pub fn incr(&self, key: &str) -> anyhow::Result<i64> {
        self.incrby(key, 1)
    }

    pub fn decr(&self, key: &str) -> anyhow::Result<i64> {
        self.incrby(key, -1)
    }

    pub fn incrby(&self, key: &str, delta: i64) -> anyhow::Result<i64> {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::Integer(0)));

        match &mut entry.value {
            Value::Integer(i) => {
                *i += delta;
                Ok(*i)
            }
            Value::String(s) => {
                let parsed: i64 = s
                    .parse()
                    .map_err(|_| anyhow::anyhow!("value is not an integer"))?;
                let result = parsed + delta;
                entry.value = Value::Integer(result);
                Ok(result)
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE")),
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        match self.data.get(key) {
            Some(entry) if !entry.is_expired() => true,
            Some(_) => {
                self.data.remove(key);
                false
            }
            None => false,
        }
    }

    pub fn del(&self, keys: Vec<&str>) -> i64 {
        keys.iter()
            .filter(|k| {
                if self.data.remove(**k).is_some() {
                    self.increment_key_version(k);
                    true
                } else {
                    false
                }
            })
            .count() as i64
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.data
            .iter()
            .filter(|e| !e.value().is_expired() && matches_pattern(e.key(), pattern))
            .map(|e| e.key().clone())
            .collect()
    }

    pub fn type_of(&self, key: &str) -> &'static str {
        match self.data.get(key) {
            Some(e) if !e.is_expired() => e.value.type_name(),
            _ => "none",
        }
    }

    pub fn expire(&self, key: &str, secs: u64) -> i64 {
        match self.data.get_mut(key) {
            Some(mut e) if !e.is_expired() => {
                e.expires_at = Some(Instant::now() + Duration::from_secs(secs));
                1
            }
            _ => 0,
        }
    }

    pub fn pexpire(&self, key: &str, ms: u64) -> i64 {
        match self.data.get_mut(key) {
            Some(mut e) if !e.is_expired() => {
                e.expires_at = Some(Instant::now() + Duration::from_millis(ms));
                1
            }
            _ => 0,
        }
    }

    pub fn persist(&self, key: &str) -> i64 {
        match self.data.get_mut(key) {
            Some(mut e) if !e.is_expired() && e.expires_at.is_some() => {
                e.expires_at = None;
                1
            }
            _ => 0,
        }
    }

    pub fn ttl(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(e) if !e.is_expired() => e.ttl_secs().unwrap_or(-1),
            Some(_) => -2,
            None => -2,
        }
    }

    pub fn pttl(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(e) if !e.is_expired() => e
                .expires_at
                .map(|exp| {
                    let now = Instant::now();
                    if exp > now {
                        (exp - now).as_millis() as i64
                    } else {
                        -2
                    }
                })
                .unwrap_or(-1),
            _ => -2,
        }
    }

    pub fn rename(&self, key: &str, newkey: &str) -> anyhow::Result<&'static str> {
        match self.data.remove(key) {
            Some((_, entry)) if !entry.is_expired() => {
                self.data.insert(newkey.to_string(), entry);
                Ok("OK")
            }
            _ => Err(anyhow::anyhow!("ERR no such key")),
        }
    }

    pub fn dbsize(&self) -> usize {
        self.data.iter().filter(|e| !e.value().is_expired()).count()
    }

    pub fn flushdb(&self) -> &'static str {
        self.data.clear();
        "OK"
    }

    pub fn lpush(&self, key: &str, values: Vec<&str>) -> anyhow::Result<i64> {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::List(VecDeque::new())));
        match &mut entry.value {
            Value::List(list) => {
                for v in values {
                    list.push_front(v.to_string());
                }
                Ok(list.len() as i64)
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE")),
        }
    }

    pub fn rpush(&self, key: &str, values: Vec<&str>) -> anyhow::Result<i64> {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::List(VecDeque::new())));
        match &mut entry.value {
            Value::List(list) => {
                for v in values {
                    list.push_back(v.to_string());
                }
                Ok(list.len() as i64)
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE")),
        }
    }

    pub fn lpop(&self, key: &str) -> Option<String> {
        let mut entry = self.data.get_mut(key)?;
        match &mut entry.value {
            Value::List(list) => list.pop_front(),
            _ => None,
        }
    }

    pub fn rpop(&self, key: &str) -> Option<String> {
        let mut entry = self.data.get_mut(key)?;
        match &mut entry.value {
            Value::List(list) => list.pop_back(),
            _ => None,
        }
    }

    pub fn llen(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::List(list) => list.len() as i64,
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    } as usize;
                    let e = if stop < 0 {
                        (len + stop + 1).max(0)
                    } else {
                        (stop + 1).min(len)
                    } as usize;
                    list.iter()
                        .skip(s)
                        .take(e.saturating_sub(s))
                        .cloned()
                        .collect()
                }
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn lindex(&self, key: &str, index: i64) -> Option<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::List(list) => {
                    let len = list.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx < 0 || idx >= len {
                        None
                    } else {
                        list.get(idx as usize).cloned()
                    }
                }
                _ => None,
            },
            None => None,
        }
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> anyhow::Result<i64> {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::Hash(HashMap::new())));
        match &mut entry.value {
            Value::Hash(map) => {
                let is_new = !map.contains_key(field);
                map.insert(field.to_string(), value.to_string());
                Ok(if is_new { 1 } else { 0 })
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE")),
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<String> {
        let e = self.data.get(key)?;
        match &e.value {
            Value::Hash(map) => map.get(field).cloned(),
            _ => None,
        }
    }

    pub fn hmset(&self, key: &str, pairs: Vec<(&str, &str)>) -> anyhow::Result<&'static str> {
        for (f, v) in pairs {
            self.hset(key, f, v)?;
        }
        Ok("OK")
    }

    pub fn hmget(&self, key: &str, fields: Vec<&str>) -> Vec<Option<String>> {
        fields.iter().map(|f| self.hget(key, f)).collect()
    }

    pub fn hdel(&self, key: &str, fields: Vec<&str>) -> i64 {
        match self.data.get_mut(key) {
            Some(mut e) => match &mut e.value {
                Value::Hash(map) => {
                    fields.iter().filter(|f| map.remove(**f).is_some()).count() as i64
                }
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn hgetall(&self, key: &str) -> Vec<(String, String)> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Hash(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn hkeys(&self, key: &str) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Hash(map) => map.keys().cloned().collect(),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn hvals(&self, key: &str) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Hash(map) => map.values().cloned().collect(),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn hlen(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Hash(map) => map.len() as i64,
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn hexists(&self, key: &str, field: &str) -> bool {
        self.hget(key, field).is_some()
    }

    pub fn sadd(&self, key: &str, members: Vec<&str>) -> anyhow::Result<i64> {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::Set(HashSet::new())));
        match &mut entry.value {
            Value::Set(set) => {
                let added = members.iter().filter(|m| set.insert(m.to_string())).count();
                Ok(added as i64)
            }
            _ => Err(anyhow::anyhow!("WRONGTYPE")),
        }
    }

    pub fn srem(&self, key: &str, members: Vec<&str>) -> i64 {
        match self.data.get_mut(key) {
            Some(mut e) => match &mut e.value {
                Value::Set(set) => members.iter().filter(|m| set.remove(**m)).count() as i64,
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn smembers(&self, key: &str) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Set(set) => set.iter().cloned().collect(),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn sismember(&self, key: &str, member: &str) -> bool {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Set(set) => set.contains(member),
                _ => false,
            },
            None => false,
        }
    }

    pub fn scard(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::Set(set) => set.len() as i64,
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn zadd(&self, key: &str, score: f64, member: &str) -> i64 {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::SortedSet(SortedSet::new())));
        match &mut entry.value {
            Value::SortedSet(zset) => zset.zadd(score, member),
            _ => -1,
        }
    }

    pub fn zrange(&self, key: &str, start: i64, stop: i64, withscores: bool) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zrange(start, stop, withscores),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn zrangebyscore(&self, key: &str, min: f64, max: f64, withscores: bool) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zrangebyscore(min, max, withscores),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn zrevrange(&self, key: &str, start: i64, stop: i64, withscores: bool) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zrevrange(start, stop, withscores),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn zrevrangebyscore(&self, key: &str, max: f64, min: f64, withscores: bool) -> Vec<String> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zrevrangebyscore(max, min, withscores),
                _ => vec![],
            },
            None => vec![],
        }
    }

    pub fn zrank(&self, key: &str, member: &str) -> Option<i64> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zrank(member),
                _ => None,
            },
            None => None,
        }
    }

    pub fn zrevrank(&self, key: &str, member: &str) -> Option<i64> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zrevrank(member),
                _ => None,
            },
            None => None,
        }
    }

    pub fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zscore(member),
                _ => None,
            },
            None => None,
        }
    }

    pub fn zrem(&self, key: &str, members: Vec<&str>) -> i64 {
        match self.data.get_mut(key) {
            Some(mut e) => match &mut e.value {
                Value::SortedSet(zset) => zset.zrem(&members),
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn zcard(&self, key: &str) -> i64 {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zcard(),
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn zcount(&self, key: &str, min: f64, max: f64) -> i64 {
        match self.data.get(key) {
            Some(e) => match &e.value {
                Value::SortedSet(zset) => zset.zcount(min, max),
                _ => 0,
            },
            None => 0,
        }
    }

    pub fn zincrby(&self, key: &str, increment: f64, member: &str) -> f64 {
        let mut entry = self
            .data
            .entry(key.to_string())
            .or_insert(Entry::new(Value::SortedSet(SortedSet::new())));
        match &mut entry.value {
            Value::SortedSet(zset) => zset.zincrby(increment, member),
            _ => 0.0,
        }
    }

    pub fn getbit(&self, key: &str, offset: usize) -> i64 {
        let value = match self.get(key) {
            Some(v) => v,
            None => return 0,
        };
        let bytes = value.as_bytes();
        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);
        if byte_idx >= bytes.len() {
            return 0;
        }
        ((bytes[byte_idx] >> bit_idx) & 1) as i64
    }

    pub fn setbit(&self, key: &str, offset: usize, value: u8) -> i64 {
        let old_value = self.get(key).unwrap_or_default();
        let old_was_empty = old_value.is_empty();
        let mut bytes = old_value.into_bytes();

        let byte_idx = offset / 8;
        let bit_idx = 7 - (offset % 8);

        while bytes.len() <= byte_idx {
            bytes.push(0);
        }

        let old_bit = (bytes[byte_idx] >> bit_idx) & 1;

        if value != 0 {
            bytes[byte_idx] |= 1 << bit_idx;
        } else {
            bytes[byte_idx] &= !(1 << bit_idx);
        }

        let new_value = String::from_utf8(bytes).unwrap_or_default();
        if new_value.is_empty() || old_was_empty {
            self.data.insert(
                key.to_string(),
                Entry::new(Value::String(new_value.clone())),
            );
        } else {
            self.set(key.to_string(), new_value);
        }

        self.increment_key_version(key);
        old_bit as i64
    }

    pub fn bitcount(&self, key: &str, start: Option<i64>, end: Option<i64>) -> i64 {
        let value = match self.get(key) {
            Some(v) => v,
            None => return 0,
        };
        let bytes = value.into_bytes();

        let (start_byte, end_byte) = match (start, end) {
            (Some(s), Some(e)) => {
                let s = if s < 0 {
                    (bytes.len() as i64 + s).max(0) as usize
                } else {
                    s as usize
                };
                let e = if e < 0 {
                    (bytes.len() as i64 + e).max(0) as usize
                } else {
                    e as usize
                };
                (s.min(bytes.len()), e.min(bytes.len().saturating_sub(1)))
            }
            _ => (0, bytes.len().saturating_sub(1)),
        };

        if start_byte > end_byte {
            return 0;
        }

        let mut count = 0i64;
        #[allow(clippy::needless_range_loop)]
        for i in start_byte..=end_byte {
            count += bytes[i].count_ones() as i64;
        }
        count
    }

    pub fn scan(&self, cursor: usize, pattern: Option<&str>, count: usize) -> (usize, Vec<String>) {
        let all_keys: Vec<String> = self
            .data
            .iter()
            .filter(|e| !e.value().is_expired())
            .map(|e| e.key().clone())
            .collect();

        if all_keys.is_empty() {
            return (0, vec![]);
        }

        let total = all_keys.len();
        let start = cursor % total;
        let mut result = Vec::new();
        let mut visited = 0;

        let mut idx = start;
        while visited < count {
            let key = &all_keys[idx];
            match pattern {
                Some(p) if !matches_pattern(key, p) => {}
                _ => result.push(key.clone()),
            }
            visited += 1;
            idx = (idx + 1) % total;
            if idx == start {
                break;
            }
        }

        let next_cursor = if idx == start {
            0
        } else {
            (cursor + count) % total
        };
        (next_cursor, result)
    }

    pub fn sscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> (usize, Vec<String>) {
        let members: Vec<String> = match self.data.get(key) {
            Some(e) if !e.is_expired() => match &e.value {
                Value::Set(set) => set.iter().cloned().collect(),
                _ => return (0, vec![]),
            },
            _ => return (0, vec![]),
        };

        if members.is_empty() {
            return (0, vec![]);
        }

        let total = members.len();
        let start = cursor % total;
        let mut result = Vec::new();
        let mut visited = 0;

        let mut idx = start;
        while visited < count {
            let member = &members[idx];
            match pattern {
                Some(p) if !matches_pattern(member, p) => {}
                _ => result.push(member.clone()),
            }
            visited += 1;
            idx = (idx + 1) % total;
            if idx == start {
                break;
            }
        }

        let next_cursor = if idx == start {
            0
        } else {
            (cursor + count) % total
        };
        (next_cursor, result)
    }

    pub fn hscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> (usize, Vec<String>) {
        let fields: Vec<String> = match self.data.get(key) {
            Some(e) if !e.is_expired() => match &e.value {
                Value::Hash(map) => map.keys().cloned().collect(),
                _ => return (0, vec![]),
            },
            _ => return (0, vec![]),
        };

        if fields.is_empty() {
            return (0, vec![]);
        }

        let total = fields.len();
        let start = cursor % total;
        let mut result = Vec::new();
        let mut visited = 0;

        let mut idx = start;
        while visited < count {
            let field = &fields[idx];
            match pattern {
                Some(p) if !matches_pattern(field, p) => {}
                _ => result.push(field.clone()),
            }
            visited += 1;
            idx = (idx + 1) % total;
            if idx == start {
                break;
            }
        }

        let next_cursor = if idx == start {
            0
        } else {
            (cursor + count) % total
        };
        (next_cursor, result)
    }

    pub fn zscan(
        &self,
        key: &str,
        cursor: usize,
        pattern: Option<&str>,
        count: usize,
    ) -> (usize, Vec<String>) {
        let members: Vec<String> = match self.data.get(key) {
            Some(e) if !e.is_expired() => match &e.value {
                Value::SortedSet(zset) => zset.members(),
                _ => return (0, vec![]),
            },
            _ => return (0, vec![]),
        };

        if members.is_empty() {
            return (0, vec![]);
        }

        let total = members.len();
        let start = cursor % total;
        let mut result = Vec::new();
        let mut visited = 0;

        let mut idx = start;
        while visited < count {
            let member = &members[idx];
            match pattern {
                Some(p) if !matches_pattern(member, p) => {}
                _ => result.push(member.clone()),
            }
            visited += 1;
            idx = (idx + 1) % total;
            if idx == start {
                break;
            }
        }

        let next_cursor = if idx == start {
            0
        } else {
            (cursor + count) % total
        };
        (next_cursor, result)
    }

    pub fn bitop(&self, op: &str, destkey: &str, keys: &[&str]) -> i64 {
        if keys.is_empty() {
            self.set(destkey.to_string(), String::new());
            return 0;
        }

        let mut result_bytes: Vec<u8> = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            let value = self.get(key).unwrap_or_default();
            let bytes = value.into_bytes();

            if i == 0 {
                result_bytes = bytes;
            } else {
                match op.to_uppercase().as_str() {
                    "AND" => {
                        for (j, &b) in bytes.iter().enumerate() {
                            if j < result_bytes.len() {
                                result_bytes[j] &= b;
                            }
                        }
                    }
                    "OR" => {
                        for (j, &b) in bytes.iter().enumerate() {
                            if j < result_bytes.len() {
                                result_bytes[j] |= b;
                            } else {
                                result_bytes.push(b);
                            }
                        }
                    }
                    "XOR" => {
                        for (j, &b) in bytes.iter().enumerate() {
                            if j < result_bytes.len() {
                                result_bytes[j] ^= b;
                            } else {
                                result_bytes.push(b);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if op.to_uppercase() == "NOT" && !keys.is_empty() {
            result_bytes = self.get(keys[0]).unwrap_or_default().into_bytes();
            for byte in &mut result_bytes {
                *byte = !*byte;
            }
        }

        let result_len = result_bytes.len() as i64;
        self.set(
            destkey.to_string(),
            String::from_utf8(result_bytes).unwrap_or_default(),
        );
        result_len
    }

    pub fn bitpos(&self, key: &str, bit: u8, start: Option<i64>, end: Option<i64>) -> i64 {
        let value = match self.get(key) {
            Some(v) => v,
            None => {
                if bit == 0 {
                    return 0;
                }
                return -1;
            }
        };
        let bytes = value.into_bytes();

        if bytes.is_empty() {
            return if bit == 0 { 0 } else { -1 };
        }

        let start_byte = start
            .map(|s| {
                if s < 0 {
                    (bytes.len() as i64 + s).max(0) as usize
                } else {
                    s as usize
                }
            })
            .unwrap_or(0);

        let end_byte = end
            .map(|e| {
                if e < 0 {
                    (bytes.len() as i64 + e).max(0) as usize
                } else {
                    e as usize
                }
            })
            .unwrap_or(bytes.len().saturating_sub(1));

        let start_byte = start_byte.min(bytes.len());
        let end_byte = end_byte.min(bytes.len().saturating_sub(1));

        if start_byte > end_byte {
            return -1;
        }

        #[allow(clippy::needless_range_loop)]
        for i in start_byte..=end_byte {
            let byte = bytes[i];
            for j in 0..8 {
                let bit_pos = 7 - j;
                let actual_bit = (byte >> bit_pos) & 1;
                if actual_bit == bit {
                    return (i * 8 + j) as i64;
                }
            }
        }

        -1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_store() -> Store {
        Store::new(None)
    }

    #[test]
    fn test_set_get() {
        let s = new_store();
        s.set("k".into(), "v".into());
        assert_eq!(s.get("k"), Some("v".into()));
        assert_eq!(s.get("missing"), None);
    }

    #[test]
    fn test_setnx() {
        let s = new_store();
        assert_eq!(s.setnx("k".into(), "first".into()), 1);
        assert_eq!(s.setnx("k".into(), "second".into()), 0);
        assert_eq!(s.get("k"), Some("first".into()));
    }

    #[test]
    fn test_mset_mget() {
        let s = new_store();
        s.mset(vec![("a".into(), "1".into()), ("b".into(), "2".into())]);
        let r = s.mget(vec!["a", "b", "c"]);
        assert_eq!(r, vec![Some("1".into()), Some("2".into()), None]);
    }

    #[test]
    fn test_incr_decr() {
        let s = new_store();
        assert_eq!(s.incr("n").unwrap(), 1);
        assert_eq!(s.incr("n").unwrap(), 2);
        assert_eq!(s.incrby("n", 8).unwrap(), 10);
        assert_eq!(s.decr("n").unwrap(), 9);
    }

    #[test]
    fn test_del_exists() {
        let s = new_store();
        s.set("k".into(), "v".into());
        assert!(s.exists("k"));
        assert_eq!(s.del(vec!["k", "missing"]), 1);
        assert!(!s.exists("k"));
    }

    #[test]
    fn test_ttl_expire() {
        let s = new_store();
        s.setex("k".into(), "v".into(), 100);
        let ttl = s.ttl("k");
        assert!(ttl > 98 && ttl <= 100, "ttl was {}", ttl);
        s.persist("k");
        assert_eq!(s.ttl("k"), -1);
    }

    #[test]
    fn test_list_operations() {
        let s = new_store();
        s.rpush("lst", vec!["a", "b", "c"]).unwrap();
        assert_eq!(s.llen("lst"), 3);
        assert_eq!(s.lrange("lst", 0, -1), vec!["a", "b", "c"]);
        assert_eq!(s.lpop("lst"), Some("a".into()));
        assert_eq!(s.rpop("lst"), Some("c".into()));
        assert_eq!(s.llen("lst"), 1);
    }

    #[test]
    fn test_hash_operations() {
        let s = new_store();
        assert_eq!(s.hset("h", "f1", "v1").unwrap(), 1);
        assert_eq!(s.hset("h", "f1", "v2").unwrap(), 0);
        assert_eq!(s.hget("h", "f1"), Some("v2".into()));
        assert_eq!(s.hlen("h"), 1);
        assert!(s.hexists("h", "f1"));
        assert_eq!(s.hdel("h", vec!["f1"]), 1);
        assert_eq!(s.hget("h", "f1"), None);
    }

    #[test]
    fn test_set_operations() {
        let s = new_store();
        assert_eq!(s.sadd("st", vec!["a", "b", "c"]).unwrap(), 3);
        assert_eq!(s.sadd("st", vec!["a"]).unwrap(), 0);
        assert_eq!(s.scard("st"), 3);
        assert!(s.sismember("st", "b"));
        assert_eq!(s.srem("st", vec!["b"]), 1);
        assert!(!s.sismember("st", "b"));
    }

    #[test]
    fn test_keys_pattern() {
        let s = new_store();
        s.set("user:1".into(), "a".into());
        s.set("user:2".into(), "b".into());
        s.set("post:1".into(), "c".into());
        let mut user_keys = s.keys("user:*");
        user_keys.sort();
        assert_eq!(user_keys, vec!["user:1", "user:2"]);
        assert_eq!(s.keys("*").len(), 3);
    }

    #[test]
    fn test_rename() {
        let s = new_store();
        s.set("old".into(), "val".into());
        s.rename("old", "new").unwrap();
        assert_eq!(s.get("new"), Some("val".into()));
        assert_eq!(s.get("old"), None);
    }

    #[test]
    fn test_type_of() {
        let s = new_store();
        s.set("str".into(), "v".into());
        s.rpush("lst", vec!["x"]).unwrap();
        s.hset("hsh", "f", "v").unwrap();
        s.sadd("st", vec!["m"]).unwrap();
        assert_eq!(s.type_of("str"), "string");
        assert_eq!(s.type_of("lst"), "list");
        assert_eq!(s.type_of("hsh"), "hash");
        assert_eq!(s.type_of("st"), "set");
        assert_eq!(s.type_of("none"), "none");
    }

    #[test]
    fn test_glob_pattern() {
        assert!(matches_pattern("hello", "*"));
        assert!(matches_pattern("hello", "h*"));
        assert!(matches_pattern("hello", "h?llo"));
        assert!(!matches_pattern("hello", "w*"));
        assert!(matches_pattern("user:123", "user:*"));
        assert!(!matches_pattern("post:1", "user:*"));
    }

    #[test]
    fn test_sortedset_operations() {
        let s = new_store();
        assert_eq!(s.zadd("myset", 1.0, "one"), 1);
        assert_eq!(s.zadd("myset", 2.0, "two"), 1);
        assert_eq!(s.zadd("myset", 3.0, "three"), 1);
        assert_eq!(s.zcard("myset"), 3);
        assert_eq!(s.zrange("myset", 0, -1, false), vec!["one", "two", "three"]);
        assert_eq!(s.zrange("myset", 0, 1, false), vec!["one", "two"]);
        assert_eq!(
            s.zrange("myset", 0, -1, true),
            vec![
                "one".to_string(),
                "1".to_string(),
                "two".to_string(),
                "2".to_string(),
                "three".to_string(),
                "3".to_string()
            ]
        );
    }

    #[test]
    fn test_zadd_update() {
        let s = new_store();
        assert_eq!(s.zadd("myset", 1.0, "one"), 1);
        assert_eq!(s.zadd("myset", 2.0, "one"), 0);
        assert_eq!(s.zscore("myset", "one"), Some(2.0));
        assert_eq!(s.zcard("myset"), 1);
    }

    #[test]
    fn test_zrank_zrevrank() {
        let s = new_store();
        s.zadd("myset", 1.0, "a");
        s.zadd("myset", 2.0, "b");
        s.zadd("myset", 3.0, "c");
        assert_eq!(s.zrank("myset", "b"), Some(1));
        assert_eq!(s.zrank("myset", "a"), Some(0));
        assert_eq!(s.zrank("myset", "d"), None);
        assert_eq!(s.zrevrank("myset", "b"), Some(1));
        assert_eq!(s.zrevrank("myset", "c"), Some(0));
    }

    #[test]
    fn test_zrevrange() {
        let s = new_store();
        s.zadd("myset", 1.0, "a");
        s.zadd("myset", 2.0, "b");
        s.zadd("myset", 3.0, "c");
        assert_eq!(s.zrevrange("myset", 0, -1, false), vec!["c", "b", "a"]);
    }

    #[test]
    fn test_zrangebyscore() {
        let s = new_store();
        s.zadd("myset", 1.0, "a");
        s.zadd("myset", 2.0, "b");
        s.zadd("myset", 3.0, "c");
        s.zadd("myset", 4.0, "d");
        assert_eq!(
            s.zrangebyscore("myset", 2.0, 4.0, false),
            vec!["b", "c", "d"]
        );
        assert_eq!(
            s.zrangebyscore("myset", f64::NEG_INFINITY, 2.0, false),
            vec!["a", "b"]
        );
    }

    #[test]
    fn test_zcount() {
        let s = new_store();
        s.zadd("myset", 1.0, "a");
        s.zadd("myset", 2.0, "b");
        s.zadd("myset", 3.0, "c");
        s.zadd("myset", 4.0, "d");
        assert_eq!(s.zcount("myset", 2.0, 4.0), 3);
        assert_eq!(s.zcount("myset", f64::NEG_INFINITY, f64::INFINITY), 4);
    }

    #[test]
    fn test_zincrby() {
        let s = new_store();
        s.zadd("myset", 1.0, "a");
        let new_score = s.zincrby("myset", 2.0, "a");
        assert_eq!(new_score, 3.0);
        assert_eq!(s.zscore("myset", "a"), Some(3.0));
    }

    #[test]
    fn test_zrem() {
        let s = new_store();
        s.zadd("myset", 1.0, "a");
        s.zadd("myset", 2.0, "b");
        s.zadd("myset", 3.0, "c");
        assert_eq!(s.zrem("myset", vec!["a", "b"]), 2);
        assert_eq!(s.zcard("myset"), 1);
        assert_eq!(s.zrange("myset", 0, -1, false), vec!["c"]);
    }

    #[test]
    fn test_sortedset_type() {
        let s = new_store();
        assert_eq!(s.type_of("none"), "none");
        s.zadd("myset", 1.0, "a");
        assert_eq!(s.type_of("myset"), "zset");
    }

    #[test]
    fn test_bit_setget() {
        let s = new_store();
        let old = s.setbit("mykey", 7, 1);
        assert_eq!(old, 0, "first setbit should return 0");
        assert_eq!(s.getbit("mykey", 7), 1, "bit 7 should be 1 after set");
        assert_eq!(s.getbit("mykey", 0), 0, "bit 0 should still be 0");
    }
}
