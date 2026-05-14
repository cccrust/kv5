use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortedSet {
    entries: Vec<(f64, String)>,
    index: HashMap<String, usize>,
}

impl Default for SortedSet {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSet {
    pub fn new() -> Self {
        SortedSet {
            entries: Vec::new(),
            index: HashMap::new(),
        }
    }

    fn reindex(&mut self) {
        self.index.clear();
        for (i, (_, member)) in self.entries.iter().enumerate() {
            self.index.insert(member.clone(), i);
        }
    }

    pub fn zadd(&mut self, score: f64, member: &str) -> i64 {
        if let Some(&idx) = self.index.get(member) {
            self.entries[idx].0 = score;
            if idx > 0 && self.entries[idx].0 < self.entries[idx - 1].0 {
                self.entries
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            } else if idx + 1 < self.entries.len() && self.entries[idx].0 > self.entries[idx + 1].0
            {
                self.entries
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
            }
            self.reindex();
            return 0;
        }
        self.entries.push((score, member.to_string()));
        self.entries
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        self.index
            .insert(member.to_string(), self.entries.len() - 1);
        1
    }

    pub fn zrange(&self, start: i64, stop: i64, withscores: bool) -> Vec<String> {
        let len = self.entries.len() as i64;
        if len == 0 {
            return vec![];
        }
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };
        let stop = if stop < 0 {
            (len + stop + 1).max(0) as usize
        } else {
            (stop + 1) as usize
        };
        if withscores {
            let mut result = Vec::new();
            for i in start..stop.min(self.entries.len()) {
                result.push(self.entries[i].1.clone());
                result.push(self.entries[i].0.to_string());
            }
            result
        } else {
            self.entries[start..stop.min(self.entries.len())]
                .iter()
                .map(|(_, m)| m.clone())
                .collect()
        }
    }

    pub fn zrangebyscore(&self, min: f64, max: f64, withscores: bool) -> Vec<String> {
        let items: Vec<_> = self
            .entries
            .iter()
            .filter(|(s, _)| *s >= min && *s <= max)
            .collect();
        if withscores {
            let mut result = Vec::new();
            for (s, m) in items {
                result.push(m.clone());
                result.push(s.to_string());
            }
            result
        } else {
            items.iter().map(|(_, m)| m.clone()).collect()
        }
    }

    pub fn zrevrange(&self, start: i64, stop: i64, withscores: bool) -> Vec<String> {
        let len = self.entries.len() as i64;
        if len == 0 {
            return vec![];
        }
        let start = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };
        let stop = if stop < 0 {
            (len + stop + 1).max(0) as usize
        } else {
            (stop + 1) as usize
        };
        let rev: Vec<_> = self
            .entries
            .iter()
            .rev()
            .skip(start)
            .take(stop.saturating_sub(start))
            .collect();
        if withscores {
            let mut result = Vec::new();
            for (s, m) in rev {
                result.push(m.clone());
                result.push(s.to_string());
            }
            result
        } else {
            rev.iter().map(|(_, m)| m.clone()).collect()
        }
    }

    pub fn zrevrangebyscore(&self, max: f64, min: f64, withscores: bool) -> Vec<String> {
        let items: Vec<_> = self
            .entries
            .iter()
            .rev()
            .filter(|(s, _)| *s >= min && *s <= max)
            .collect();
        if withscores {
            let mut result = Vec::new();
            for (s, m) in items {
                result.push(m.clone());
                result.push(s.to_string());
            }
            result
        } else {
            items.iter().map(|(_, m)| m.clone()).collect()
        }
    }

    pub fn zrank(&self, member: &str) -> Option<i64> {
        self.index.get(member).map(|&i| i as i64)
    }

    pub fn zrevrank(&self, member: &str) -> Option<i64> {
        self.index
            .get(member)
            .map(|&i| (self.entries.len() - 1 - i) as i64)
    }

    pub fn zscore(&self, member: &str) -> Option<f64> {
        self.index.get(member).map(|&i| self.entries[i].0)
    }

    pub fn zrem(&mut self, members: &[&str]) -> i64 {
        let mut count = 0;
        for member in members {
            let member_str = member.to_string();
            if self.index.remove(&member_str).is_some() {
                self.entries.retain(|(_, m)| m != &member_str);
                count += 1;
            }
        }
        self.reindex();
        count
    }

    pub fn zcard(&self) -> i64 {
        self.entries.len() as i64
    }

    pub fn zcount(&self, min: f64, max: f64) -> i64 {
        self.entries
            .iter()
            .filter(|(s, _)| *s >= min && *s <= max)
            .count() as i64
    }

    pub fn zincrby(&mut self, increment: f64, member: &str) -> f64 {
        let old_score = self
            .index
            .get(member)
            .map(|&i| self.entries[i].0)
            .unwrap_or(0.0);
        let new_score = old_score + increment;
        self.zadd(new_score, member);
        new_score
    }
}
