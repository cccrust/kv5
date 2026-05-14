use crate::resp::RespValue;
use crate::store::Store;
use std::sync::Arc;

fn extract_args(value: RespValue) -> Option<Vec<String>> {
    match value {
        RespValue::Array(Some(items)) => items
            .into_iter()
            .map(|v| match v {
                RespValue::BulkString(Some(s)) => Some(s),
                RespValue::SimpleString(s) => Some(s),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

pub fn handle_command(store: Arc<Store>, input: RespValue) -> RespValue {
    let args = match extract_args(input) {
        Some(a) if !a.is_empty() => a,
        _ => return RespValue::error("invalid command format"),
    };

    let cmd = args[0].to_uppercase();

    match cmd.as_str() {
        // Connection
        "PING" => {
            let msg = args.get(1).cloned().unwrap_or_else(|| "PONG".to_string());
            RespValue::SimpleString(msg)
        }
        "ECHO" => {
            let msg = args.get(1).cloned().unwrap_or_default();
            RespValue::BulkString(Some(msg))
        }
        "QUIT" => RespValue::SimpleString("OK".to_string()),

        // String
        "SET" => {
            if args.len() < 3 {
                return wrong_arity("SET");
            }
            let key = args[1].clone();
            let value = args[2].clone();

            let mut ttl_secs: Option<u64> = None;
            let mut ttl_ms: Option<u64> = None;
            let mut nx = false;
            let mut xx = false;
            let mut i = 3;
            while i < args.len() {
                match args[i].to_uppercase().as_str() {
                    "EX" if i + 1 < args.len() => {
                        ttl_secs = args[i + 1].parse().ok();
                        i += 2;
                    }
                    "PX" if i + 1 < args.len() => {
                        ttl_ms = args[i + 1].parse().ok();
                        i += 2;
                    }
                    "NX" => {
                        nx = true;
                        i += 1;
                    }
                    "XX" => {
                        xx = true;
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }

            if nx && store.exists(&key) {
                return RespValue::BulkString(None);
            }
            if xx && !store.exists(&key) {
                return RespValue::BulkString(None);
            }

            if let Some(secs) = ttl_secs {
                store.setex(key, value, secs);
            } else if let Some(ms) = ttl_ms {
                store.psetex(key, value, ms);
            } else {
                store.set(key, value);
            }
            RespValue::ok()
        }

        "GET" => {
            if args.len() < 2 {
                return wrong_arity("GET");
            }
            match store.get(&args[1]) {
                Some(v) if v.starts_with("(error)") => RespValue::wrongtype(),
                Some(v) => RespValue::BulkString(Some(v)),
                None => RespValue::BulkString(None),
            }
        }

        "GETSET" => {
            if args.len() < 3 {
                return wrong_arity("GETSET");
            }
            match store.getset(args[1].clone(), args[2].clone()) {
                Some(v) => RespValue::BulkString(Some(v)),
                None => RespValue::BulkString(None),
            }
        }

        "SETNX" => {
            if args.len() < 3 {
                return wrong_arity("SETNX");
            }
            RespValue::Integer(store.setnx(args[1].clone(), args[2].clone()))
        }

        "SETEX" => {
            if args.len() < 4 {
                return wrong_arity("SETEX");
            }
            let ttl: u64 = match args[2].parse() {
                Ok(t) => t,
                Err(_) => return RespValue::error("value is not an integer or out of range"),
            };
            store.setex(args[1].clone(), args[3].clone(), ttl);
            RespValue::ok()
        }

        "PSETEX" => {
            if args.len() < 4 {
                return wrong_arity("PSETEX");
            }
            let ttl: u64 = match args[2].parse() {
                Ok(t) => t,
                Err(_) => return RespValue::error("value is not an integer or out of range"),
            };
            store.psetex(args[1].clone(), args[3].clone(), ttl);
            RespValue::ok()
        }

        "MSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return RespValue::error("wrong number of arguments for MSET");
            }
            let pairs: Vec<(String, String)> = args[1..]
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            store.mset(pairs);
            RespValue::ok()
        }

        "MGET" => {
            if args.len() < 2 {
                return wrong_arity("MGET");
            }
            let keys: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();
            let values = store.mget(keys);
            RespValue::Array(Some(
                values.into_iter().map(RespValue::BulkString).collect(),
            ))
        }

        "APPEND" => {
            if args.len() < 3 {
                return wrong_arity("APPEND");
            }
            RespValue::Integer(store.append(&args[1], &args[2]))
        }

        "STRLEN" => {
            if args.len() < 2 {
                return wrong_arity("STRLEN");
            }
            RespValue::Integer(store.strlen(&args[1]))
        }

        // Integer
        "INCR" => {
            if args.len() < 2 {
                return wrong_arity("INCR");
            }
            match store.incr(&args[1]) {
                Ok(v) => RespValue::Integer(v),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }

        "DECR" => {
            if args.len() < 2 {
                return wrong_arity("DECR");
            }
            match store.decr(&args[1]) {
                Ok(v) => RespValue::Integer(v),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }

        "INCRBY" => {
            if args.len() < 3 {
                return wrong_arity("INCRBY");
            }
            let delta: i64 = match args[2].parse() {
                Ok(d) => d,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            match store.incrby(&args[1], delta) {
                Ok(v) => RespValue::Integer(v),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }

        "DECRBY" => {
            if args.len() < 3 {
                return wrong_arity("DECRBY");
            }
            let delta: i64 = match args[2].parse() {
                Ok(d) => d,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            match store.incrby(&args[1], -delta) {
                Ok(v) => RespValue::Integer(v),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }

        // Key
        "EXISTS" => {
            if args.len() < 2 {
                return wrong_arity("EXISTS");
            }
            RespValue::Integer(if store.exists(&args[1]) { 1 } else { 0 })
        }

        "DEL" => {
            if args.len() < 2 {
                return wrong_arity("DEL");
            }
            let keys: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();
            RespValue::Integer(store.del(keys))
        }

        "KEYS" => {
            let pattern = args.get(1).map(|s| s.as_str()).unwrap_or("*");
            let mut keys = store.keys(pattern);
            keys.sort();
            RespValue::Array(Some(
                keys.into_iter()
                    .map(|k| RespValue::BulkString(Some(k)))
                    .collect(),
            ))
        }

        "TYPE" => {
            if args.len() < 2 {
                return wrong_arity("TYPE");
            }
            RespValue::SimpleString(store.type_of(&args[1]).to_string())
        }

        "EXPIRE" => {
            if args.len() < 3 {
                return wrong_arity("EXPIRE");
            }
            let secs: u64 = match args[2].parse() {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            RespValue::Integer(store.expire(&args[1], secs))
        }

        "PEXPIRE" => {
            if args.len() < 3 {
                return wrong_arity("PEXPIRE");
            }
            let ms: u64 = match args[2].parse() {
                Ok(m) => m,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            RespValue::Integer(store.pexpire(&args[1], ms))
        }

        "PERSIST" => {
            if args.len() < 2 {
                return wrong_arity("PERSIST");
            }
            RespValue::Integer(store.persist(&args[1]))
        }

        "TTL" => {
            if args.len() < 2 {
                return wrong_arity("TTL");
            }
            RespValue::Integer(store.ttl(&args[1]))
        }

        "PTTL" => {
            if args.len() < 2 {
                return wrong_arity("PTTL");
            }
            RespValue::Integer(store.pttl(&args[1]))
        }

        "RENAME" => {
            if args.len() < 3 {
                return wrong_arity("RENAME");
            }
            match store.rename(&args[1], &args[2]) {
                Ok(_) => RespValue::ok(),
                Err(e) => RespValue::error(&e.to_string()),
            }
        }

        "DBSIZE" => RespValue::Integer(store.dbsize() as i64),

        "FLUSHDB" | "FLUSHALL" => {
            store.flushdb();
            RespValue::ok()
        }

        // List
        "LPUSH" => {
            if args.len() < 3 {
                return wrong_arity("LPUSH");
            }
            let values: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            match store.lpush(&args[1], values) {
                Ok(n) => RespValue::Integer(n),
                Err(_) => RespValue::wrongtype(),
            }
        }

        "RPUSH" => {
            if args.len() < 3 {
                return wrong_arity("RPUSH");
            }
            let values: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            match store.rpush(&args[1], values) {
                Ok(n) => RespValue::Integer(n),
                Err(_) => RespValue::wrongtype(),
            }
        }

        "LPOP" => {
            if args.len() < 2 {
                return wrong_arity("LPOP");
            }
            RespValue::BulkString(store.lpop(&args[1]))
        }

        "RPOP" => {
            if args.len() < 2 {
                return wrong_arity("RPOP");
            }
            RespValue::BulkString(store.rpop(&args[1]))
        }

        "LLEN" => {
            if args.len() < 2 {
                return wrong_arity("LLEN");
            }
            RespValue::Integer(store.llen(&args[1]))
        }

        "LRANGE" => {
            if args.len() < 4 {
                return wrong_arity("LRANGE");
            }
            let start: i64 = args[2].parse().unwrap_or(0);
            let stop: i64 = args[3].parse().unwrap_or(-1);
            let items = store.lrange(&args[1], start, stop);
            RespValue::Array(Some(
                items
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            ))
        }

        "LINDEX" => {
            if args.len() < 3 {
                return wrong_arity("LINDEX");
            }
            let index: i64 = args[2].parse().unwrap_or(0);
            RespValue::BulkString(store.lindex(&args[1], index))
        }

        // Hash
        "HSET" => {
            if args.len() < 4 {
                return wrong_arity("HSET");
            }
            match store.hset(&args[1], &args[2], &args[3]) {
                Ok(n) => RespValue::Integer(n),
                Err(_) => RespValue::wrongtype(),
            }
        }

        "HGET" => {
            if args.len() < 3 {
                return wrong_arity("HGET");
            }
            RespValue::BulkString(store.hget(&args[1], &args[2]))
        }

        "HMSET" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return RespValue::error("wrong number of arguments for HMSET");
            }
            let pairs: Vec<(&str, &str)> = args[2..]
                .chunks(2)
                .map(|c| (c[0].as_str(), c[1].as_str()))
                .collect();
            match store.hmset(&args[1], pairs) {
                Ok(_) => RespValue::ok(),
                Err(_) => RespValue::wrongtype(),
            }
        }

        "HMGET" => {
            if args.len() < 3 {
                return wrong_arity("HMGET");
            }
            let fields: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            let values = store.hmget(&args[1], fields);
            RespValue::Array(Some(
                values.into_iter().map(RespValue::BulkString).collect(),
            ))
        }

        "HDEL" => {
            if args.len() < 3 {
                return wrong_arity("HDEL");
            }
            let fields: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            RespValue::Integer(store.hdel(&args[1], fields))
        }

        "HGETALL" => {
            if args.len() < 2 {
                return wrong_arity("HGETALL");
            }
            let pairs = store.hgetall(&args[1]);
            let mut items = Vec::new();
            for (k, v) in pairs {
                items.push(RespValue::BulkString(Some(k)));
                items.push(RespValue::BulkString(Some(v)));
            }
            RespValue::Array(Some(items))
        }

        "HKEYS" => {
            if args.len() < 2 {
                return wrong_arity("HKEYS");
            }
            RespValue::Array(Some(
                store
                    .hkeys(&args[1])
                    .into_iter()
                    .map(|k| RespValue::BulkString(Some(k)))
                    .collect(),
            ))
        }

        "HVALS" => {
            if args.len() < 2 {
                return wrong_arity("HVALS");
            }
            RespValue::Array(Some(
                store
                    .hvals(&args[1])
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            ))
        }

        "HLEN" => {
            if args.len() < 2 {
                return wrong_arity("HLEN");
            }
            RespValue::Integer(store.hlen(&args[1]))
        }

        "HEXISTS" => {
            if args.len() < 3 {
                return wrong_arity("HEXISTS");
            }
            RespValue::Integer(if store.hexists(&args[1], &args[2]) {
                1
            } else {
                0
            })
        }

        // Set
        "SADD" => {
            if args.len() < 3 {
                return wrong_arity("SADD");
            }
            let members: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            match store.sadd(&args[1], members) {
                Ok(n) => RespValue::Integer(n),
                Err(_) => RespValue::wrongtype(),
            }
        }

        "SREM" => {
            if args.len() < 3 {
                return wrong_arity("SREM");
            }
            let members: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            RespValue::Integer(store.srem(&args[1], members))
        }

        "SMEMBERS" => {
            if args.len() < 2 {
                return wrong_arity("SMEMBERS");
            }
            let mut members = store.smembers(&args[1]);
            members.sort();
            RespValue::Array(Some(
                members
                    .into_iter()
                    .map(|m| RespValue::BulkString(Some(m)))
                    .collect(),
            ))
        }

        "SISMEMBER" => {
            if args.len() < 3 {
                return wrong_arity("SISMEMBER");
            }
            RespValue::Integer(if store.sismember(&args[1], &args[2]) {
                1
            } else {
                0
            })
        }

        "SCARD" => {
            if args.len() < 2 {
                return wrong_arity("SCARD");
            }
            RespValue::Integer(store.scard(&args[1]))
        }

        // Sorted Set
        "ZADD" => {
            if args.len() < 4 {
                return wrong_arity("ZADD");
            }
            let key = &args[1];
            let score: f64 = match args[2].parse() {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let member = &args[3];
            RespValue::Integer(store.zadd(key, score, member))
        }

        "ZRANGE" => {
            if args.len() < 4 {
                return wrong_arity("ZRANGE");
            }
            let key = &args[1];
            let start: i64 = match args[2].parse() {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            let stop: i64 = match args[3].parse() {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            let withscores =
                args.get(4).map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());
            let items = store.zrange(key, start, stop, withscores);
            RespValue::Array(Some(
                items
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            ))
        }

        "ZRANGEBYSCORE" => {
            if args.len() < 4 {
                return wrong_arity("ZRANGEBYSCORE");
            }
            let key = &args[1];
            let min: f64 = match parse_score(&args[2]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let max: f64 = match parse_score(&args[3]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let withscores =
                args.get(4).map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());
            let items = store.zrangebyscore(key, min, max, withscores);
            RespValue::Array(Some(
                items
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            ))
        }

        "ZREVRANGE" => {
            if args.len() < 4 {
                return wrong_arity("ZREVRANGE");
            }
            let key = &args[1];
            let start: i64 = match args[2].parse() {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            let stop: i64 = match args[3].parse() {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not an integer"),
            };
            let withscores =
                args.get(4).map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());
            let items = store.zrevrange(key, start, stop, withscores);
            RespValue::Array(Some(
                items
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            ))
        }

        "ZREVRANGEBYSCORE" => {
            if args.len() < 4 {
                return wrong_arity("ZREVRANGEBYSCORE");
            }
            let key = &args[1];
            let max: f64 = match parse_score(&args[2]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let min: f64 = match parse_score(&args[3]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let withscores =
                args.get(4).map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());
            let items = store.zrevrangebyscore(key, max, min, withscores);
            RespValue::Array(Some(
                items
                    .into_iter()
                    .map(|v| RespValue::BulkString(Some(v)))
                    .collect(),
            ))
        }

        "ZRANK" => {
            if args.len() < 3 {
                return wrong_arity("ZRANK");
            }
            match store.zrank(&args[1], &args[2]) {
                Some(r) => RespValue::Integer(r),
                None => RespValue::BulkString(None),
            }
        }

        "ZREVRANK" => {
            if args.len() < 3 {
                return wrong_arity("ZREVRANK");
            }
            match store.zrevrank(&args[1], &args[2]) {
                Some(r) => RespValue::Integer(r),
                None => RespValue::BulkString(None),
            }
        }

        "ZSCORE" => {
            if args.len() < 3 {
                return wrong_arity("ZSCORE");
            }
            match store.zscore(&args[1], &args[2]) {
                Some(s) => RespValue::BulkString(Some(s.to_string())),
                None => RespValue::BulkString(None),
            }
        }

        "ZREM" => {
            if args.len() < 3 {
                return wrong_arity("ZREM");
            }
            let key = &args[1];
            let members: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
            RespValue::Integer(store.zrem(key, members))
        }

        "ZCARD" => {
            if args.len() < 2 {
                return wrong_arity("ZCARD");
            }
            RespValue::Integer(store.zcard(&args[1]))
        }

        "ZCOUNT" => {
            if args.len() < 4 {
                return wrong_arity("ZCOUNT");
            }
            let key = &args[1];
            let min: f64 = match parse_score(&args[2]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let max: f64 = match parse_score(&args[3]) {
                Ok(s) => s,
                Err(_) => return RespValue::error("value is not a float"),
            };
            RespValue::Integer(store.zcount(key, min, max))
        }

        "ZINCRBY" => {
            if args.len() < 4 {
                return wrong_arity("ZINCRBY");
            }
            let key = &args[1];
            let increment: f64 = match args[2].parse() {
                Ok(i) => i,
                Err(_) => return RespValue::error("value is not a float"),
            };
            let member = &args[3];
            let result = store.zincrby(key, increment, member);
            RespValue::BulkString(Some(result.to_string()))
        }

        // Server
        "INFO" => {
            let info = format!(
                "# Server\r\nkv5_version:0.1.0\r\nmode:standalone\r\nos:Linux\r\n\
                 # Stats\r\nconnected_clients:1\r\nused_memory:unknown\r\n\
                 # Keyspace\r\ndb0:keys={},expires=0\r\n",
                store.dbsize()
            );
            RespValue::BulkString(Some(info))
        }

        "COMMAND" => RespValue::SimpleString("OK".to_string()),

        "SELECT" => RespValue::ok(),

        "SAVE" | "BGSAVE" => match store.save_to_disk() {
            Ok(_) => RespValue::ok(),
            Err(e) => RespValue::error(&e.to_string()),
        },

        // Pub/Sub
        "PUBLISH" => {
            if args.len() < 3 {
                return wrong_arity("PUBLISH");
            }
            let channel = &args[1];
            let message = &args[2];
            let count = store.pubsub.publish(channel, message);
            RespValue::Integer(count as i64)
        }

        "SUBSCRIBE" => {
            if args.len() < 2 {
                return wrong_arity("SUBSCRIBE");
            }
            let channels: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();
            let count = store.pubsub.channels_count();
            RespValue::Array(Some(
                channels
                    .iter()
                    .map(|ch| {
                        RespValue::Array(Some(vec![
                            RespValue::SimpleString("subscribe".to_string()),
                            RespValue::BulkString(Some(ch.to_string())),
                            RespValue::Integer(count as i64),
                        ]))
                    })
                    .collect(),
            ))
        }

        "UNSUBSCRIBE" => {
            let channels: Vec<&str> = if args.len() > 1 {
                args[1..].iter().map(|s| s.as_str()).collect()
            } else {
                vec![]
            };
            RespValue::Array(Some(
                channels
                    .iter()
                    .map(|ch| {
                        RespValue::Array(Some(vec![
                            RespValue::SimpleString("unsubscribe".to_string()),
                            RespValue::BulkString(Some(ch.to_string())),
                            RespValue::Integer(0),
                        ]))
                    })
                    .collect(),
            ))
        }

        "PSUBSCRIBE" => {
            if args.len() < 2 {
                return wrong_arity("PSUBSCRIBE");
            }
            let patterns: Vec<&str> = args[1..].iter().map(|s| s.as_str()).collect();
            let count = store.pubsub.patterns_count();
            RespValue::Array(Some(
                patterns
                    .iter()
                    .map(|pat| {
                        RespValue::Array(Some(vec![
                            RespValue::SimpleString("psubscribe".to_string()),
                            RespValue::BulkString(Some(pat.to_string())),
                            RespValue::Integer(count as i64),
                        ]))
                    })
                    .collect(),
            ))
        }

        "PUNSUBSCRIBE" => {
            let patterns: Vec<&str> = if args.len() > 1 {
                args[1..].iter().map(|s| s.as_str()).collect()
            } else {
                vec![]
            };
            RespValue::Array(Some(
                patterns
                    .iter()
                    .map(|pat| {
                        RespValue::Array(Some(vec![
                            RespValue::SimpleString("punsubscribe".to_string()),
                            RespValue::BulkString(Some(pat.to_string())),
                            RespValue::Integer(0),
                        ]))
                    })
                    .collect(),
            ))
        }

        "PUBSUB" => {
            if args.len() < 2 {
                return wrong_arity("PUBSUB");
            }
            match args[1].to_uppercase().as_str() {
                "CHANNELS" => {
                    let pattern = args.get(2).map(|s| s.as_str());
                    let channels = store.pubsub.channels(pattern);
                    RespValue::Array(Some(
                        channels
                            .into_iter()
                            .map(|c| RespValue::BulkString(Some(c)))
                            .collect(),
                    ))
                }
                "NUMSUB" => {
                    let channels: Vec<&str> = args[2..].iter().map(|s| s.as_str()).collect();
                    let counts = store.pubsub.numsub(&channels);
                    let mut result = Vec::new();
                    for (ch, count) in counts {
                        result.push(RespValue::BulkString(Some(ch)));
                        result.push(RespValue::Integer(count as i64));
                    }
                    RespValue::Array(Some(result))
                }
                "NUMPAT" => {
                    let count = store.pubsub.numpat();
                    RespValue::Integer(count as i64)
                }
                _ => RespValue::error("ERR Unknown PUBSUB subcommand"),
            }
        }

        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd)),
    }
}

fn wrong_arity(cmd: &str) -> RespValue {
    RespValue::error(&format!("wrong number of arguments for '{}' command", cmd))
}

fn parse_score(s: &str) -> Result<f64, ()> {
    match s.to_uppercase().as_str() {
        "-INF" => Ok(f64::NEG_INFINITY),
        "+INF" | "INF" => Ok(f64::INFINITY),
        _ => s.parse().map_err(|_| ()),
    }
}
