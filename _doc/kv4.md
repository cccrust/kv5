# kv4 文件

## 版本資訊

- **版本**: 0.1.0
- **地位**: kv5 進階版的基礎版本（v0.1）
- **語言**: Rust
- **授權**: MIT

## 專案概述

kv4 是一個用 Rust 編寫的高效能記憶體鍵值資料庫，相容 Redis 的 RESP（REdis Serialization Protocol）協議。可直接使用 `redis-cli` 連接操作。

## 核心特性

1. **Redis 協議相容** - 支援 RESP 協議，可使用 redis-cli 操作
2. **多種資料型態** - String、Integer、List、Hash、Set
3. **TTL 過期机制** - EXPIRE、PEXPIRE、SETEX、PSETEX，支援毫秒精度
4. **持久化儲存** - JSON 快照格式（啟動時載入，關閉時儲存，每 60 秒自動儲存）
5. **並行處理** - 基於 Tokio 非同步執行，DashMap 無鎖並行存取
6. **Glob 模式** - KEYS 命令支援 * 和 ? 萬用字元

## 專案架構

```
kv4/
├── Cargo.toml          # 專案配置與依賴
├── kv4-README.md       # 原始 README
└── src/
    ├── main.rs         # TCP 伺服器、連線處理、優雅關閉
    ├── store.rs        # 核心 KV 儲存（DashMap + TTL）
    ├── resp.rs         # RESP 協議解析器與序列化器
    ├── cmd.rs          # 命令處理與分派
    ├── cli.rs          # 互動式命令列客戶端
    └── lib.rs          # 函式庫導出
```

## 依賴套件

| 套件 | 版本 | 用途 |
|------|------|------|
| tokio | 1 | 非同步執行環境 |
| serde | 1 | 序列化/反序列化 |
| serde_json | 1 | JSON 處理 |
| dashmap | 5 | 並髑型 HashMap |
| chrono | 0.4 | 時間處理 |
| anyhow | 1 | 錯誤處理 |
| tracing | 0.1 | 日誌記錄 |
| tracing-subscriber | 0.3 | 日誌訂閱 |
| bytes | 1 | 位元組處理 |

## 資料模型

### Value 類型（store.rs:9-16）

```rust
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
    Set(HashSet<String>),
    Integer(i64),
}
```

### 儲存結構（store.rs:31-35）

```rust
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}
```

每個鍵值對都包裝在 Entry 中，包含可選的過期時間。

### 持久化格式（store.rs:67-69）

```rust
struct Snapshot {
    entries: Vec<(String, Value, Option<u64>)>,
}
```

儲存時保留剩餘 TTL 毫秒數。

## 支援的命令

### 連線命令

| 命令 | 說明 | 範例 |
|------|------|------|
| PING [msg] | 心跳測試 | `PING` → `PONG` |
| ECHO msg | 回傳訊息 | `ECHO hello` → `hello` |
| QUIT | 關閉連線 | `QUIT` → `OK` |

### 字串命令（String）

| 命令 | 說明 | 範例 |
|------|------|------|
| SET key value [EX secs] [PX ms] [NX] [XX] | 設定值 | `SET foo bar EX 60` |
| GET key | 取得值 | `GET foo` → `bar` |
| GETSET key value | 設定並回傳舊值 | `GETSET foo new` |
| SETNX key value | 僅在不存在時設定 | `SETNX foo bar` |
| SETEX key secs value | 設定值並指定過期秒數 | `SETEX foo 60 bar` |
| PSETEX key ms value | 設定值並指定過期毫秒 | `PSETEX foo 60000 bar` |
| MSET k1 v1 k2 v2 ... | 批次設定 | `MSET a 1 b 2` |
| MGET k1 k2 ... | 批次取得 | `MGET a b c` |
| APPEND key value | 附加字串 | `APPEND foo bar` |
| STRLEN key | 字串長度 | `STRLEN foo` |
| INCR key | 加 1 | `INCR n` |
| DECR key | 減 1 | `DECR n` |
| INCRBY key n | 加 n | `INCRBY n 5` |
| DECRBY key n | 減 n | `DECRBY n 3` |

### 鍵命令（Key）

| 命令 | 說明 | 範例 |
|------|------|------|
| EXISTS key | 是否存在 | `EXISTS foo` → `1` |
| DEL key [key ...] | 刪除 | `DEL foo bar` |
| KEYS pattern | 列出符合的鍵 | `KEYS user:*` |
| TYPE key | 型態名稱 | `TYPE foo` → `string` |
| EXPIRE key secs | 設定過期秒數 | `EXPIRE foo 60` |
| PEXPIRE key ms | 設定過期毫秒 | `PEXPIRE foo 60000` |
| PERSIST key | 移除過期設定 | `PERSIST foo` |
| TTL key | 剩餘秒數 | `TTL foo` |
| PTTL key | 剩餘毫秒 | `PTTL foo` |
| RENAME key newkey | 重新命名 | `RENAME foo bar` |
| DBSIZE | 鍵的數量 | `DBSIZE` |
| FLUSHDB | 清空資料庫 | `FLUSHDB` |

### 列表命令（List）

| 命令 | 說明 | 範例 |
|------|------|------|
| LPUSH key val [val ...] | 從左端新增 | `LPUSH list a b c` |
| RPUSH key val [val ...] | 從右端新增 | `RPUSH list a b c` |
| LPOP key | 從左端移除並回傳 | `LPOP list` |
| RPOP key | 從右端移除並回傳 | `RPOP list` |
| LLEN key | 長度 | `LLEN list` |
| LRANGE key start stop | 範圍取得 | `LRANGE list 0 -1` |
| LINDEX key index | 取得指定索引 | `LINDEX list 0` |

### 雜湊命令（Hash）

| 命令 | 說明 | 範例 |
|------|------|------|
| HSET key field value | 設定欄位 | `HSET user name john` |
| HGET key field | 取得欄位 | `HGET user name` → `john` |
| HMSET key f1 v1 f2 v2 ... | 批次設定 | `HMSET user name john age 30` |
| HMGET key f1 f2 ... | 批次取得 | `HMGET user name age` |
| HDEL key field [field ...] | 刪除欄位 | `HDEL user age` |
| HGETALL key | 取得所有欄位和值 | `HGETALL user` |
| HKEYS key | 所有欄位名稱 | `HKEYS user` |
| HVALS key | 所有值 | `HVALS user` |
| HLEN key | 欄位數量 | `HLEN user` |
| HEXISTS key field | 欄位是否存在 | `HEXISTS user name` |

### 集合命令（Set）

| 命令 | 說明 | 範例 |
|------|------|------|
| SADD key member [member ...] | 新增成員 | `SADD tags rust redis` |
| SREM key member [member ...] | 移除成員 | `SREM tags redis` |
| SMEMBERS key | 所有成員 | `SMEMBERS tags` |
| SISMEMBER key member | 是否為成員 | `SISMEMBER tags rust` |
| SCARD key | 成員數量 | `SCARD tags` |

### 伺服器命令

| 命令 | 說明 | 範例 |
|------|------|------|
| INFO | 伺服器資訊 | `INFO` |
| SAVE / BGSAVE | 立即儲存快照 | `SAVE` |
| SELECT db | 選擇資料庫 | `SELECT 0` |

## 核心實作

### RESP 協議解析（resp.rs）

支援以下 RESP 類型：
- `+` Simple String
- `-` Error
- `:` Integer
- `$` Bulk String
- `*` Array

同時支援 inline 指令（來自 telnet）。

### 儲存引擎（store.rs）

- 使用 `DashMap` 提供並髑讀寫
- 每秒背景任務清理過期鍵
- 讀取時惰性清除過期鍵
- 持久化採用 JSON 格式

### 命令分派（cmd.rs）

將 RESP 陣列解析為命令參數，依命令字串分派到對應的 Store 方法。

## 快速開始

### 啟動伺服器

```bash
# 預設監聽 127.0.0.1:6380，持久化到 kv4.dump.json
cargo run --bin kv4-server

# 自訂設定
KV4_ADDR=0.0.0.0:6380 KV4_PERSIST=/data/kv4.json cargo run --bin kv4-server

# 停用持久化
KV4_PERSIST="" cargo run --bin kv4-server
```

### 使用 kv4-cli

```bash
cargo run --bin kv4-cli              # 連接 127.0.0.1:6380
cargo run --bin kv4-cli 192.168.1.1  # 指定主機
cargo run --bin kv4-cli 127.0.0.1 6381  # 指定主機和埠
```

### 使用 redis-cli

```bash
redis-cli -p 6380
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
```

### 編譯發布版本

```bash
cargo build --release

# 二進位檔位於
./target/release/kv4-server
./target/release/kv4-cli
```

## 環境變數

| 變數 | 預設值 | 說明 |
|------|--------|------|
| KV4_ADDR | 127.0.0.1:6380 | 監聽地址 |
| KV4_PERSIST | kv4.dump.json | 快照檔案路徑（空字串停用） |
| RUST_LOG | kv4=info | 日誌等級 |

## 單元測試

專案包含完整的單元測試，位於 store.rs 的測試模組：

```bash
cargo test
```

測試項目涵蓋：
- 基本 SET/GET 操作
- SETNX 原子性設定
- MSET/MGET 批次操作
- INCR/DECR 遞增/遞減
- TTL/EXPIRE 過期机制
- List 操作（LPUSH/RPUSH/LPOP/RPOP/LRANGE）
- Hash 操作（HSET/HGET/HDEL/HGETALL）
- Set 操作（SADD/SREM/SMEMBERS/SISMEMBER）
- KEYS glob 模式匹配
- RENAME/TYPE 操作

## 已知限制

1. 僅支援單一資料庫（SELECT 命令目前無效）
2. 持久化採用 JSON 格式，大型資料集效能有限
3. 沒有發布/訂閱機制
4. 沒有交易（MULTI/EXEC）支援
5. 沒有 Lua 腳本支援

## kv5 發展方向

kv5 將基於 kv4 v0.1.0 进行以下擴展：

1. **效能優化** - 支援更多並發連線
2. **叢集支援** - Redis Cluster 相容模式
3. **進階資料結構** - Sorted Set、Stream
4. **效能監控** - 即時效能指標
5. **指令擴展** - 更多 Redis 相容命令

## 參考資源

- Redis Protocol: https://redis.io/topics/protocol
- RESP 規格: https://redis.io/docs/reference/protocol-spec/