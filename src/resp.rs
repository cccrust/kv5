/// RESP (REdis Serialization Protocol) implementation for kv5
///
/// Protocol format:
///   Simple string:  +OK\r\n
///   Error:          -ERR message\r\n
///   Integer:        :42\r\n
///   Bulk string:    $6\r\nfoobar\r\n
///   Null bulk:      $-1\r\n
///   Array:          *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
#[allow(dead_code, unused)]
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(e) => format!("-{}\r\n", e).into_bytes(),
            RespValue::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s).into_bytes(),
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(items)) => {
                let mut out = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    out.extend(item.serialize());
                }
                out
            }
        }
    }

    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn null() -> Self {
        RespValue::BulkString(None)
    }

    pub fn error(msg: &str) -> Self {
        RespValue::Error(format!("ERR {}", msg))
    }

    pub fn wrongtype() -> Self {
        RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        )
    }
}

pub struct RespParser {
    buf: Vec<u8>,
    pos: usize,
}

impl RespParser {
    pub fn new(data: Vec<u8>) -> Self {
        RespParser { buf: data, pos: 0 }
    }

    pub fn parse(&mut self) -> anyhow::Result<Option<RespValue>> {
        if self.pos >= self.buf.len() {
            return Ok(None);
        }

        let byte = self.buf[self.pos];
        self.pos += 1;

        match byte {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk_string(),
            b'*' => self.parse_array(),
            _ => {
                self.pos -= 1;
                self.parse_inline()
            }
        }
    }

    fn read_line(&mut self) -> anyhow::Result<String> {
        let start = self.pos;
        while self.pos + 1 < self.buf.len() {
            if self.buf[self.pos] == b'\r' && self.buf[self.pos + 1] == b'\n' {
                let line = std::str::from_utf8(&self.buf[start..self.pos])?.to_string();
                self.pos += 2;
                return Ok(line);
            }
            self.pos += 1;
        }
        self.pos = start;
        Err(anyhow::anyhow!("incomplete"))
    }

    fn parse_simple_string(&mut self) -> anyhow::Result<Option<RespValue>> {
        Ok(Some(RespValue::SimpleString(self.read_line()?)))
    }

    fn parse_error(&mut self) -> anyhow::Result<Option<RespValue>> {
        Ok(Some(RespValue::Error(self.read_line()?)))
    }

    fn parse_integer(&mut self) -> anyhow::Result<Option<RespValue>> {
        let line = self.read_line()?;
        Ok(Some(RespValue::Integer(line.parse()?)))
    }

    fn parse_bulk_string(&mut self) -> anyhow::Result<Option<RespValue>> {
        let line = self.read_line()?;
        let len: i64 = line.parse()?;
        if len == -1 {
            return Ok(Some(RespValue::BulkString(None)));
        }
        let len = len as usize;
        if self.pos + len + 2 > self.buf.len() {
            return Err(anyhow::anyhow!("incomplete"));
        }
        let s = std::str::from_utf8(&self.buf[self.pos..self.pos + len])?.to_string();
        self.pos += len + 2;
        Ok(Some(RespValue::BulkString(Some(s))))
    }

    fn parse_array(&mut self) -> anyhow::Result<Option<RespValue>> {
        let line = self.read_line()?;
        let count: i64 = line.parse()?;
        if count == -1 {
            return Ok(Some(RespValue::Array(None)));
        }
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            match self.parse()? {
                Some(v) => items.push(v),
                None => return Err(anyhow::anyhow!("incomplete array")),
            }
        }
        Ok(Some(RespValue::Array(Some(items))))
    }

    fn parse_inline(&mut self) -> anyhow::Result<Option<RespValue>> {
        let line = self.read_line()?;
        let parts: Vec<RespValue> = line
            .split_whitespace()
            .map(|s| RespValue::BulkString(Some(s.to_string())))
            .collect();
        Ok(Some(RespValue::Array(Some(parts))))
    }

    pub fn consumed(&self) -> usize {
        self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut p = RespParser::new(b"+OK\r\n".to_vec());
        assert_eq!(
            p.parse().unwrap(),
            Some(RespValue::SimpleString("OK".into()))
        );
    }

    #[test]
    fn test_parse_array() {
        let mut p = RespParser::new(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n".to_vec());
        let val = p.parse().unwrap().unwrap();
        assert_eq!(
            val,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some("GET".into())),
                RespValue::BulkString(Some("foo".into())),
            ]))
        );
    }

    #[test]
    fn test_serialize_ok() {
        assert_eq!(RespValue::ok().serialize(), b"+OK\r\n");
    }
}
