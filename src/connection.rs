use crate::resp::{RespParser, RespValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream: BufWriter::new(stream),
            buffer: Vec::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> anyhow::Result<Option<RespValue>> {
        loop {
            if let Some(value) = self.parse_frame()? {
                return Ok(Some(value));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow::anyhow!("connection reset by peer"));
                }
            }
        }
    }

    fn parse_frame(&mut self) -> anyhow::Result<Option<RespValue>> {
        let mut parser = RespParser::new(self.buffer.clone());
        match parser.parse() {
            Ok(Some(value)) => {
                let consumed = parser.consumed();
                self.buffer.drain(..consumed);
                Ok(Some(value))
            }
            Ok(None) => Ok(None),
            Err(_) => {
                self.buffer.clear();
                Err(anyhow::anyhow!("incomplete frame"))
            }
        }
    }

    pub async fn write_frame(&mut self, response: &RespValue) -> anyhow::Result<()> {
        let data = response.serialize();
        self.stream.write_all(&data).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> anyhow::Result<()> {
        self.stream.flush().await?;
        Ok(())
    }
}
