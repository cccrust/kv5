use std::io::Read;
/// kv5-cli: Interactive command-line client for kv5
/// Usage: kv5-cli [host] [port]
use std::io::{self, BufRead, Write};
use std::net::TcpStream;

fn main() {
    let host = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let port = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "6380".to_string());
    let addr = format!("{}:{}", host, port);

    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Could not connect to kv5 at {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    println!("Connected to kv5 at {}", addr);
    println!("Type commands and press Enter. Ctrl+C to quit.\n");

    let stdin = io::stdin();
    loop {
        print!("kv5> ");
        io::stdout().flush().unwrap();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) => {
                eprintln!("Read error: {}", e);
                break;
            }
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.eq_ignore_ascii_case("exit") || line.eq_ignore_ascii_case("quit") {
            println!("Bye!");
            break;
        }

        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        let mut cmd = format!("*{}\r\n", parts.len());
        for part in &parts {
            cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
        }

        if let Err(e) = stream.write_all(cmd.as_bytes()) {
            eprintln!("Write error: {}", e);
            break;
        }

        match read_response(&mut stream) {
            Ok(resp) => println!("{}", resp),
            Err(e) => {
                eprintln!("Error reading response: {}", e);
                break;
            }
        }
    }
}

fn read_response(stream: &mut TcpStream) -> Result<String, String> {
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).map_err(|e| e.to_string())?;
    let data = &buf[..n];
    format_resp(data, &mut 0)
}

fn format_resp(data: &[u8], pos: &mut usize) -> Result<String, String> {
    if *pos >= data.len() {
        return Err("empty response".to_string());
    }

    let prefix = data[*pos] as char;
    *pos += 1;

    match prefix {
        '+' => {
            let line = read_line(data, pos)?;
            Ok(line)
        }
        '-' => {
            let line = read_line(data, pos)?;
            Ok(format!("(error) {}", line))
        }
        ':' => {
            let line = read_line(data, pos)?;
            Ok(format!("(integer) {}", line))
        }
        '$' => {
            let line = read_line(data, pos)?;
            let len: i64 = line.parse().map_err(|_| "invalid bulk length")?;
            if len == -1 {
                Ok("(nil)".to_string())
            } else {
                let len = len as usize;
                if *pos + len > data.len() {
                    return Err("truncated bulk string".to_string());
                }
                let s = std::str::from_utf8(&data[*pos..*pos + len]).map_err(|_| "invalid utf8")?;
                *pos += len + 2;
                Ok(format!(
                    "\"{}\"",
                    s.replace('\n', "\\n").replace('\r', "\\r")
                ))
            }
        }
        '*' => {
            let line = read_line(data, pos)?;
            let count: i64 = line.parse().map_err(|_| "invalid array length")?;
            if count == -1 {
                return Ok("(nil)".to_string());
            }
            let mut out = Vec::new();
            for i in 0..count as usize {
                let item = format_resp(data, pos)?;
                out.push(format!("{}) {}", i + 1, item));
            }
            if out.is_empty() {
                Ok("(empty array)".to_string())
            } else {
                Ok(out.join("\n"))
            }
        }
        _ => Ok(String::from_utf8_lossy(data).to_string()),
    }
}

fn read_line(data: &[u8], pos: &mut usize) -> Result<String, String> {
    let start = *pos;
    while *pos + 1 < data.len() {
        if data[*pos] == b'\r' && data[*pos + 1] == b'\n' {
            let line = std::str::from_utf8(&data[start..*pos])
                .map_err(|_| "invalid utf8")?
                .to_string();
            *pos += 2;
            return Ok(line);
        }
        *pos += 1;
    }
    Err("incomplete line".to_string())
}
