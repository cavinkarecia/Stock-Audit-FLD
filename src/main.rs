use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

const INDEX_HTML: &str = include_str!("../public/index.html");

fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "10000".to_string());
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).unwrap_or_else(|err| {
        eprintln!("failed to bind {addr}: {err}");
        std::process::exit(1);
    });
    eprintln!("stock audit listening on {addr}");

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                thread::spawn(|| handle(stream));
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }
}

fn handle(mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(8)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(8)));

    let request = match read_head(&mut stream) {
        Some(request) => request,
        None => return,
    };
    let mut parts = request.split_whitespace();
    let method = parts.next().unwrap_or("");
    let raw_path = parts.next().unwrap_or("/");
    let path = raw_path.split('?').next().unwrap_or("/");

    let (status, content_type, body): (&str, &str, &[u8]) = match (method, path) {
        ("GET" | "HEAD", "/" | "/index.html") => {
            ("200 OK", "text/html; charset=utf-8", INDEX_HTML.as_bytes())
        }
        ("GET" | "HEAD", "/healthz") => ("200 OK", "text/plain; charset=utf-8", b"ok"),
        ("GET" | "HEAD", _) => ("404 Not Found", "text/plain; charset=utf-8", b"Not found"),
        _ => ("405 Method Not Allowed", "text/plain; charset=utf-8", b"Method not allowed"),
    };

    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-cache\r\n\r\n",
        body.len()
    );
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    if method != "HEAD" && stream.write_all(body).is_err() {
        return;
    }
    let _ = stream.flush();
}

fn read_head(stream: &mut TcpStream) -> Option<String> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).ok()?;
    if n == 0 {
        return None;
    }
    let text = String::from_utf8_lossy(&buf[..n]);
    text.lines().next().map(|line| line.trim_end().to_string())
}
