use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const INDEX_HTML: &str = include_str!("../public/index.html");

fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "10000".to_string());
    let state = Arc::new(stock_correction_survey::SurveyState::load());
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).unwrap_or_else(|err| {
        eprintln!("failed to bind {addr}: {err}");
        std::process::exit(1);
    });
    eprintln!("stock audit listening on {addr}");

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let state = Arc::clone(&state);
                thread::spawn(move || handle(stream, &state));
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }
}

fn handle(mut stream: TcpStream, state: &stock_correction_survey::SurveyState) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(60)));
    let (method, path, body) = match stock_correction_survey::read_incoming(&mut stream) {
        Some(req) => req,
        None => return,
    };
    let bare = path.split('?').next().unwrap_or("/");
    let result = if (method == "GET" || method == "HEAD") && (bare == "/" || bare == "/index.html") {
        (
            "200 OK".to_string(),
            "text/html; charset=utf-8".to_string(),
            String::new(),
            INDEX_HTML.as_bytes().to_vec(),
        )
    } else {
        state.respond(&method, bare, &body)
    };
    stock_correction_survey::send(&mut stream, &method, result);
}
