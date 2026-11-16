use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use lettre::message::{header::ContentType, Attachment, Message, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{SmtpTransport, Transport};
use serde::{Deserialize, Serialize};

const SURVEY_HTML: &str = include_str!("../public/survey.html");
const MAX_BODY: usize = 20 * 1024 * 1024;
const MAX_FILE: usize = 8 * 1024 * 1024;

struct App {
    dir: PathBuf,
    responses: Vec<Stored>,
    recipients: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StockRow {
    code: String,
    desc: String,
    mrp: f64,
    saleable: f64,
    unsaleable: f64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredFile {
    name: String,
    content_type: String,
    path: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Stored {
    id: String,
    submitted_at: String,
    auditor: String,
    auditor_other: String,
    conducted: String,
    date: String,
    db_code: String,
    db_name: String,
    stock_file_name: String,
    stock_rows: Vec<StockRow>,
    consent: Option<StoredFile>,
    audit_type: String,
    sr_code: String,
    sr_name: String,
    sr_value: String,
    sr_proof: Option<StoredFile>,
    sec_code: String,
    sec_name: String,
    sec_value: String,
    sec_proof: Option<StoredFile>,
    cancel_reason: String,
    cancel_date: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IncomingFile {
    name: String,
    content_type: String,
    data_base64: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Incoming {
    auditor: String,
    #[serde(default)]
    auditor_other: String,
    conducted: String,
    #[serde(default)]
    date: String,
    #[serde(default)]
    db_code: String,
    #[serde(default)]
    db_name: String,
    #[serde(default)]
    stock_file_name: String,
    #[serde(default)]
    stock_rows: Vec<StockRow>,
    consent: Option<IncomingFile>,
    #[serde(default)]
    audit_type: String,
    #[serde(default)]
    sr_code: String,
    #[serde(default)]
    sr_name: String,
    #[serde(default)]
    sr_value: String,
    sr_proof: Option<IncomingFile>,
    #[serde(default)]
    sec_code: String,
    #[serde(default)]
    sec_name: String,
    #[serde(default)]
    sec_value: String,
    sec_proof: Option<IncomingFile>,
    #[serde(default)]
    cancel_reason: String,
    #[serde(default)]
    cancel_date: String,
}

#[derive(Deserialize)]
struct EmailBody {
    email: String,
}

fn main() {
    let port = env::var("PORT").unwrap_or_else(|_| "10001".to_string());
    let dir = env::var("DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"));
    let app = Arc::new(Mutex::new(App::load(dir)));
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).unwrap_or_else(|err| {
        eprintln!("failed to bind {addr}: {err}");
        std::process::exit(1);
    });
    eprintln!("stock correction survey listening on {addr}");

    for incoming in listener.incoming() {
        match incoming {
            Ok(stream) => {
                let app = Arc::clone(&app);
                std::thread::spawn(move || handle(stream, &app));
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }
}

fn handle(mut stream: TcpStream, app: &Arc<Mutex<App>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(60)));
    let req = match read_request(&mut stream) {
        Some(req) => req,
        None => return,
    };
    let path = req.path.split('?').next().unwrap_or("/");
    let result = if req.method == "GET" || req.method == "HEAD" {
        get(path, app)
    } else if req.method == "POST" {
        post(path, &req.body, app)
    } else if req.method == "DELETE" {
        delete(path, &req.body, app)
    } else {
        text(405, "text/plain; charset=utf-8", "Method not allowed")
    };
    let (status, content_type, extra, body) = result;
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\nCache-Control: no-cache\r\n{extra}\r\n",
        body.len()
    );
    if stream.write_all(head.as_bytes()).is_err() {
        return;
    }
    if req.method != "HEAD" {
        let _ = stream.write_all(&body);
    }
    let _ = stream.flush();
}

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        let n = stream.read(&mut tmp).ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_sep(&buf) {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let first = lines.next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut content_length = 0usize;
    for line in lines {
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }
    if content_length > MAX_BODY {
        return None;
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);
    Some(Request { method, path, body })
}

fn find_sep(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn get(path: &str, app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    match path {
        "/" | "/index.html" | "/survey" => html(SURVEY_HTML),
        "/healthz" => text(200, "text/plain; charset=utf-8", "ok"),
        "/api/responses" => {
            let app = app.lock().unwrap_or_else(|e| e.into_inner());
            let view: Vec<serde_json::Value> = app.responses.iter().map(public_response).collect();
            json_ok(&view)
        }
        "/api/recipients" => {
            let app = app.lock().unwrap_or_else(|e| e.into_inner());
            json_ok(&app.recipients)
        }
        "/api/dump.csv" => {
            let app = app.lock().unwrap_or_else(|e| e.into_inner());
            attachment("stock-correction-responses.csv", responses_csv(&app.responses))
        }
        "/api/stock.csv" => {
            let app = app.lock().unwrap_or_else(|e| e.into_inner());
            attachment("stock-correction-stock.csv", stock_csv(&app.responses))
        }
        path if path.starts_with("/api/files/") => serve_file(path, app),
        _ => text(404, "text/plain; charset=utf-8", "Not found"),
    }
}

fn post(path: &str, body: &[u8], app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    match path {
        "/api/responses" => save_response(body, app),
        "/api/recipients" => add_recipient(body, app),
        "/api/mail" => mail_dump(app),
        _ => text(404, "text/plain; charset=utf-8", "Not found"),
    }
}

fn delete(path: &str, body: &[u8], app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    match path {
        "/api/recipients" => remove_recipient(body, app),
        path if path.starts_with("/api/responses/") => {
            let id = path.trim_start_matches("/api/responses/");
            remove_response(id, app)
        }
        _ => text(404, "text/plain; charset=utf-8", "Not found"),
    }
}

fn save_response(body: &[u8], app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    let incoming: Incoming = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_err(400, "The response could not be read."),
    };
    if let Err(msg) = validate(&incoming) {
        return json_err(400, &msg);
    }
    let mut app = app.lock().unwrap_or_else(|e| e.into_inner());
    let id = new_id(&app.responses);
    let folder = app.dir.join("files").join(&id);
    let consent = match save_upload(&folder, "consent", incoming.consent.as_ref()) {
        Ok(v) => v,
        Err(msg) => return json_err(400, &msg),
    };
    let sr_proof = match save_upload(&folder, "sales-return-proof", incoming.sr_proof.as_ref()) {
        Ok(v) => v,
        Err(msg) => return json_err(400, &msg),
    };
    let sec_proof = match save_upload(&folder, "secondary-proof", incoming.sec_proof.as_ref()) {
        Ok(v) => v,
        Err(msg) => return json_err(400, &msg),
    };
    let stored = Stored {
        id,
        submitted_at: now_ist(),
        auditor: incoming.auditor.trim().to_string(),
        auditor_other: incoming.auditor_other.trim().to_string(),
        conducted: incoming.conducted,
        date: incoming.date.trim().to_string(),
        db_code: incoming.db_code.trim().to_string(),
        db_name: incoming.db_name.trim().to_string(),
        stock_file_name: incoming.stock_file_name.trim().to_string(),
        stock_rows: incoming.stock_rows,
        consent,
        audit_type: incoming.audit_type.trim().to_string(),
        sr_code: incoming.sr_code.trim().to_string(),
        sr_name: incoming.sr_name.trim().to_string(),
        sr_value: incoming.sr_value.trim().to_string(),
        sr_proof,
        sec_code: incoming.sec_code.trim().to_string(),
        sec_name: incoming.sec_name.trim().to_string(),
        sec_value: incoming.sec_value.trim().to_string(),
        sec_proof,
        cancel_reason: incoming.cancel_reason.trim().to_string(),
        cancel_date: incoming.cancel_date.trim().to_string(),
    };
    app.responses.push(stored);
    if let Err(err) = app.save_responses() {
        app.responses.pop();
        return json_err(500, &format!("Could not save the response: {err}"));
    }
    let saved = app.responses.last().unwrap();
    json_ok(&public_response(saved))
}

fn validate(inc: &Incoming) -> Result<(), String> {
    if inc.auditor.trim().is_empty() {
        return Err("Auditor Name is required.".into());
    }
    if inc.auditor == "Other" && inc.auditor_other.trim().is_empty() {
        return Err("Enter the auditor name for Other.".into());
    }
    if inc.conducted != "Yes" && inc.conducted != "No" {
        return Err("Was the Audit conducted as planned is required.".into());
    }
    if inc.conducted == "No" {
        if inc.cancel_reason.trim().is_empty() {
            return Err("What is the reason for Audit Cancellation is required.".into());
        }
        if inc.cancel_date.trim().is_empty() {
            return Err("When did the Audit get cancelled is required.".into());
        }
        return Ok(());
    }
    if inc.date.trim().is_empty() {
        return Err("Date of audit is required.".into());
    }
    if inc.db_code.trim().is_empty() {
        return Err("DB Code is required.".into());
    }
    if inc.db_name.trim().is_empty() {
        return Err("DB Name is required.".into());
    }
    if inc.stock_rows.is_empty() {
        return Err("Physical Stock file has to be read before you continue.".into());
    }
    if inc.consent.is_none() {
        return Err("Distributor consent form is required.".into());
    }
    match inc.audit_type.as_str() {
        "Sales Return" => require_sales(inc)?,
        "Secondary" => require_secondary(inc)?,
        "Both (Sales Return + Secondary)" => {
            require_sales(inc)?;
            require_secondary(inc)?;
        }
        _ => return Err("What is the Audit which you are doing is required.".into()),
    }
    Ok(())
}

fn require_sales(inc: &Incoming) -> Result<(), String> {
    if inc.sr_code.trim().is_empty() || inc.sr_name.trim().is_empty() || inc.sr_value.trim().is_empty() {
        return Err("Sales return outlet code, name and value are required.".into());
    }
    if inc.sr_proof.is_none() {
        return Err("Proof of sales return is required.".into());
    }
    Ok(())
}

fn require_secondary(inc: &Incoming) -> Result<(), String> {
    if inc.sec_code.trim().is_empty() || inc.sec_name.trim().is_empty() || inc.sec_value.trim().is_empty()
    {
        return Err("Secondary outlet code, name and value are required.".into());
    }
    if inc.sec_proof.is_none() {
        return Err("Proof of secondary is required.".into());
    }
    Ok(())
}

fn save_upload(folder: &Path, kind: &str, file: Option<&IncomingFile>) -> Result<Option<StoredFile>, String> {
    let Some(file) = file else {
        return Ok(None);
    };
    if file.name.trim().is_empty() {
        return Err("An uploaded file is missing its name.".into());
    }
    let bytes = decode_b64(&file.data_base64)?;
    if bytes.len() > MAX_FILE {
        return Err(format!("{} is larger than 8 MB.", file.name));
    }
    fs::create_dir_all(folder).map_err(|e| e.to_string())?;
    let stored_name = format!("{kind}__{}", safe_name(&file.name));
    let path = folder.join(&stored_name);
    fs::write(&path, &bytes).map_err(|e| e.to_string())?;
    Ok(Some(StoredFile {
        name: file.name.trim().to_string(),
        content_type: if file.content_type.trim().is_empty() {
            "application/octet-stream".into()
        } else {
            file.content_type.trim().to_string()
        },
        path: path.to_string_lossy().to_string(),
    }))
}

fn decode_b64(data: &str) -> Result<Vec<u8>, String> {
    let data = data.trim();
    let data = data.rsplit_once(',').map(|(_, rest)| rest).unwrap_or(data);
    base64::engine::general_purpose::STANDARD
        .decode(data.trim())
        .map_err(|_| "An uploaded file could not be read.".to_string())
}

fn add_recipient(body: &[u8], app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    let incoming: EmailBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_err(400, "Enter an email address."),
    };
    let emails = split_emails(&incoming.email);
    if emails.is_empty() {
        return json_err(400, "Enter an email address.");
    }
    for email in &emails {
        if !valid_email(email) {
            return json_err(400, &format!("{email} is not a valid email address."));
        }
    }
    let mut app = app.lock().unwrap_or_else(|e| e.into_inner());
    for email in emails {
        if !app.recipients.iter().any(|e| e.eq_ignore_ascii_case(&email)) {
            app.recipients.push(email);
        }
    }
    if let Err(err) = app.save_recipients() {
        return json_err(500, &format!("Could not save the mailbox: {err}"));
    }
    json_ok(&app.recipients)
}

fn remove_recipient(body: &[u8], app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    let incoming: EmailBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return json_err(400, "Enter an email address."),
    };
    let email = incoming.email.trim();
    let mut app = app.lock().unwrap_or_else(|e| e.into_inner());
    app.recipients.retain(|e| !e.eq_ignore_ascii_case(email));
    if let Err(err) = app.save_recipients() {
        return json_err(500, &format!("Could not save the mailbox: {err}"));
    }
    json_ok(&app.recipients)
}

fn remove_response(id: &str, app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    let mut app = app.lock().unwrap_or_else(|e| e.into_inner());
    let Some(pos) = app.responses.iter().position(|r| r.id == id) else {
        return json_err(404, "That response was not found.");
    };
    let removed = app.responses.remove(pos);
    if let Err(err) = app.save_responses() {
        app.responses.insert(pos, removed);
        return json_err(500, &format!("Could not update the responses: {err}"));
    }
    let _ = fs::remove_dir_all(app.dir.join("files").join(id));
    json_ok(&serde_json::json!({ "ok": true }))
}

fn mail_dump(app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    let (recipients, responses, stock, count) = {
        let app = app.lock().unwrap_or_else(|e| e.into_inner());
        if app.recipients.is_empty() {
            return json_err(400, "Add at least one email address in the mailbox.");
        }
        if app.responses.is_empty() {
            return json_err(400, "There are no responses to mail yet.");
        }
        (
            app.recipients.clone(),
            responses_csv(&app.responses),
            stock_csv(&app.responses),
            app.responses.len(),
        )
    };
    let host = match env::var("SMTP_HOST") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            return json_err(
                400,
                "Mail is not set up on the server yet. Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS and SMTP_FROM, then try again. The dump can still be downloaded.",
            )
        }
    };
    let port: u16 = env::var("SMTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(587);
    let user = env::var("SMTP_USER").unwrap_or_default();
    let pass = env::var("SMTP_PASS").unwrap_or_default();
    let from = env::var("SMTP_FROM").unwrap_or_else(|_| user.clone());
    if user.is_empty() || pass.is_empty() || from.is_empty() {
        return json_err(
            400,
            "Mail is not set up on the server yet. Set SMTP_USER, SMTP_PASS and SMTP_FROM. The dump can still be downloaded.",
        );
    }
    let mut sent = Vec::new();
    let mut failed = Vec::new();
    for email in &recipients {
        match send_dump(&host, port, &user, &pass, &from, email, &responses, &stock, count) {
            Ok(()) => sent.push(email.clone()),
            Err(err) => failed.push(serde_json::json!({ "email": email, "error": err })),
        }
    }
    json_ok(&serde_json::json!({ "sent": sent, "failed": failed }))
}

fn send_dump(
    host: &str,
    port: u16,
    user: &str,
    pass: &str,
    from: &str,
    to: &str,
    responses: &str,
    stock: &str,
    count: usize,
) -> Result<(), String> {
    let body = format!(
        "Stock Correction Audit dump.\n\n{count} response(s) are attached.\n\nstock-correction-responses.csv is the raw data.\nstock-correction-stock.csv is the stock read from each physical stock file."
    );
    let email = Message::builder()
        .from(from.parse().map_err(|_| format!("SMTP_FROM {from} is not a valid email."))?)
        .to(to.parse().map_err(|_| format!("{to} is not a valid email."))?)
        .subject("Stock Correction Audit dump")
        .multipart(
            MultiPart::mixed()
                .singlepart(SinglePart::plain(body))
                .singlepart(
                    Attachment::new("stock-correction-responses.csv".to_string()).body(
                        responses.to_string(),
                        ContentType::parse("text/csv").unwrap(),
                    ),
                )
                .singlepart(
                    Attachment::new("stock-correction-stock.csv".to_string()).body(
                        stock.to_string(),
                        ContentType::parse("text/csv").unwrap(),
                    ),
                ),
        )
        .map_err(|e| e.to_string())?;
    let mailer = SmtpTransport::starttls_relay(host)
        .map_err(|e| e.to_string())?
        .port(port)
        .credentials(Credentials::new(user.to_string(), pass.to_string()))
        .build();
    mailer.send(&email).map_err(|e| e.to_string())?;
    Ok(())
}

fn serve_file(path: &str, app: &Arc<Mutex<App>>) -> (String, String, String, Vec<u8>) {
    let rest = path.trim_start_matches("/api/files/");
    let mut bits = rest.split('/');
    let id = bits.next().unwrap_or("");
    let kind = bits.next().unwrap_or("");
    if bits.next().is_some() || id.is_empty() || kind.is_empty() {
        return text(404, "text/plain; charset=utf-8", "Not found");
    }
    let app = app.lock().unwrap_or_else(|e| e.into_inner());
    let Some(resp) = app.responses.iter().find(|r| r.id == id) else {
        return text(404, "text/plain; charset=utf-8", "Not found");
    };
    let file = match kind {
        "consent" => resp.consent.as_ref(),
        "sales-return" => resp.sr_proof.as_ref(),
        "secondary" => resp.sec_proof.as_ref(),
        _ => None,
    };
    let Some(file) = file else {
        return text(404, "text/plain; charset=utf-8", "Not found");
    };
    let files_root = app.dir.join("files");
    let full = PathBuf::from(&file.path);
    if !full.starts_with(&files_root) {
        return text(404, "text/plain; charset=utf-8", "Not found");
    }
    match fs::read(&full) {
        Ok(bytes) => {
            let extra = format!(
                "Content-Disposition: attachment; filename=\"{}\"\r\n",
                safe_name(&file.name)
            );
            (
                "200 OK".into(),
                safe_type(&file.content_type),
                extra,
                bytes,
            )
        }
        Err(_) => text(404, "text/plain; charset=utf-8", "Not found"),
    }
}

fn public_response(r: &Stored) -> serde_json::Value {
    let file = |id: &str, kind: &str, f: &Option<StoredFile>| {
        f.as_ref().map(|file| {
            serde_json::json!({
                "name": file.name,
                "href": format!("/api/files/{id}/{kind}")
            })
        })
    };
    let saleable: f64 = r.stock_rows.iter().map(|row| row.saleable).sum();
    let unsaleable: f64 = r.stock_rows.iter().map(|row| row.unsaleable).sum();
    serde_json::json!({
        "id": r.id,
        "submittedAt": r.submitted_at,
        "auditor": r.auditor,
        "auditorOther": r.auditor_other,
        "conducted": r.conducted,
        "date": r.date,
        "dbCode": r.db_code,
        "dbName": r.db_name,
        "stockFileName": r.stock_file_name,
        "productCount": r.stock_rows.len(),
        "saleableTotal": saleable,
        "unsaleableTotal": unsaleable,
        "stockRows": r.stock_rows,
        "consent": file(&r.id, "consent", &r.consent),
        "auditType": r.audit_type,
        "srCode": r.sr_code,
        "srName": r.sr_name,
        "srValue": r.sr_value,
        "srProof": file(&r.id, "sales-return", &r.sr_proof),
        "secCode": r.sec_code,
        "secName": r.sec_name,
        "secValue": r.sec_value,
        "secProof": file(&r.id, "secondary", &r.sec_proof),
        "cancelReason": r.cancel_reason,
        "cancelDate": r.cancel_date
    })
}

fn responses_csv(rows: &[Stored]) -> String {
    let mut out = String::from("\u{feff}");
    out.push_str("Submitted at,Auditor,Other auditor,Conducted as planned,Date of audit,DB Code,DB Name,Physical stock file,Products,Saleable total,Unsaleable total,Consent form,Audit type,Sales return outlet code,Sales return outlet name,Value of sales return,Proof of sales return,Secondary outlet code,Secondary outlet name,Value of secondary,Proof of secondary,Cancellation reason,Cancellation date\n");
    for r in rows {
        let saleable: f64 = r.stock_rows.iter().map(|row| row.saleable).sum();
        let unsaleable: f64 = r.stock_rows.iter().map(|row| row.unsaleable).sum();
        let products = r.stock_rows.len().to_string();
        let saleable_text = num(saleable);
        let unsaleable_text = num(unsaleable);
        let consent = r.consent.as_ref().map(|f| f.name.as_str()).unwrap_or("");
        let sr_proof = r.sr_proof.as_ref().map(|f| f.name.as_str()).unwrap_or("");
        let sec_proof = r.sec_proof.as_ref().map(|f| f.name.as_str()).unwrap_or("");
        out.push_str(&csv_line(&[
            &r.submitted_at,
            &r.auditor,
            &r.auditor_other,
            &r.conducted,
            &r.date,
            &r.db_code,
            &r.db_name,
            &r.stock_file_name,
            &products,
            &saleable_text,
            &unsaleable_text,
            consent,
            &r.audit_type,
            &r.sr_code,
            &r.sr_name,
            &r.sr_value,
            sr_proof,
            &r.sec_code,
            &r.sec_name,
            &r.sec_value,
            sec_proof,
            &r.cancel_reason,
            &r.cancel_date,
        ]));
        out.push('\n');
    }
    out
}

fn stock_csv(rows: &[Stored]) -> String {
    let mut out = String::from("\u{feff}");
    out.push_str("Submitted at,Auditor,DB Code,DB Name,Stock file,Product code,Product description,MRP,Saleable,Unsaleable\n");
    for r in rows {
        for row in &r.stock_rows {
            let mrp = num(row.mrp);
            let saleable = num(row.saleable);
            let unsaleable = num(row.unsaleable);
            out.push_str(&csv_line(&[
                &r.submitted_at,
                &r.auditor,
                &r.db_code,
                &r.db_name,
                &r.stock_file_name,
                &row.code,
                &row.desc,
                &mrp,
                &saleable,
                &unsaleable,
            ]));
            out.push('\n');
        }
    }
    out
}

fn csv_line(cells: &[&str]) -> String {
    cells.iter().copied().map(csv_cell).collect::<Vec<_>>().join(",")
}

fn csv_cell(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn num(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{}", n as i64)
    } else {
        format!("{n:.2}")
    }
}

impl App {
    fn load(dir: PathBuf) -> Self {
        let _ = fs::create_dir_all(&dir);
        let responses = read_json(&dir.join("responses.json")).unwrap_or_default();
        let recipients = read_json(&dir.join("recipients.json")).unwrap_or_default();
        Self { dir, responses, recipients }
    }

    fn save_responses(&self) -> Result<(), String> {
        write_json(&self.dir.join("responses.json"), &self.responses)
    }

    fn save_recipients(&self) -> Result<(), String> {
        write_json(&self.dir.join("recipients.json"), &self.recipients)
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Option<T> {
    let text = fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let text = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, text).map_err(|e| e.to_string())?;
    let _ = fs::remove_file(path);
    fs::rename(&tmp, path).map_err(|e| e.to_string())
}

fn html(body: &str) -> (String, String, String, Vec<u8>) {
    text(200, "text/html; charset=utf-8", body)
}

fn text(code: u16, content_type: &str, body: &str) -> (String, String, String, Vec<u8>) {
    (
        status_line(code).into(),
        content_type.into(),
        String::new(),
        body.as_bytes().to_vec(),
    )
}

fn json_ok<T: Serialize>(value: &T) -> (String, String, String, Vec<u8>) {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    ("200 OK".into(), "application/json; charset=utf-8".into(), String::new(), body)
}

fn json_err(code: u16, message: &str) -> (String, String, String, Vec<u8>) {
    let body = serde_json::to_vec(&serde_json::json!({ "error": message })).unwrap();
    (
        status_line(code).into(),
        "application/json; charset=utf-8".into(),
        String::new(),
        body,
    )
}

fn attachment(name: &str, body: String) -> (String, String, String, Vec<u8>) {
    (
        "200 OK".into(),
        "text/csv; charset=utf-8".into(),
        format!("Content-Disposition: attachment; filename=\"{name}\"\r\n"),
        body.into_bytes(),
    )
}

fn status_line(code: u16) -> &'static str {
    match code {
        200 => "200 OK",
        400 => "400 Bad Request",
        404 => "404 Not Found",
        405 => "405 Method Not Allowed",
        _ => "500 Internal Server Error",
    }
}

fn new_id(existing: &[Stored]) -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let mut id = format!("r{millis}");
    let mut n = 1u32;
    while existing.iter().any(|r| r.id == id) {
        id = format!("r{millis}-{n}");
        n += 1;
    }
    id
}

fn now_ist() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() + 5 * 3600 + 30 * 60)
        .unwrap_or(0);
    format!("{} IST", format_utc_date(secs))
}

fn format_utc_date(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { y + 1 } else { y };
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}")
}

fn split_emails(value: &str) -> Vec<String> {
    value
        .split(|c: char| c.is_whitespace() || c == ',' || c == ';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn valid_email(email: &str) -> bool {
    let mut parts = email.split('@');
    let local = parts.next().unwrap_or("");
    let host = parts.next().unwrap_or("");
    parts.next().is_none()
        && !local.is_empty()
        && host.contains('.')
        && !host.starts_with('.')
        && !host.ends_with('.')
        && email.chars().all(|c| !c.is_whitespace())
}

fn safe_type(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .filter(|c| !c.is_control() && *c != '"' && *c != '\\')
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() {
        "application/octet-stream".into()
    } else {
        cleaned.to_string()
    }
}

fn safe_name(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let cleaned = cleaned.trim_matches('.').to_string();
    if cleaned.is_empty() {
        "file".into()
    } else {
        cleaned
    }
}
