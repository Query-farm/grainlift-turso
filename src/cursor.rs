// Copyright (c) 2026 Query Farm LLC
// SPDX-License-Identifier: Apache-2.0

//! Streamed Turso Cloud query results.
//!
//! `turso_serverless` reads a query's whole result before returning it. Turso
//! Cloud's cursor endpoint sends rows as newline-delimited JSON, so this reads
//! that response incrementally instead: a result is held one batch at a time,
//! however large it is, and the HTTP connection applies backpressure while the
//! client is not fetching.
//!
//! Each cursor runs on a new server-side stream (a separate database
//! connection), so it is used only outside transactions; see
//! [`crate::db::Connection::query`].

use adbc_core::error::Result;
use turso_serverless::protocol::{
    Batch, BatchStep, CursorEntry, CursorRequest, CursorResponse, PipelineRequest, Stmt,
    StreamRequest, decode_value_owned, encode_value,
};

use crate::db::{Column, Value, from_remote, remote_error, to_remote};

type Failure = turso_serverless::Error;

/// Where Turso Cloud queries go and how they authenticate.
#[derive(Clone)]
pub struct Endpoint {
    client: reqwest::Client,
    /// `https://` base URL.
    url: String,
    auth_token: Option<String>,
}

impl Endpoint {
    /// An endpoint for `url`. Connecting is bounded here; each request is
    /// bounded by its operation's deadline (see [`crate::ops`]).
    pub fn new(url: &str, auth_token: Option<String>) -> adbc_core::error::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .build()
            .map_err(|failure| {
                http_failure(format!("could not create the HTTP client: {failure}"))
            })?;
        Ok(Self {
            client,
            url: protocol_url(url),
            auth_token,
        })
    }

    async fn post(&self, base_url: &str, path: &str, body: String) -> Result<reqwest::Response> {
        let mut request = self
            .client
            .post(format!("{base_url}{path}"))
            .header("Content-Type", "application/json")
            .body(body);
        if let Some(token) = &self.auth_token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|failure| http_failure(format!("request failed: {failure}")))?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        // Match turso_serverless's wording so errors classify the same way.
        let detail = response
            .json::<serde_json::Value>()
            .await
            .ok()
            .and_then(|body| {
                let message = body.get("error").or_else(|| body.get("message"))?;
                message.as_str().map(str::to_string)
            });
        Err(http_failure(match detail {
            Some(detail) => format!("HTTP status {status}: {detail}"),
            None => format!("HTTP status {status}"),
        }))
    }
}

fn http_failure(message: String) -> adbc_core::error::Error {
    remote_error(Failure::Http(message))
}

/// The rows of one query, read from the response as they are needed.
pub struct Cursor {
    endpoint: Endpoint,
    response: reqwest::Response,
    buffer: Vec<u8>,
    columns: Vec<Column>,
    /// The server-side stream, closed once the result is read.
    baton: Option<String>,
    base_url: String,
    done: bool,
}

impl Cursor {
    /// Start `sql` and read up to its column list.
    pub async fn open(endpoint: &Endpoint, sql: String, params: Vec<Value>) -> Result<Self> {
        let mut stmt = Stmt::new(sql, true);
        stmt.args = params
            .iter()
            .map(|value| encode_value(&to_remote(value.clone())))
            .collect::<turso_serverless::Result<_>>()
            .map_err(remote_error)?;
        let request = CursorRequest {
            baton: None,
            batch: Batch {
                steps: vec![BatchStep {
                    condition: None,
                    stmt,
                }],
            },
        };
        let body = serde_json::to_string(&request)
            .map_err(|failure| http_failure(format!("could not encode the request: {failure}")))?;
        let response = endpoint.post(&endpoint.url, "/v3/cursor", body).await?;
        let mut cursor = Self {
            endpoint: endpoint.clone(),
            response,
            buffer: Vec::new(),
            columns: Vec::new(),
            baton: None,
            base_url: endpoint.url.clone(),
            done: false,
        };
        let first = cursor
            .next_line()
            .await?
            .ok_or_else(|| http_failure("the cursor response was empty".into()))?;
        let head: CursorResponse = serde_json::from_str(&first)
            .map_err(|failure| http_failure(format!("invalid cursor response: {failure}")))?;
        cursor.baton = head.baton;
        if let Some(base_url) = head.base_url {
            cursor.base_url = protocol_url(&base_url);
        }
        loop {
            match cursor.next_entry().await? {
                Some(CursorEntry::StepBegin { cols, .. }) => {
                    cursor.columns = cols
                        .into_iter()
                        .map(|column| Column {
                            name: column.name.unwrap_or_default(),
                            decl_type: column.decltype,
                        })
                        .collect();
                    return Ok(cursor);
                }
                Some(CursorEntry::StepError { error, .. } | CursorEntry::Error { error }) => {
                    cursor.finish().await;
                    return Err(remote_error(error.into()));
                }
                Some(_) => {}
                None => {
                    cursor.finish().await;
                    return Err(http_failure(
                        "the cursor ended before the result began".into(),
                    ));
                }
            }
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    /// The next rows, as [`crate::db::Rows::next_rows`].
    pub async fn next_rows(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<Value>>, bool)> {
        let mut rows = Vec::new();
        let mut bytes = 0;
        while !self.done && rows.len() < max_rows && bytes < max_bytes {
            match self.next_entry().await {
                Ok(Some(CursorEntry::Row { row })) => {
                    let row = row
                        .into_iter()
                        .map(|value| decode_value_owned(value).map(from_remote))
                        .collect::<turso_serverless::Result<Vec<_>>>()
                        .map_err(remote_error)?;
                    bytes += crate::types::row_bytes(&row);
                    rows.push(row);
                }
                Ok(Some(CursorEntry::StepEnd { .. })) => self.finish().await,
                Ok(Some(CursorEntry::StepError { error, .. } | CursorEntry::Error { error })) => {
                    self.finish().await;
                    return Err(remote_error(error.into()));
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    self.finish().await;
                    return Err(http_failure(
                        "the cursor ended in the middle of the result".into(),
                    ));
                }
                Err(failure) => {
                    self.done = true;
                    return Err(failure);
                }
            }
        }
        Ok((rows, self.done))
    }

    /// Mark the result read and close its server-side stream. Failures are
    /// ignored: Turso Cloud also expires idle streams.
    async fn finish(&mut self) {
        self.done = true;
        if let Some(baton) = self.baton.take() {
            let request = PipelineRequest {
                baton: Some(baton),
                requests: vec![StreamRequest::Close],
            };
            if let Ok(body) = serde_json::to_string(&request) {
                let _ = self
                    .endpoint
                    .post(&self.base_url, "/v3/pipeline", body)
                    .await;
            }
        }
    }

    async fn next_entry(&mut self) -> Result<Option<CursorEntry>> {
        let Some(line) = self.next_line().await? else {
            return Ok(None);
        };
        serde_json::from_str(&line)
            .map(Some)
            .map_err(|failure| http_failure(format!("invalid cursor entry: {failure}")))
    }

    async fn next_line(&mut self) -> Result<Option<String>> {
        loop {
            if let Some(end) = self.buffer.iter().position(|&byte| byte == b'\n') {
                let line = self.buffer.drain(..=end).collect::<Vec<_>>();
                let line = String::from_utf8(line)
                    .map_err(|_| http_failure("the cursor response is not UTF-8".into()))?;
                if line.trim().is_empty() {
                    continue;
                }
                return Ok(Some(line));
            }
            match self.response.chunk().await {
                Ok(Some(chunk)) => self.buffer.extend_from_slice(&chunk),
                Ok(None) if self.buffer.iter().all(u8::is_ascii_whitespace) => return Ok(None),
                Ok(None) => self.buffer.push(b'\n'),
                Err(failure) => {
                    return Err(http_failure(format!(
                        "the cursor response failed: {failure}"
                    )));
                }
            }
        }
    }
}

/// The protocol's `https://` base URL for a `libsql://`, `turso://` or
/// `https://` database URL, without a trailing slash.
fn protocol_url(url: &str) -> String {
    let url = ["libsql://", "turso://"]
        .iter()
        .find_map(|scheme| url.strip_prefix(scheme))
        .map_or_else(|| url.to_string(), |rest| format!("https://{rest}"));
    url.trim_end_matches('/').to_string()
}
