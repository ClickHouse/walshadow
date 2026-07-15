//! Response construction over hyper types, filling axum's IntoResponse role

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::header::{CONTENT_TYPE, HeaderValue};
use hyper::{Response, StatusCode};
use serde::Serialize;
use serde_json::json;

use crate::error::GrpcError;

pub struct Json<T>(pub T);

pub trait IntoResponse {
    fn into_response(self) -> Response<Full<Bytes>>;
}

fn json_response(status: StatusCode, buf: Vec<u8>) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::from(buf));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

impl IntoResponse for Response<Full<Bytes>> {
    fn into_response(self) -> Response<Full<Bytes>> {
        self
    }
}

impl<T: Serialize> IntoResponse for Json<T> {
    fn into_response(self) -> Response<Full<Bytes>> {
        match serde_json::to_vec(&self.0) {
            Ok(buf) => json_response(StatusCode::OK, buf),
            Err(e) => GrpcError::internal(format!("serialize response: {e}")).into_response(),
        }
    }
}

impl IntoResponse for GrpcError {
    fn into_response(self) -> Response<Full<Bytes>> {
        let body = json!({
            "code": self.code as i32,
            "message": self.message,
            "details": [],
        });
        json_response(
            self.code.http_status(),
            serde_json::to_vec(&body).unwrap_or_default(),
        )
    }
}

impl<T: IntoResponse> IntoResponse for Result<T, GrpcError> {
    fn into_response(self) -> Response<Full<Bytes>> {
        match self {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        }
    }
}
