use hyper::StatusCode;

/// gRPC status codes the shim emits. Wire shape follows grpc-gateway:
/// `{"code": <grpc code>, "message": …, "details": []}` with the gateway's
/// HTTP status mapping
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Code {
    InvalidArgument = 3,
    NotFound = 5,
    AlreadyExists = 6,
    FailedPrecondition = 9,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    Unauthenticated = 16,
}

impl Code {
    pub fn http_status(self) -> StatusCode {
        match self {
            Code::InvalidArgument | Code::FailedPrecondition => StatusCode::BAD_REQUEST,
            Code::NotFound => StatusCode::NOT_FOUND,
            Code::AlreadyExists => StatusCode::CONFLICT,
            Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            Code::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        }
    }
}

#[derive(Debug)]
pub struct GrpcError {
    pub code: Code,
    pub message: String,
}

impl GrpcError {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(Code::InvalidArgument, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(Code::NotFound, message)
    }

    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(Code::AlreadyExists, message)
    }

    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(Code::FailedPrecondition, message)
    }

    pub fn unimplemented(message: impl Into<String>) -> Self {
        Self::new(Code::Unimplemented, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Code::Internal, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(Code::Unavailable, message)
    }
}

impl std::fmt::Display for GrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for GrpcError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_mapping_matches_gateway() {
        assert_eq!(Code::InvalidArgument.http_status(), StatusCode::BAD_REQUEST);
        assert_eq!(Code::NotFound.http_status(), StatusCode::NOT_FOUND);
        assert_eq!(Code::AlreadyExists.http_status(), StatusCode::CONFLICT);
        assert_eq!(
            Code::Unimplemented.http_status(),
            StatusCode::NOT_IMPLEMENTED
        );
        assert_eq!(
            Code::Internal.http_status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            Code::Unavailable.http_status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
