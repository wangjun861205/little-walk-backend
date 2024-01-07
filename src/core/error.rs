use std::fmt::{Debug, Display};

pub struct Error {
    message: String,
    cause: Option<Box<dyn Display>>,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(cause) = &self.cause {
            return write!(f, "{}: {}", self.message, cause);
        }
        write!(f, "{}", self.message)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(cause) = &self.cause {
            return write!(f, "{}: {}", self.message, cause);
        }
        write!(f, "{}", self.message)
    }
}

impl Error {
    pub fn new<S>(message: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            message: message.into(),
            cause: None,
        }
    }

    pub fn with_cause(self, cause: impl Display + 'static) -> Self {
        Self {
            cause: Some(Box::new(cause)),
            ..self
        }
    }

    pub fn msg(msg: &str) -> Self {
        Self {
            message: msg.into(),
            cause: None,
        }
    }

    pub fn from_error<E>(err: E) -> Self
    where
        E: Display,
    {
        Self {
            message: err.to_string(),
            cause: None,
        }
    }

    pub fn wrap<E>(err: E, msg: &str) -> Self
    where
        E: Display + 'static,
    {
        Self {
            message: msg.into(),
            cause: Some(Box::new(err)),
        }
    }
}
