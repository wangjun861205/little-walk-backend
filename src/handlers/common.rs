use actix_web::{error::ErrorBadRequest, Error, FromRequest};
use futures::future::{err, ok, Ready};
use serde::Serialize;

pub struct HeaderUserID(pub String);

impl FromRequest for HeaderUserID {
    type Error = Error;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &actix_web::HttpRequest, _: &mut actix_web::dev::Payload) -> Self::Future {
        match req.headers().get("X-User-ID") {
            Some(hv) => {
                if let Ok(s) = hv.to_str() {
                    return ok(HeaderUserID(s.to_owned()));
                }
                err(ErrorBadRequest("no user id"))
            }
            None => err(ErrorBadRequest("no user id")),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ListResp<T>
where
    T: Serialize,
{
    pub list: Vec<T>,
    pub total: i64,
}

impl<T> ListResp<T>
where
    T: Serialize,
{
    pub fn new(list: Vec<T>, total: i64) -> Self {
        Self { list, total }
    }
}
