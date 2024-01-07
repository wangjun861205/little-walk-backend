use actix_web::dev::{Service, ServiceRequest, ServiceResponse, Transform};
use actix_web::http::header::{HeaderName, HeaderValue};
use std::future::{ready, Ready};
use std::pin::Pin;
use std::str::FromStr;

pub struct ResponseEncoding;

impl<S> Transform<S, ServiceRequest> for ResponseEncoding
where
    S: Service<ServiceRequest, Response = ServiceResponse>,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = S::Error;
    type Transform = ResponseEncodingService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(ResponseEncodingService { next: service }))
    }
}

pub struct ResponseEncodingService<S>
where
    S: Service<ServiceRequest>,
{
    next: S,
}

impl<S> Service<ServiceRequest> for ResponseEncodingService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse>,
    S::Future: 'static,
{
    type Response = ServiceResponse;
    type Error = S::Error;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, ctx: &mut core::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.next.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let future = self.next.call(req);
        Box::pin(async move {
            let mut res = future.await?;
            res.headers_mut()
                .insert(HeaderName::from_str("Content-Type").unwrap(), HeaderValue::from_str("application/json; charset=utf-8").unwrap());
            Ok(res)
        })
    }
}
