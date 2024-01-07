use actix_multipart::Multipart;
use actix_web::{
    error::{ErrorBadRequest, ErrorInternalServerError, ErrorNotFound, Result},
    http::StatusCode,
    web::{Data, Json, Path},
    HttpRequest, HttpResponse,
};
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use upload_service::core::{repository::Repository, service::Service, store::Store};

#[derive(Debug, Serialize)]
pub struct UploadResult {
    ids: Vec<String>,
}

pub(crate) async fn upload<R, S>(
    req: HttpRequest,
    service: Data<Service<R, S>>,
    mut form: Multipart,
) -> Result<Json<UploadResult>>
where
    R: Repository + Clone,
    S: Store + Clone,
{
    let hv = req
        .headers()
        .get("X-User-ID")
        .ok_or(ErrorBadRequest("Unauthorized"))?;
    let uid = hv.to_str().map_err(ErrorBadRequest)?;
    let mut ids = Vec::new();
    while let Some(field) = form.next().await {
        let field = field.map_err(ErrorInternalServerError)?;
        let disposition = field.content_disposition().clone();
        let filename = disposition
            .get_filename()
            .ok_or(ErrorBadRequest("failed to get filename"))?;
        let id = service
            .upload(
                field.map_err(|e| anyhow::Error::msg(e.to_string())),
                filename,
                uid,
                Some(1024 * 1024),
            )
            .await
            .map_err(ErrorInternalServerError)?;
        ids.push(id);
    }
    Ok(Json(UploadResult { ids }))
}

pub(crate) async fn get<R, S>(
    service: Data<Service<R, S>>,
    id: Path<(String,)>,
) -> Result<HttpResponse>
where
    R: Repository + Clone,
    S: Store + Clone,
{
    let file_info = service
        .get_uploaded_file(&id.0)
        .await
        .map_err(ErrorInternalServerError)?
        .ok_or(ErrorNotFound("file not found"))?;
    let stream = service
        .download(&id.0)
        .await
        .map_err(ErrorInternalServerError)?;
    Ok(HttpResponse::build(StatusCode::OK)
        .insert_header(("Content-Type", file_info.mime_type))
        .streaming(stream))
}
