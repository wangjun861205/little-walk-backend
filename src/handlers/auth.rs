use actix_web::{
    error::{ErrorInternalServerError, ErrorUnauthorized},
    web::{Data, Json, Path},
    Error,
};

use auth_service::core::{
    hasher::Hasher, repository::Repository, service::Service, token_manager::TokenManager,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct LoginByPasswordParams {
    phone: String,
    password: String,
}

#[derive(Debug, Serialize)]
pub struct LoginByPasswordResp {
    token: String,
}

pub async fn login_by_password<R, H, T>(
    service: Data<Service<R, H, T>>,
    Json(params): Json<LoginByPasswordParams>,
) -> Result<Json<LoginByPasswordResp>, Error>
where
    R: Repository + Clone,
    H: Hasher + Clone,
    T: TokenManager + Clone,
{
    Ok(Json(LoginByPasswordResp {
        token: service
            .login_by_password(&params.phone, &params.password)
            .await
            .map_err(ErrorInternalServerError)?,
    }))
}

#[derive(Debug, Serialize)]
pub struct VerifyTokenResp {
    id: String,
}

pub async fn verify_token<R, H, T>(
    service: Data<Service<R, H, T>>,
    token: Path<(String,)>,
) -> Result<Json<VerifyTokenResp>, Error>
where
    R: Repository + Clone,
    H: Hasher + Clone,
    T: TokenManager + Clone,
{
    let id = service
        .verify_token(&token.0)
        .await
        .map_err(ErrorUnauthorized)?;
    Ok(Json(VerifyTokenResp { id }))
}

#[derive(Debug, Deserialize)]
pub struct SignupParams {
    phone: String,
    password: String,
}

#[derive(Debug, Serialize)]
pub struct SignupResp {
    token: String,
}

pub async fn signup<R, H, T>(
    service: Data<Service<R, H, T>>,
    Json(params): Json<SignupParams>,
) -> Result<Json<SignupResp>, Error>
where
    R: Repository + Clone,
    H: Hasher + Clone,
    T: TokenManager + Clone,
{
    let token = service
        .signup(&params.phone, &params.password)
        .await
        .map_err(ErrorInternalServerError)?;
    Ok(Json(SignupResp { token }))
}

#[derive(Debug, Serialize)]
pub struct ExistsUserResp {
    exists: bool,
}

pub async fn exists_user<R, H, T>(
    service: Data<Service<R, H, T>>,
    phone: Path<(String,)>,
) -> Result<Json<ExistsUserResp>, Error>
where
    R: Repository + Clone,
    H: Hasher + Clone,
    T: TokenManager + Clone,
{
    let exists = service
        .exists_user(&phone.to_owned().0)
        .await
        .map_err(ErrorInternalServerError)?;
    Ok(Json(ExistsUserResp { exists }))
}

#[derive(Debug, Serialize)]
pub struct GenerateTokenResp {
    token: String,
}

pub async fn generate_token<R, H, T>(
    service: Data<Service<R, H, T>>,
    phone: Path<(String,)>,
) -> Result<Json<GenerateTokenResp>, Error>
where
    R: Repository + Clone,
    H: Hasher + Clone,
    T: TokenManager + Clone,
{
    let token = service
        .generate_token(&phone.to_owned().0)
        .await
        .map_err(ErrorInternalServerError)?;
    Ok(Json(GenerateTokenResp { token }))
}
