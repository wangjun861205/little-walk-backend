mod core;
mod handlers;
mod middlewares;
mod repositories;

use std::io;

use actix_web::{
    middleware::Logger,
    web::{get, post, put, resource, scope, Data},
    App, HttpServer,
};
use auth_service::{
    core::service::Service, hashers::sha::ShaHasher, repositories::mongo::MongodbRepository,
    token_managers::jwt::JWTTokenManager,
};
use core::service::Service as DogService;
use handlers::{auth, upload};
use hmac::{Hmac, Mac};
use middlewares::response_encoding::ResponseEncoding;
use mongodb::Client;
use nb_from_env::{FromEnv, FromEnvDerive};
use repositories::mongodb::MongoDB;
use sha2::Sha384;
use upload_service::{
    core::service::Service as UploadService, repositories::mongo::Mongo,
    stores::local_fs::LocalFSStore,
};

#[derive(Debug, FromEnvDerive)]
pub struct Config {
    server_address: String,
    db_uri: String,
    secret: String,
    store_path: String,
    #[env_default("info")]
    log_level: String,
    #[env_default("%t %s %r %D")]
    log_format: String,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    dotenv::dotenv().ok();
    let config = Config::from_env();
    env_logger::init_from_env(env_logger::Env::default().default_filter_or(config.log_level));
    let db = Client::with_uri_str(&config.db_uri)
        .await
        .expect("failed to connect to mongodb")
        .database("little-walk-auth");

    let service = Data::new(Service::<
        MongodbRepository,
        ShaHasher,
        JWTTokenManager<Hmac<Sha384>>,
    >::new(
        MongodbRepository::new(db.clone()),
        ShaHasher,
        JWTTokenManager::new(
            Hmac::new_from_slice(config.secret.as_bytes())
                .expect("failed to create jwt signing key"),
        ),
    ));

    let upload_service = Data::new(UploadService::<Mongo, LocalFSStore>::new(
        Mongo::new(db.clone()),
        LocalFSStore::new(&config.store_path),
    ));

    let dog_service = Data::new(DogService::new(MongoDB::new(db)));

    HttpServer::new(move || {
        let logger = Logger::new(&config.log_format);
        App::new()
            .wrap(ResponseEncoding)
            .wrap(logger)
            .app_data(service.clone())
            .app_data(upload_service.clone())
            .app_data(dog_service.clone())
            .route(
                "/login",
                put().to(auth::login_by_password::<
                    MongodbRepository,
                    ShaHasher,
                    JWTTokenManager<Hmac<Sha384>>,
                >),
            )
            .route(
                "/signup",
                post().to(auth::signup::<
                    MongodbRepository,
                    ShaHasher,
                    JWTTokenManager<Hmac<Sha384>>,
                >),
            )
            .route(
                "/tokens/{token}/verification",
                get().to(auth::verify_token::<
                    MongodbRepository,
                    ShaHasher,
                    JWTTokenManager<Hmac<Sha384>>,
                >),
            )
            .route(
                "/phones/{phone}/tokens",
                put().to(auth::generate_token::<
                    MongodbRepository,
                    ShaHasher,
                    JWTTokenManager<Hmac<Sha384>>,
                >),
            )
            .route(
                "/phones/{phone}/exists",
                get().to(auth::exists_user::<
                    MongodbRepository,
                    ShaHasher,
                    JWTTokenManager<Hmac<Sha384>>,
                >),
            )
            .service(
                scope("/apis").service(
                    scope("/uploads")
                        .route("/{id}", get().to(upload::get::<Mongo, LocalFSStore>))
                        .route("", post().to(upload::upload::<Mongo, LocalFSStore>)),
                ),
            )
            .service(
                scope("apis")
                    .service(
                        resource("breeds")
                            .post(handlers::breed::create_breed::<MongoDB>)
                            .get(handlers::breed::breeds::<MongoDB>),
                    )
                    .service(
                        scope("dogs")
                            .route("", post().to(handlers::dog::create_dog::<MongoDB>))
                            .route("", get().to(handlers::dog::dogs::<MongoDB>))
                            .route("", put().to(handlers::dog::update_dog::<MongoDB>))
                            .route("mine", get().to(handlers::dog::my_dogs::<MongoDB>))
                            .route(
                                "exists",
                                get().to(handlers::dog::is_owner_of_the_dog::<MongoDB>),
                            )
                            .route(
                                "{id}/portrait",
                                put().to(handlers::dog::update_dog_portrait::<MongoDB>),
                            )
                            .route("{id}", put().to(handlers::dog::update_dog::<MongoDB>)),
                    ),
            )
    })
    .bind(config.server_address)?
    .run()
    .await
}
