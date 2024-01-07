// use sqlx::PgPool;

// use crate::core::repository::Repository;
// pub struct Postgres {
//     pool: PgPool,
// }

// impl Repository for Postgres {
//     async fn create_breed(&self, breed: crate::core::repository::BreedCreate) -> Result<String, String> {
//         unimplemented!()
//     }

//     async fn create_dog(&self, dog: crate::core::repository::DogCreate) -> Result<String, String> {
//         unimplemented!()
//     }

//     async fn create_tag(&self, tag: crate::core::repository::TagCreate) -> Result<String, String> {
//         unimplemented!()
//     }

//     async fn delete_breeds(&self, query: crate::core::repository::BreedQuery) -> Result<i32, String> {
//         unimplemented!()
//     }

//     async fn delete_dogs(&self, query: crate::core::repository::DogQuery) -> Result<i32, String> {
//         unimplemented!()
//     }

//     async fn delete_tags(&self, query: crate::core::repository::TagQuery) -> Result<i32, String> {
//         unimplemented!()
//     }

//     async fn query_breeds(&self, query: crate::core::repository::BreedQuery, page: Option<crate::core::repository::Pagination>) -> Result<(Vec<crate::core::entities::Breed>, i32), String> {
//         unimplemented!()
//     }

//     async fn query_dogs(&self, query: crate::core::repository::DogQuery, page: Option<crate::core::repository::Pagination>) -> Result<(Vec<crate::core::entities::Dog>, i32), String> {
//         unimplemented!()
//     }

//     async fn query_tags(&self, query: crate::core::repository::TagQuery, page: Option<crate::core::repository::Pagination>) -> Result<(Vec<crate::core::entities::Tag>, i32), String> {
//         unimplemented!()
//     }

//     async fn update_dogs(&self, query: crate::core::repository::DogQuery, dog: crate::core::repository::DogUpdate) -> Result<i32, String> {
//         unimplemented!()
//     }
// }
