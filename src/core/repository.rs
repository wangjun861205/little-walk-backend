use crate::core::entities::WalkRequest;
use crate::core::entities::{Breed, Category, Dog};
use crate::core::error::Error;
use chrono::{DateTime, Utc};
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Pagination {
    pub limit: i64,
    pub skip: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BreedCreate {
    pub category: Category,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BreedUpdate {
    pub name: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct BreedQuery {
    pub id: Option<String>,
    pub category: Option<Category>,
    pub name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DogCreate {
    pub owner_id: String,
    pub name: String,
    pub gender: String,
    pub breed: BreedQuery,       // 品种
    pub birthday: DateTime<Utc>, // 生日
    // pub is_sterilized: bool,     // 是否绝育
    // pub introduction: String,
    pub tags: Vec<String>,
    pub portrait_id: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DogUpdate {
    pub name: Option<String>,
    pub gender: Option<String>,
    pub breed: Option<BreedQuery>,   // 品种
    pub birthday: Option<String>,    // 生日
    pub is_sterilized: Option<bool>, // 是否绝育
    pub introduction: Option<String>,
    pub owner_id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub portrait_id: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DogQuery {
    pub id: Option<String>,
    pub id_in: Option<Vec<String>>,
    pub owner_id: Option<String>,
    pub pagination: Option<Pagination>,
}

pub trait Repository {
    async fn create_breed(&self, breed: &BreedCreate) -> Result<String, Error>;
    async fn delete_breed(&self, id: &str) -> Result<bool, Error>;
    async fn query_breeds(&self, query: &BreedQuery) -> Result<(Vec<Breed>, i64), Error>;
    async fn create_dog(&self, dog: &DogCreate) -> Result<Dog, Error>;
    async fn delete_dog(&self, id: &str) -> Result<bool, Error>;
    async fn update_dog(&self, id: &str, dog: &DogUpdate) -> Result<bool, Error>;
    async fn query_dogs(&self, query: &DogQuery) -> Result<Vec<Dog>, Error>;
    async fn exists_dog(&self, query: &DogQuery) -> Result<bool, Error>;
    async fn create_walk_request(&self, request: WalkRequestCreate) -> Result<String, Error>;
    async fn update_walk_request(
        &self,
        id: &str,
        request: WalkRequestUpdate,
    ) -> Result<WalkRequest, Error>;
    async fn update_walk_request_by_query(
        &self,
        query: WalkRequestQuery,
        update: WalkRequestUpdate,
    ) -> Result<WalkRequest, Error>;
    async fn update_walk_requests_by_query(
        &self,
        query: WalkRequestQuery,
        update: WalkRequestUpdate,
    ) -> Result<u64, Error>;
    async fn get_walk_request(&self, id: &str) -> Result<WalkRequest, Error>;
    async fn query_walk_requests(
        &self,
        query: WalkRequestQuery,
        sort_by: Option<SortBy>,
        pagination: Option<Pagination>,
    ) -> Result<Vec<WalkRequest>, Error>;
    async fn create_walking_location(&self, create: WalkingLocationCreate)
        -> Result<String, Error>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WalkRequestCreate {
    pub dogs: Vec<Dog>,
    pub should_start_after: Option<DateTime<Utc>>,
    pub should_start_before: Option<DateTime<Utc>>,
    pub should_end_before: Option<DateTime<Utc>>,
    pub should_end_after: Option<DateTime<Utc>>,
    pub latitude: f64,
    pub longitude: f64,
    #[serde(default = "empty_string")]
    pub created_by: String,
}

fn empty_string() -> String {
    String::new()
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct WalkRequestUpdate {
    pub dogs: Option<Vec<Dog>>,
    pub should_start_after: Option<DateTime<Utc>>,
    pub should_start_before: Option<DateTime<Utc>>,
    pub should_end_before: Option<DateTime<Utc>>,
    pub should_end_after: Option<DateTime<Utc>>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub accepted_by: Option<String>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub canceled_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub unset_accepted_by: bool,
    pub unset_accepted_at: bool,
    pub add_to_acceptances: Option<String>,
    pub remove_from_acceptances: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct WalkRequestQuery {
    pub id: Option<String>,
    pub dog_ids_includes_all: Option<Vec<String>>,
    pub dog_ids_includes_any: Option<Vec<String>>,
    pub nearby: Option<Vec<f64>>,
    pub accepted_by: Option<String>,
    pub accepted_by_neq: Option<String>,
    pub accepted_by_is_null: Option<bool>,
    pub acceptances_includes_all: Option<Vec<String>>,
    pub acceptances_includes_any: Option<Vec<String>>,
    pub created_by: Option<String>,
}

pub struct WalkingLocationCreate<'a> {
    pub walk_request_id: &'a str,
    pub longitude: f64,
    pub latitude: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SortBy {
    pub field: String,
    pub order: Order,
}
