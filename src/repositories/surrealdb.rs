// use crate::core::entities::{Breed, Dog, Tag};
// use crate::core::repository::{BreedCreate, BreedQuery, DogCreate, DogQuery, DogUpdate, Pagination, Repository, TagCreate, TagQuery};
// use serde::Deserialize;
// use surrealdb::{sql::Thing, Connection, Surreal};

// pub struct SurrealDB<C>
// where
//     C: Connection,
// {
//     surreal: Surreal<C>,
// }

// impl<C> SurrealDB<C>
// where
//     C: Connection,
// {
//     pub fn new(surreal: Surreal<C>) -> Self {
//         Self { surreal }
//     }
// }

// #[derive(Debug, Deserialize)]
// struct Record {
//     id: Thing,
// }

// impl<C> Repository for SurrealDB<C>
// where
//     C: Connection,
// {
//     async fn create_breed(&self, breed: BreedCreate) -> Result<String, String> {
//         self.surreal
//             .query("CREATE breeds CONTENT $breed")
//             .bind(("breed", breed))
//             .await
//             .map_err(|e| e.to_string())?
//             .take::<Option<Record>>(0)
//             .map_err(|e| e.to_string())?
//             .ok_or("No breed created".to_string())
//             .map(|r| r.id.id.to_string())
//     }

//     async fn create_dog(&self, dog: DogCreate) -> Result<String, String> {
//         let res: Vec<Dog> = self.surreal.create("dogs").content(dog).await.map_err(|e| e.to_string())?;
//         Ok(res[0].id.clone())
//     }

//     async fn create_tag(&self, tag: TagCreate) -> Result<String, String> {
//         let res: Vec<Tag> = self.surreal.create("tags").content(tag).await.map_err(|e| e.to_string())?;
//         Ok(res[0].id.clone())
//     }

//     async fn delete_breeds(&self, query: BreedQuery) -> Result<i32, String> {
//         let mut res = self
//             .surreal
//             .query("DELETE breeds WHERE $id IS NULL OR id = $id RETURN id")
//             .bind(("id", query.id_eq))
//             .await
//             .map_err(|e| e.to_string())?;
//         let ids: Vec<String> = res.take(0).map_err(|e| e.to_string())?;
//         Ok(ids.len() as i32)
//     }

//     async fn delete_dogs(&self, query: DogQuery) -> Result<i32, String> {
//         let mut res = self
//             .surreal
//             .query("DELETE dogs WHERE $id IS NULL OR id = $id AND $owner_id IS NULL OR owner.id = $owner_id RETURN id")
//             .bind(("id", query.id_eq))
//             .bind(("owner_id", query.owner_id_eq))
//             .await
//             .map_err(|e| e.to_string())?;
//         let ids: Vec<String> = res.take(0).map_err(|e| e.to_string())?;
//         Ok(ids.len() as i32)
//     }

//     async fn delete_tags(&self, query: TagQuery) -> Result<i32, String> {
//         let mut res = self
//             .surreal
//             .query("DELETE tags WHERE $id IS NULL OR id = $id RETURN id")
//             .bind(("id", query.id_eq))
//             .await
//             .map_err(|e| e.to_string())?;
//         let ids: Vec<String> = res.take(0).map_err(|e| e.to_string())?;
//         Ok(ids.len() as i32)
//     }

//     async fn query_breeds(&self, query: BreedQuery, page: Option<Pagination>) -> Result<(Vec<Breed>, i32), String> {
//         let total = self
//             .surreal
//             .query("SELECT VALUE COUNT() FROM breeds WHERE $id IS NONE OR id = $id GROUP ALL")
//             .bind(("id", query.id_eq.clone()))
//             .await
//             .map_err(|e| e.to_string())?
//             .take::<Option<i32>>(0)
//             .map_err(|e| e.to_string())?
//             .ok_or("No total found")?;
//         let breeds = self
//             .surreal
//             .query("SELECT * FROM breeds WHERE $id IS NULL OR id = $id LIMIT $limit START $start")
//             .bind(("id", query.id_eq.clone()))
//             .bind(("limit", page.as_ref().map(|p| p.size)))
//             .bind(("start", page.as_ref().map(|p| p.page * p.size + 1)))
//             .await
//             .map_err(|e| e.to_string())?
//             .take(0)
//             .map_err(|e| e.to_string())?;
//         Ok((breeds, total))
//     }

//     async fn query_dogs(&self, query: DogQuery, page: Option<Pagination>) -> Result<(Vec<Dog>, i32), String> {
//         let total = self
//             .surreal
//             .query("SELECT COUNT(id) FROM dogs WHERE ($id IS NULL OR id = $id) AND ($owner_id IS NULL OR owner.id = $owner_id)")
//             .bind(("id", query.id_eq.clone()))
//             .bind(("owner_id", query.owner_id_eq.clone()))
//             .await
//             .map_err(|e| e.to_string())?
//             .take::<Option<i32>>(0)
//             .map_err(|e| e.to_string())?
//             .ok_or("No total found")?;
//         let dogs = self
//             .surreal
//             .query("SELECT * FROM breeds WHERE ($id IS NULL OR id = $id) AND ($owner_id IS NULL OR owner.id = $owner_id) LIMIT $limit START $start")
//             .bind(("id", query.id_eq.clone()))
//             .bind(("owner_id", query.owner_id_eq.clone()))
//             .bind(("limit", page.as_ref().map(|p| p.size)))
//             .bind(("start", page.as_ref().map(|p| p.page * p.size + 1)))
//             .await
//             .map_err(|e| e.to_string())?
//             .take(0)
//             .map_err(|e| e.to_string())?;
//         Ok((dogs, total))
//     }

//     async fn query_tags(&self, query: TagQuery, page: Option<Pagination>) -> Result<(Vec<Tag>, i32), String> {
//         let total = self
//             .surreal
//             .query("SELECT COUNT(id) FROM tags WHERE $id IS NULL OR id = $id")
//             .bind(("id", query.id_eq.clone()))
//             .await
//             .map_err(|e| e.to_string())?
//             .take::<Option<i32>>(0)
//             .map_err(|e| e.to_string())?
//             .ok_or("No total found")?;
//         let tags = self
//             .surreal
//             .query("SELECT * FROM tags WHERE $id IS NULL OR id = $id LIMIT $limit START $start")
//             .bind(("id", query.id_eq.clone()))
//             .bind(("limit", page.as_ref().map(|p| p.size)))
//             .bind(("start", page.as_ref().map(|p| p.page * p.size + 1)))
//             .await
//             .map_err(|e| e.to_string())?
//             .take(0)
//             .map_err(|e| e.to_string())?;
//         Ok((tags, total))
//     }

//     async fn update_dogs(&self, query: DogQuery, dog: DogUpdate) -> Result<i32, String> {
//         self.surreal
//             .query("UPDATE dogs MERGE $dog WHERE ($id IS NULL OR id = $id) AND ($owner_id IS NULL OR owner.id = $owner_id) RETURN id")
//             .bind(("dog", dog))
//             .bind(("id", query.id_eq))
//             .bind(("owner_id", query.owner_id_eq))
//             .await
//             .map_err(|e| e.to_string())?
//             .take::<Vec<String>>(0)
//             .map_err(|e| e.to_string())
//             .map(|v| v.len() as i32)
//     }
// }

// #[cfg(test)]
// mod test {
//     use super::*;

//     use surrealdb::engine::remote::ws::Ws;

//     #[tokio::test]
//     async fn test_create_breed() {
//         let surreal = Surreal::new::<Ws>("localhost:8000").await.expect("failed to connect to surrealdb");
//         surreal.use_ns("little-walk").use_db("dogs").await.expect("failed to use database");
//         let id = SurrealDB::new(surreal).create_breed(BreedCreate { name: "金毛".into() }).await.expect("failed to create breed");
//         println!("id: {}", id);
//     }

//     #[tokio::test]
//     async fn test_query_breeds() {
//         let surreal = Surreal::new::<Ws>("localhost:8000").await.expect("failed to connect to surrealdb");
//         surreal.use_ns("little-walk").use_db("dogs").await.expect("failed to use database");
//         let (breeds, total) = SurrealDB::new(surreal).query_breeds(BreedQuery { id_eq: None }, None).await.expect("failed to create breed");
//         println!("breeds: {:?}", breeds);
//         println!("total: {}", total);
//     }
// }
