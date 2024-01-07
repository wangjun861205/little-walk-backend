use std::ops::Deref;

use mongodb::{
    bson::{doc, from_document, oid::ObjectId, to_document, Bson, Document},
    options::FindOneOptions,
    Database,
};

use crate::core::{
    entities::{Breed, Dog},
    error::Error,
    repository::{BreedCreate, BreedQuery, DogCreate, DogQuery, DogUpdate, Repository},
};

use mongodb::options::FindOptions;

use futures::TryStreamExt;

use chrono::{Local, Utc};

impl TryFrom<&DogCreate> for Document {
    type Error = Error;
    fn try_from(dog: &DogCreate) -> Result<Self, Self::Error> {
        let mut d = to_document(&dog)
            .map_err(|e| Error::new("failed to convert DogCreate to Document").with_cause(e))?;
        d.insert("created_at", Utc::now());
        d.insert("updated_at", Utc::now());
        Ok(d)
    }
}

impl Dog {
    pub fn projection() -> Document {
        doc! {
            "id": {"$toString": "$_id"},
            "name": 1,
            "gender": 1,
            "breed": 1,
            "birthday": 1,
            "owner_id": 1,
            "tags": 1,
            "portrait_id": 1,
        }
    }
}

impl From<Dog> for Bson {
    fn from(value: Dog) -> Self {
        let mut d = to_document(&value).unwrap();
        d.insert("_id", ObjectId::parse_str(&value.id).unwrap());
        d.remove("id");
        Bson::Document(d)
    }
}

pub struct MongoDB {
    db: Database,
}

impl MongoDB {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
}

impl Repository for MongoDB {
    async fn create_breed(&self, breed: &BreedCreate) -> Result<String, Error> {
        let now = Local::now();
        let d = doc! {
            "name": &breed.name,
            "category": &breed.category.to_string(),
            "created_at": now.to_rfc3339(),
            "updated_at": now.to_rfc3339(),
        };
        let res = self
            .db
            .collection::<Document>("breeds")
            .insert_one(d, None)
            .await
            .map_err(|e| Error::new("failed to create breed").with_cause(e))?;
        res.inserted_id
            .as_object_id()
            .ok_or(Error::new("failed to create breed").with_cause("invalid inserted id"))
            .map(|id| id.to_string())
    }

    async fn create_dog(&self, dog: &DogCreate) -> Result<Dog, Error> {
        let dog = Document::try_from(dog)?;
        let res = self
            .db
            .collection::<Document>("dogs")
            .insert_one(dog, None)
            .await
            .map_err(|e| Error::new("failed to create dog").with_cause(e))?;
        self.db
            .collection("dogs")
            .find_one(
                doc! {"_id": res.inserted_id},
                FindOneOptions::builder()
                    .projection(Dog::projection())
                    .build(),
            )
            .await
            .map_err(|e| Error::new("failed to get created dog").with_cause(e))?
            .ok_or(Error::new("created dog not exists"))
    }

    async fn delete_breed(&self, id: &str) -> Result<bool, Error> {
        self.db
            .collection::<Breed>("breeds")
            .delete_one(
                doc! {"_id": ObjectId::parse_str(id).map_err(|e| Error::new("failed to delete breed").with_cause(e))?},
                None,
            )
            .await
            .map_err(|e| Error::new("failed to delete breed").with_cause(e))
            .map(|res| res.deleted_count > 0)
    }

    async fn delete_dog(&self, id: &str) -> Result<bool, Error> {
        self.db
            .collection::<Breed>("dogs")
            .delete_one(
                doc! {"_id": ObjectId::parse_str(id).map_err(|e| Error::new("failed to delete dog").with_cause(e))?},
                None,
            )
            .await
            .map_err(|e| Error::new("failed to delete dog").with_cause(e))
            .map(|res| res.deleted_count > 0)
    }

    async fn update_dog(&self, id: &str, dog: &DogUpdate) -> Result<bool, Error> {
        let mut update = doc! {};
        if let Some(name) = &dog.name {
            update.insert("name", name);
        }
        if let Some(gender) = &dog.gender {
            update.insert("gender", gender);
        }
        if let Some(breed) = &dog.breed {
            update.insert("breed", &breed.id);
        }
        if let Some(birthday) = &dog.birthday {
            update.insert("birthday", birthday);
        }
        if let Some(is_sterilized) = &dog.is_sterilized {
            update.insert("is_sterilized", is_sterilized);
        }
        if let Some(introduction) = &dog.introduction {
            update.insert("introduction", introduction);
        }
        if let Some(owner_id) = &dog.owner_id {
            update.insert("owner_id", owner_id);
        }
        if let Some(tags) = &dog.tags {
            update.insert("tags", tags);
        }
        if let Some(portrait_id) = &dog.portrait_id {
            update.insert("portrait_id", portrait_id);
        }
        if !update.is_empty() {
            update.insert("updated_at", Local::now().to_rfc3339());
        }
        Ok(self
            .db
            .collection::<DogUpdate>("dogs")
            .update_one(
                doc! {
                    "_id": ObjectId::parse_str(id).map_err(|e| Error::new("failed to update dog").with_cause(e))?
                },
                doc! { "$set": update},
                None,
            )
            .await
            .map_err(|e| Error::new("failed to update dog").with_cause(e))?
            .modified_count
            > 0)
    }

    async fn query_breeds(&self, query: &BreedQuery) -> Result<(Vec<Breed>, i64), Error> {
        let mut q = doc! {};
        if let Some(category) = &query.category {
            q.insert("category", category.to_string());
        }
        let count = self
            .db
            .collection::<Breed>("breeds")
            .count_documents(q.clone(), None)
            .await
            .map_err(|e| Error::new("failed to query breeds").with_cause(e))?;
        let breeds = self
            .db
            .collection::<Breed>("breeds")
            .find(
                q,
                FindOptions::builder()
                    .projection(doc! {
                        "id": { "$toString": "$_id" },
                        "category": 1,
                        "name": 1,
                        "created_at": 1,
                        "updated_at": 1,
                    })
                    .build(),
            )
            .await
            .map_err(|e| Error::new("failed to query breeds").with_cause(e))?
            .try_collect::<Vec<Breed>>()
            .await
            .map_err(|e| Error::new("failed to query breeds").with_cause(e))?;
        Ok((breeds, count as i64))
    }

    async fn query_dogs(&self, query: &DogQuery) -> Result<Vec<Dog>, Error> {
        let mut q = doc! {};
        if let Some(owner_id) = &query.owner_id {
            q.insert("owner_id", owner_id);
        }
        if let Some(id_in) = &query.id_in {
            q.insert(
                "_id",
                doc! { "$in": id_in.deref().iter().map(|id| ObjectId::parse_str(id).map_err(|e| Error::new("failed to query my dogs").with_cause(e))).collect::<Result<Vec<_>, Error>>()? },
            );
        }
        let options = FindOptions::builder()
            .projection(Dog::projection())
            .skip(query.pagination.as_ref().map(|p| p.skip as u64))
            .limit(query.pagination.as_ref().map(|p| p.limit));
        self.db
            .collection::<Dog>("dogs")
            .find(q, options.build())
            .await
            .map_err(|e| Error::new("failed to query my dogs").with_cause(e))?
            .try_collect::<Vec<Dog>>()
            .await
            .map_err(|e| Error::new("failed to query my dogs").with_cause(e))
        // let mut pipeline = vec![
        //     doc! {
        //         "$match": q,
        //     },
        //     doc! {
        //         "$addFields": {
        //             "breed_id": { "$toObjectId": "$breed" }
        //         }
        //     },
        //     doc! {
        //         "$lookup": {
        //             "from": "breeds",
        //             "localField": "breed_id",
        //             "foreignField": "_id",
        //             "as": "breed",
        //             "pipeline": [
        //                 {
        //                     "$project": {
        //                         "id": { "$toString": "$_id" },
        //                         "category": 1,
        //                         "name": 1,
        //                         "created_at": 1,
        //                         "updated_at": 1,
        //                     }

        //                 }
        //             ]

        //         }
        //     },
        //     doc! {
        //         "$project": {
        //             "id": { "$toString": "$_id" },
        //             "name": 1,
        //             "gender": 1,
        //             "breed": { "$arrayElemAt": [ "$breed", 0 ] } ,
        //             "birthday": 1,
        //             "is_sterilized": 1,
        //             "introduction": 1,
        //             "owner_id": 1,
        //             "tags": 1,
        //             "portrait_id": 1,
        //             "created_at": 1,
        //             "updated_at": 1,
        //         }
        //     },
        // ];
        // if let Some(pagination) = &query.pagination {
        //     pipeline.append(&mut vec![
        //         doc! {
        //             "$limit": pagination.limit
        //         },
        //         doc! {
        //             "$skip": pagination.skip
        //         },
        //     ])
        // }
        // let dogs = self
        //     .db
        //     .collection::<Dog>("dogs")
        //     .aggregate(pipeline, None)
        //     .await
        //     .map_err(|e| Error::new("failed to query my dogs").with_cause(e))?
        //     .try_collect::<Vec<Document>>()
        //     .await
        //     .map(|ds| {
        //         ds.into_iter()
        //             .map(|d| from_document::<Dog>(d).map_err(|e| Error::new("failed to convert dog to document").with_cause(e)))
        //     })
        //     .map_err(|e| Error::new("failed to query my dogs").with_cause(e))?
        //     .collect::<Result<Vec<Dog>, Error>>()?;
        // Ok(dogs)
    }

    async fn exists_dog(&self, query: &DogQuery) -> Result<bool, Error> {
        let mut q = doc! {};
        if let Some(id) = &query.id {
            q.insert(
                "_id",
                ObjectId::parse_str(id)
                    .map_err(|e| Error::new("failed to query my dogs").with_cause(e))?,
            );
        }
        if let Some(owner_id) = &query.owner_id {
            q.insert("owner_id", owner_id);
        }
        Ok(self
            .db
            .collection::<Dog>("dogs")
            .count_documents(q.clone(), None)
            .await
            .map_err(|e| Error::new("failed to query my dogs").with_cause(e))?
            > 0)
    }

    async fn create_walk_request(&self, request: WalkRequestCreate) -> Result<String, Error> {
        let inserted = self
            .db
            .collection::<Document>("walk_requests")
            .insert_one(Document::from(request), None)
            .await
            .map_err(|e| Error::new("failed to create walk request").with_cause(e))?;
        Ok(inserted.inserted_id.to_string())
    }

    async fn get_walk_request(&self, id: &str) -> Result<WalkRequest, Error> {
        self.db
            .collection::<WalkRequest>("walk_requests")
            .find_one(
                doc! {"_id": ObjectId::from_str(id).map_err(|e| Error::new("failed to convert object id").with_cause(e))?},
                FindOneOptions::builder()
                    .projection(WalkRequest::projection())
                    .build(),
            )
            .await
            .map_err(|e| Error::new("failed to get walk request").with_cause(e))?
            .ok_or(Error::msg("walk request not found"))
    }

    async fn query_walk_requests(
        &self,
        query: WalkRequestQuery,
        sort_by: Option<SortBy>,
        pagination: Option<Pagination>,
    ) -> Result<Vec<WalkRequest>, Error> {
        if query.nearby.is_some() {
            let mut pipeline = vec![
                Document::try_from(query)?,
                doc! { "$project": WalkRequest::projection() },
            ];
            if let Some(pagination) = pagination {
                pipeline.push(doc! {
                    "$skip": pagination.skip
                });
                pipeline.push(doc! {
                    "$limit": pagination.limit
                });
            }
            if let Some(sort_by) = sort_by {
                pipeline.push(doc! {
                    "$sort": {sort_by.field: if sort_by.order == Order::Asc { 1 } else { - 1} }
                })
            }
            return self
                .db
                .collection::<WalkRequest>("walk_requests")
                .aggregate(pipeline, None)
                .await
                .map_err(|e| Error::new("failed to query walk requests").with_cause(e))?
                .map(|res| match res {
                    Err(e) => Err(Error::new("failed to query walk requests").with_cause(e)),
                    Ok(doc) => from_document::<WalkRequest>(doc)
                        .map_err(|e| Error::new("failed to convert document").with_cause(e)),
                })
                .try_collect::<Vec<WalkRequest>>()
                .await;
        }
        self.db
            .collection::<WalkRequest>("walk_requests")
            .find(
                Document::try_from(query)?,
                FindOptions::builder()
                    .projection(WalkRequest::projection())
                    .limit(pagination.as_ref().map(|p| p.limit))
                    .skip(pagination.as_ref().map(|p| p.limit as u64))
                    .sort(
                        sort_by.map(|s| doc! {s.field: if s.order == Order::Asc { 1 } else { - 1}}),
                    )
                    .build(),
            )
            .await
            .map_err(Error::from_error)?
            .try_collect::<Vec<WalkRequest>>()
            .await
            .map_err(Error::from_error)
    }

    async fn update_walk_request(
        &self,
        id: &str,
        request: WalkRequestUpdate,
    ) -> Result<WalkRequest, Error> {
        self.db
            .collection("walk_requests")
            .find_one_and_update(
                doc! {"_id": ObjectId::from_str(id).map_err(Error::from_error)?},
                Document::from(request),
                FindOneAndUpdateOptions::builder()
                    .return_document(Some(mongodb::options::ReturnDocument::After))
                    .projection(WalkRequest::projection())
                    .build(),
            )
            .await
            .map_err(Error::from_error)?
            .ok_or(Error::msg("代遛请求不存在"))
    }

    async fn update_walk_request_by_query(
        &self,
        query: WalkRequestQuery,
        update: WalkRequestUpdate,
    ) -> Result<WalkRequest, Error> {
        self.db
            .collection("walk_requests")
            .find_one_and_update(
                Document::try_from(query)?,
                Document::from(update),
                FindOneAndUpdateOptions::builder()
                    .return_document(Some(mongodb::options::ReturnDocument::After))
                    .projection(WalkRequest::projection())
                    .build(),
            )
            .await
            .map_err(Error::from_error)?
            .ok_or(Error::msg("代遛请求不存在"))
    }

    async fn update_walk_requests_by_query(
        &self,
        query: WalkRequestQuery,
        update: WalkRequestUpdate,
    ) -> Result<u64, Error> {
        Ok(self
            .db
            .collection::<Document>("walk_requests")
            .update_many(Document::try_from(query)?, Document::from(update), None)
            .await
            .map_err(Error::from_error)?
            .modified_count)
    }

    async fn create_walking_location<'a>(
        &self,
        create: WalkingLocationCreate<'a>,
    ) -> Result<String, Error> {
        self.db
            .collection("walking_locations")
            .insert_one(Document::from(create), None)
            .await
            .map_err(|e| Error::wrap(e, "创建Walking定位失败"))
            .map(|r| r.inserted_id.to_string())
    }
}

// #[cfg(test)]
// mod test {

//     use super::*;
//     use mongodb::Client;

//     #[tokio::test]
//     async fn test_create_breed() {
//         let client = Client::with_uri_str("mongodb://localhost:27017").await.expect("Failed to connect to MongoDB");
//         let db = client.database("test");
//         let repo = MongoDB::new(db);
//         let id = repo.create_breed(BreedCreate { name: "金毛".to_owned() }).await.expect("Failed to create breed");
//         println!("{}", id);
//     }

//     #[tokio::test]
//     async fn delete_breeds() {
//         let client = Client::with_uri_str("mongodb://localhost:27017").await.expect("Failed to connect to MongoDB");
//         let db = client.database("test");
//         let repo = MongoDB::new(db);
//         let id = repo.create_breed(BreedCreate { name: "金毛".to_owned() }).await.expect("Failed to create breed");
//         repo.create_breed(BreedCreate { name: "拉布拉多".to_owned() }).await.expect("Failed to create breed");
//         let deleted = repo.delete_breeds(BreedQuery { id_eq: Some(id) }).await.expect("Failed to delete breeds");
//         assert!(deleted == 1);
//         repo.delete_breeds(BreedQuery { id_eq: None }).await.expect("Failed to delete breeds");
//     }

//     #[tokio::test]
//     async fn query_breeds() {
//         let client = Client::with_uri_str("mongodb://localhost:27017").await.expect("Failed to connect to MongoDB");
//         let db = client.database("test");
//         let repo = MongoDB::new(db);
//         repo.create_breed(BreedCreate { name: "金毛".to_owned() }).await.expect("Failed to create breed");
//         repo.create_breed(BreedCreate { name: "拉布拉多".to_owned() }).await.expect("Failed to create breed");
//         let (breeds, total) = repo
//             .query_breeds(BreedQuery { id_eq: None }, Some(Pagination { page: 1, size: 1 }))
//             .await
//             .expect("Failed to query breeds");
//         println!("breeds: {:?}, total: {}", breeds, total);
//     }
// }

use mongodb::options::FindOneAndUpdateOptions;

use crate::core::entities::WalkRequest;
use crate::core::repository::{Order, Pagination, SortBy, WalkingLocationCreate};
use crate::core::repository::{WalkRequestCreate, WalkRequestQuery, WalkRequestUpdate};
use futures::StreamExt;
use std::str::FromStr;

impl WalkRequest {
    pub fn projection() -> Document {
        doc! {
            "id": {"$toString": "$_id"},
            "dogs": Dog::projection(),
            "should_start_after": {"$dateToString": {"date":"$should_start_after", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "should_start_before": {"$dateToString": {"date":"$should_start_before", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "should_end_after": {"$dateToString": {"date":"$should_end_after", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "should_end_before": {"$dateToString": {"date":"$should_end_before", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "longitude": { "$arrayElemAt": [ "$location.coordinates", 0]},
            "latitude": { "$arrayElemAt": [ "$location.coordinates", 1]},
            "distance": "$distance",
            "canceled_at": {"$dateToString": {"date":"$canceled_at", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "accepted_by": "$accepted_by",
            "accepted_at": {"$dateToString": {"date":"$accepted_at", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "started_at": {"$dateToString": {"date":"$started_at", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "finished_at": {"$dateToString": {"date":"$finished_at", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "status": {
                "$switch": {
                    "branches": [
                        {"case": {"$ne": [{"$ifNull": ["$canceled_at", null]}, null]}, "then": "Canceled" },
                        {"case": {"$ne": [{"$ifNull": ["$accepted_at", null]}, null]}, "then": "Accepted" },
                        {"case": {"$ne": [{"$ifNull": ["$started_at", null]}, null]}, "then": "Started" },
                        {"case": {"$ne": [{"$ifNull": ["$finished_at", null]}, null]}, "then": "Finished" },
                    ],
                    "default": "Waiting"
                }
            },
            "acceptances": "$acceptances",
            "created_at": {"$dateToString": {"date":"$created_at", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
            "updated_at": {"$dateToString": {"date":"$updated_at", "format": "%Y-%m-%dT%H:%M:%S.%LZ"}},
        }
    }
}

impl TryFrom<WalkRequestQuery> for Document {
    type Error = Error;
    fn try_from(value: WalkRequestQuery) -> Result<Self, Self::Error> {
        let mut q = doc! {};
        if let Some(id) = value.id {
            q.insert("_id", ObjectId::from_str(&id).map_err(Error::from_error)?);
        }
        if let Some(ids) = value.dog_ids_includes_any {
            q.insert("dogs.id", doc! {"$elemMatch": {"$in": ids }});
        }
        if let Some(ids) = value.dog_ids_includes_all {
            q.insert("dogs.id", doc! {"$all": ids });
        }
        if let Some(accepted_by) = value.accepted_by {
            q.insert("accepted_by", accepted_by);
        }
        if let Some(accepted_by_neq) = value.accepted_by_neq {
            q.insert("accepted_by", doc! {"$ne": accepted_by_neq });
        }
        if let Some(accepted_by_is_null) = value.accepted_by_is_null {
            if accepted_by_is_null {
                q.insert("accepted_by", doc! {"$eq": null});
            } else {
                q.insert("accepted_by", doc! {"$neq": null});
            }
        }
        if let Some(acceptances_includes_all) = value.acceptances_includes_all {
            q.insert("acceptances", doc! {"$all": acceptances_includes_all });
        }
        if let Some(acceptances_includes_any) = value.acceptances_includes_any {
            q.insert(
                "acceptances",
                doc! {"$elemMatch": {"$in": acceptances_includes_any }},
            );
        }
        if let Some(nearby) = value.nearby {
            if nearby.len() != 3 {
                return Err(Error::new("Invalid nearby query, expect [f64;3]"));
            }
            return Ok(doc! {
                "$geoNear": {
                    "near": { "type": "Point", "coordinates": [nearby[0], nearby[1]] },
                    "distanceField": "distance",
                    "maxDistance": nearby[2],
                    "spherical": true,
                    "query": q,
                    "includeLocs": "location",
                }
            });
        }
        if let Some(created_by) = value.created_by {
            q.insert("created_by", created_by);
        }
        Ok(q)
    }
}

impl From<WalkRequestUpdate> for Document {
    fn from(update: WalkRequestUpdate) -> Self {
        let mut set = doc! {};
        if let Some(dogs) = update.dogs {
            set.insert("dogs", dogs);
        }
        if let Some(accepted_by) = update.accepted_by {
            set.insert("accepted_by", accepted_by);
        }
        if let Some(accepted_at) = update.accepted_at {
            set.insert("accepted_at", accepted_at);
        }
        if let Some(latitude) = update.latitude {
            set.insert("latitude", latitude);
        }
        if let Some(longitude) = update.longitude {
            set.insert("longitude", longitude);
        }
        if let Some(should_start_after) = update.should_start_after {
            set.insert("should_start_after", should_start_after);
        }
        if let Some(should_start_before) = update.should_start_before {
            set.insert("should_start_before", should_start_before);
        }
        if let Some(should_end_before) = update.should_end_before {
            set.insert("should_end_before", should_end_before);
        }
        if let Some(should_end_after) = update.should_end_after {
            set.insert("should_end_after", should_end_after);
        }
        if let Some(add_to_acceptances) = update.add_to_acceptances {
            set.insert("$addToSet", doc! {"acceptances": add_to_acceptances});
        }
        if let Some(started_at) = update.started_at {
            set.insert("started_at", started_at);
        }
        if let Some(finished_at) = update.finished_at {
            set.insert("finished_at", finished_at);
        }
        let mut pull = doc! {};
        if let Some(remove_from_acceptances) = update.remove_from_acceptances {
            pull.insert("acceptances", remove_from_acceptances);
        }
        let mut unset = doc! {};
        if update.unset_accepted_by {
            unset.insert("accepted_by", "");
        }
        if update.unset_accepted_at {
            unset.insert("accepted_at", "");
        }
        doc! {"$set": set, "$unset": unset, "$pull": pull}
    }
}

impl From<WalkRequestCreate> for Document {
    fn from(value: WalkRequestCreate) -> Self {
        doc! {
            "dogs": value.dogs,
            "should_start_after": value.should_start_after,
            "should_start_before": value.should_start_before,
            "should_end_before": value.should_end_before,
            "should_end_after": value.should_end_after,
            "location": { "type": "Point", "coordinates": [value.longitude, value.latitude] },
            "created_by": value.created_by,
            "created_at": Utc::now(),
            "updated_at": Utc::now(),
        }
    }
}

impl<'a> From<WalkingLocationCreate<'a>> for Document {
    fn from(value: WalkingLocationCreate) -> Self {
        doc! {
            "walk_request_id": value.walk_request_id,
            "longitude": value.longitude,
            "latitude": value.latitude,
            "created_at": Utc::now(),
            "updated_at": Utc::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Mongodb {
    db: Database,
}

impl Mongodb {
    pub fn new(db: Database) -> Self {
        Mongodb { db }
    }
}
