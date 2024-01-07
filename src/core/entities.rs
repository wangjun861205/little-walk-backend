use chrono::{DateTime, Utc};
use nb_field_names::FieldNames;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Category {
    Small,
    Medium,
    Large,
    Giant,
}

impl Display for Category {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Category::Small => "Small",
                Category::Medium => "Medium",
                Category::Large => "Large",
                Category::Giant => "Giant",
            }
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Breed {
    pub id: String,
    pub category: Category,
    pub name: String,
}

// 性别
#[derive(Debug, Serialize, Deserialize)]
pub enum Gender {
    Other,
    Male,
    Female,
}

impl Default for Gender {
    fn default() -> Self {
        Self::Other
    }
}

// 狗狗
#[derive(Debug, Serialize, Deserialize)]
pub struct Dog {
    pub id: String,
    pub name: String,
    pub gender: Gender,
    pub breed: Breed,            // 品种
    pub birthday: DateTime<Utc>, // 生日
    // pub is_sterilized: bool,     // 是否绝育
    // pub introduction: String,
    pub owner_id: String,
    pub tags: Vec<String>,
    pub portrait_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, FieldNames, Default)]
pub struct WalkRequest {
    pub id: String,
    pub dogs: Vec<Dog>,
    pub should_start_after: Option<DateTime<Utc>>,
    pub should_start_before: Option<DateTime<Utc>>,
    pub should_end_after: Option<DateTime<Utc>>,
    pub should_end_before: Option<DateTime<Utc>>,
    pub latitude: f64,
    pub longitude: f64,
    pub distance: Option<f64>,
    pub canceled_at: Option<DateTime<Utc>>,
    pub accepted_by: Option<String>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: String,
    pub acceptances: Option<Vec<String>>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Serialize, FieldNames, Default)]
pub struct WalkingLocation {
    pub id: String,
    pub request_id: String,
    pub longitude: f64,
    pub latitude: f64,
}
