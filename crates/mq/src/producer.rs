use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerMeta {
    pub producer_id: u64,
    #[serde(default)]
    pub name: Option<String>,
    pub connected_at: u64,
    pub last_active_at: u64,
}

impl ProducerMeta {
    pub fn new(producer_id: u64, name: Option<String>, connected_at: u64) -> Self {
        Self {
            producer_id,
            name,
            connected_at,
            last_active_at: connected_at,
        }
    }
}
