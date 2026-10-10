#[derive(Clone, Debug, sqlx::FromRow)]
pub struct StatusRow {
    pub signature: String,
    pub slot: i64,
    pub source_status: String,
    pub dex: Option<String>,
    pub pool: Option<String>,
    pub execution_target: Option<String>,
    pub copy_status: Option<String>,
    pub local_signature: Option<String>,
    pub landed_slot: Option<i64>,
    pub landed_route: Option<String>,
    pub error: Option<String>,
    pub timings_json: Option<String>,
}

impl StatusRow {
    pub fn slot_delta(&self) -> Option<i64> {
        if self.execution_target.as_deref() != Some("mainnet") {
            return None;
        }
        self.landed_slot
            .and_then(|slot| slot.checked_sub(self.slot))
    }
}
