use super::*;

struct TTLSet {
    cache: moka::sync::Cache<String, ()>,
}
impl TTLSet {
    pub fn new(ttl: Duration) -> Self {
        let builder = moka::sync::Cache::builder()
            .initial_capacity(1000)
            .time_to_live(ttl);
        Self {
            cache: builder.build(),
        }
    }

    pub fn exists(&self, k: &str) -> bool {
        self.cache.contains_key(k)
    }

    pub fn insert(&self, k: String) {
        self.cache.insert(k, ());
    }
}

pub struct ResponseCache {
    responses: spin::Mutex<HashMap<String, Bytes>>,
    completes: TTLSet,
}
impl ResponseCache {
    pub fn new() -> Self {
        Self {
            responses: spin::Mutex::new(HashMap::new()),
            // Client may retry the request even after the response
            // but with a sane client this could happen in quite a short time.
            // Completed request_id is kept for 5 seconds to prevent executing
            // the same request twice.
            completes: TTLSet::new(Duration::from_secs(5)),
        }
    }

    pub fn should_execute(&self, k: &str) -> bool {
        if self.completes.exists(k) {
            return false;
        }
        match self.responses.lock().get(k) {
            Some(_) => false,
            None => true,
        }
    }

    pub fn insert_response(&self, k: String, v: Bytes) {
        self.responses.lock().insert(k, v);
    }

    pub fn get_response(&self, k: &str) -> Option<Bytes> {
        self.responses.lock().get(k).cloned()
    }

    pub fn complete_response(&self, k: &str) {
        self.responses.lock().remove(k);
        self.completes.insert(k.to_owned());
    }
}
