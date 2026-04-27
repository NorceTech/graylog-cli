#[cfg(test)]
pub(crate) mod fakes {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::application::ports::cache_store::{CacheError, CacheStore};

    #[derive(Clone, Default)]
    pub(crate) struct FakeCacheStore {
        storage: Arc<Mutex<HashMap<String, String>>>,
    }

    impl FakeCacheStore {
        pub(crate) fn get(&self, key: &str) -> Option<String> {
            self.storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .get(key)
                .cloned()
        }

        pub(crate) fn insert(&self, key: &str, value: String) {
            self.storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .insert(key.to_string(), value);
        }
    }

    #[async_trait]
    impl CacheStore for FakeCacheStore {
        async fn get_serialized(&self, key: &str) -> exn::Result<Option<String>, CacheError> {
            Ok(self
                .storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .get(key)
                .cloned())
        }

        async fn save_serialized(&self, key: String, data: String) -> exn::Result<(), CacheError> {
            self.storage
                .lock()
                .expect("cache mutex should not be poisoned")
                .insert(key, data);
            Ok(())
        }
    }
}
