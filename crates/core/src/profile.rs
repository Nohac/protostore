use crate::{
    hash::{ChunkId, Hash32, ProfileId, TreeId},
    store::BlobStore,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessEvent {
    pub tree_id: TreeId,
    pub chunk_id: ChunkId,
    pub first_read_unix_ns: u128,
    pub read_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccessProfile {
    pub version: u32,
    pub events: Vec<AccessEvent>,
}

#[derive(Clone, Default)]
pub struct ProfileRecorder {
    inner: Arc<Mutex<HashMap<(TreeId, ChunkId), AccessEvent>>>,
}

impl ProfileRecorder {
    pub fn record(&self, tree_id: TreeId, chunk_id: ChunkId) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let mut inner = self.inner.lock().expect("profile recorder poisoned");
        inner
            .entry((tree_id, chunk_id))
            .and_modify(|event| event.read_count = event.read_count.saturating_add(1))
            .or_insert(AccessEvent {
                tree_id,
                chunk_id,
                first_read_unix_ns: now,
                read_count: 1,
            });
    }

    pub fn profile(&self) -> AccessProfile {
        let mut events: Vec<_> = self
            .inner
            .lock()
            .expect("profile recorder poisoned")
            .values()
            .cloned()
            .collect();
        events.sort_by_key(|event| (event.first_read_unix_ns, event.chunk_id));
        AccessProfile { version: 1, events }
    }

    pub async fn write<S: BlobStore>(&self, store: &S) -> Result<ProfileId> {
        write_profile(store, &self.profile()).await
    }
}

pub async fn write_profile<S: BlobStore>(store: &S, profile: &AccessProfile) -> Result<ProfileId> {
    let json = serde_json::to_vec(profile).context("serializing access profile")?;
    let profile_id = ProfileId::new(Hash32::digest(&json));
    store
        .put_bytes(&profile_key(profile_id), bytes::Bytes::from(json))
        .await?;
    Ok(profile_id)
}

pub async fn load_profile<S: BlobStore>(store: &S, profile_id: ProfileId) -> Result<AccessProfile> {
    store.get_json(&profile_key(profile_id)).await
}

pub fn profile_key(profile_id: ProfileId) -> String {
    format!("profiles/{profile_id}.profile")
}
