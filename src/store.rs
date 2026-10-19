use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::Instant;

const SHARED_COUNT: usize = 64;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ZSetMember {
    pub score: i64,
    pub value: Bytes,
}

impl Ord for ZSetMember {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.score.cmp(&other.score) {
            Ordering::Equal => self.value.cmp(&other.value),
            other_ord => other_ord,
        }
    }
}

impl PartialOrd for ZSetMember {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    String(Bytes),
    Hash(HashMap<String, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet(BTreeSet<ZSetMember>),
}

#[derive(Debug, Clone)]
pub struct DbEntry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

#[derive(Debug)]
pub struct Db {
    pub shards: Vec<RwLock<HashMap<String, DbEntry>>>,
    pub operation_gate: RwLock<()>,
    pub pubsub: RwLock<HashMap<String, broadcast::Sender<Bytes>>>,
}

#[derive(Debug, Clone)]
pub struct SharedDb(pub Arc<Db>);

fn is_expired(entry: &DbEntry) -> bool {
    match entry.expires_at {
        Some(expiration) => Instant::now() >= expiration,
        None => false,
    }
}

fn remove_if_expired(entries: &mut HashMap<String, DbEntry>, key: &str) {
    if entries.get(key).is_some_and(is_expired) {
        entries.remove(key);
    }
}

impl SharedDb {
    pub fn new() -> Self {
        let shards = (0..SHARED_COUNT)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self(Arc::new(Db {
            shards,
            operation_gate: RwLock::new(()),
            pubsub: RwLock::new(HashMap::new()),
        }))
    }

    fn shard_index(key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);

        (hasher.finish() as usize) % SHARED_COUNT
    }

    fn shard(&self, key: &str) -> &RwLock<HashMap<String, DbEntry>> {
        &self.0.shards[Self::shard_index(key)]
    }

    // Genral key operations

    pub async fn exists(&self, keys: &[String]) -> usize {
        let _gate = self.0.operation_gate.write().await;
        let mut count = 0;

        for key in keys {
            let mut entries = self.shard(key).write().await;
            remove_if_expired(&mut entries, key);

            if entries.contains_key(key) {
                count += 1;
            }
        }

        count
    }

    pub async fn del(&self, keys: &[String]) -> usize {
        let _gate = self.0.operation_gate.write().await;
        let mut deleted = 0;

        for key in keys {
            let mut entries = self.shard(&key).write().await;
            remove_if_expired(&mut entries, key);

            if entries.remove(key).is_some() {
                deleted += 1;
            }
        }

        deleted
    }

    pub async fn expire(&self, key: &str, seconds: u64) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        if let Some(entry) = entries.get_mut(key) {
            if let Some(exp) = entry.expires_at {
                if Instant::now() >= exp {
                    entries.remove(key);
                    return 0;
                }
            }
            entry.expires_at = Some(Instant::now() + std::time::Duration::from_secs(seconds));
            return 1;
        }
        0
    }

    pub async fn ttl(&self, key: &str) -> i64 {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        if let Some(entry) = entries.get(key) {
            if let Some(exp) = entry.expires_at {
                let now = Instant::now();
                if now >= exp {
                    entries.remove(key);
                    return -2;
                }
                return (exp - now).as_secs() as i64;
            }
            return -1;
        }
        -2
    }

    pub async fn pttl(&self, key: &str) -> i64 {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;

        match entries.get(key).map(|entry| entry.expires_at) {
            None => -2,

            Some(None) => -1,

            Some(Some(expiration)) => {
                let now = Instant::now();

                if now >= expiration {
                    entries.remove(key);
                    -2
                } else {
                    (expiration - now).as_millis() as i64
                }
            }
        }
    }

    pub async fn scan(&self) -> Vec<String> {
        let _gate = self.0.operation_gate.write().await;
        let now = Instant::now();
        let mut keys = Vec::new();

        for shard in &self.0.shards {
            let mut entries = shard.write().await;
            entries.retain(|key, entry| {
                if let Some(expiration) = entry.expires_at {
                    if now >= expiration {
                        return false;
                    }
                }
                keys.push(key.clone());
                true
            });
        }
        keys
    }

    pub async fn flushall(&self) {
        let _gate = self.0.operation_gate.write().await;

        for shard in &self.0.shards {
            let mut entries = shard.write().await;
            entries.clear();
        }
    }

    // Strings and numeric operations

    pub async fn set(&self, key: String, value: Bytes) {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        entries.insert(
            key,
            DbEntry {
                value: Value::String(value),
                expires_at: None,
            },
        );
    }

    pub async fn set_with_options(
        &self,
        key: String,
        value: Bytes,
        expiry: Option<std::time::Duration>,
        nx: bool,
        xx: bool,
    ) -> bool {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;

        remove_if_expired(&mut entries, &key);

        let exists = entries.contains_key(&key);

        if nx && exists {
            return false;
        }

        if xx && !exists {
            return false;
        }

        let expires_at = expiry.map(|duration| Instant::now() + duration);

        entries.insert(
            key,
            DbEntry {
                value: Value::String(value),
                expires_at,
            },
        );
        true
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        if let Some(entry) = entries.get(key) {
            if let Some(exp) = entry.expires_at {
                if Instant::now() >= exp {
                    entries.remove(key);
                    return None;
                }
            }
            if let Value::String(ref val) = &entry.value {
                return Some(val.clone());
            }
        }
        None
    }

    pub async fn setnx(&self, key: String, value: Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        if entries.contains_key(&key) {
            return 0;
        }
        entries.insert(
            key,
            DbEntry {
                value: Value::String(value),
                expires_at: None,
            },
        );
        1
    }

    pub async fn incrby(&self, key: String, amount: i64) -> Result<i64, &'static str> {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::String(Bytes::from("0")),
            expires_at: None,
        });

        if let Value::String(ref mut b) = entry.value {
            let s = std::str::from_utf8(b)
                .map_err(|_| "ERR value is not an integer or out of range")?;
            let mut val: i64 = s
                .trim()
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range")?;
            val += amount;
            *b = Bytes::from(val.to_string());
            return Ok(val);
        }
        Err("WRONGTYPE Operation against a key holding the wrong kind of value")
    }

    // Hashes Engine

    pub async fn hset(&self, key: String, field: String, value: Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::Hash(HashMap::new()),
            expires_at: None,
        });

        if let Value::Hash(ref mut map) = entry.value {
            return if map.insert(field, value).is_none() {
                1
            } else {
                0
            };
        }
        0
    }

    pub async fn hset_many(&self, key: String, fields: Vec<(String, Bytes)>) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::Hash(HashMap::new()),
            expires_at: None,
        });

        if let Value::Hash(ref mut map) = entry.value {
            let mut added = 0;

            for (field, value) in fields {
                if map.insert(field, value).is_none() {
                    added += 1;
                }
            }

            added
        } else {
            0
        }
    }

    pub async fn hget(&self, key: &str, field: &str) -> Option<Bytes> {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return None;
            }
            if let Value::Hash(ref map) = entry.value {
                return map.get(field).cloned();
            }
        }
        None
    }

    pub async fn hgetall(&self, key: &str) -> Option<Vec<(String, Bytes)>> {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return None;
            }
            if let Value::Hash(ref map) = entry.value {
                return Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            }
        }
        None
    }

    pub async fn hdel(&self, key: &str, field: &str) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);
        if let Some(entry) = entries.get_mut(key) {
            if let Value::Hash(ref mut map) = entry.value {
                return map.remove(field).map(|_| 1).unwrap_or(0);
            }
        }
        0
    }

    pub async fn hkeys(&self, key: &str) -> Option<Vec<String>> {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return None;
            }
            if let Value::Hash(ref map) = entry.value {
                return Some(map.keys().cloned().collect());
            }
        }
        None
    }

    // List engine
    pub async fn push_list(&self, key: String, value: Bytes, front: bool) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });

        if let Value::List(ref mut deque) = entry.value {
            if front {
                deque.push_front(value);
            } else {
                deque.push_back(value);
            }
            return deque.len();
        }
        0
    }

    pub async fn push_list_many(
        &self,
        key: String,
        values: Vec<Bytes>,
        front: bool,
    ) -> Result<usize, &'static str> {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;

        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });

        if let Value::List(ref mut deque) = entry.value {
            for value in values {
                if front {
                    deque.push_front(value);
                } else {
                    deque.push_back(value);
                }
            }

            Ok(deque.len())
        } else {
            Err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
    }

    pub async fn pop_list(&self, key: &str, front: bool) -> Option<Bytes> {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);
        if let Some(entry) = entries.get_mut(key) {
            if let Value::List(ref mut deque) = entry.value {
                return if front {
                    deque.pop_front()
                } else {
                    deque.pop_back()
                };
            }
        }
        None
    }

    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> Option<Vec<Bytes>> {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return None;
            }
            if let Value::List(ref deque) = entry.value {
                let len = deque.len() as i64;
                if len == 0 {
                    return Some(vec![]);
                }
                let mut start_idx = if start < 0 { len + start } else { start };
                let mut stop_idx = if stop < 0 { len + stop } else { stop };

                if start_idx < 0 {
                    start_idx = 0;
                }
                if stop_idx >= len {
                    stop_idx = len - 1;
                }
                if start_idx > stop_idx {
                    return Some(vec![]);
                };

                return Some(
                    deque
                        .iter()
                        .skip(start_idx as usize)
                        .take((stop_idx - start_idx + 1) as usize)
                        .cloned()
                        .collect(),
                );
            }
        }
        None
    }

    pub async fn llen(&self, key: &str) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return 0;
            }
            if let Value::List(ref deque) = entry.value {
                return deque.len();
            }
        }
        0
    }

    // Sets engine

    pub async fn sadd(&self, key: String, member: Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::Set(HashSet::new()),
            expires_at: None,
        });
        if let Value::Set(ref mut set) = entry.value {
            return if set.insert(member) { 1 } else { 0 };
        }
        0
    }

    pub async fn sadd_many(&self, key: String, members: Vec<Bytes>) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::Set(HashSet::new()),
            expires_at: None,
        });

        if let Value::Set(ref mut set) = entry.value {
            let mut added = 0;

            for member in members {
                if set.insert(member) {
                    added += 1;
                }
            }

            added
        } else {
            0
        }
    }

    pub async fn smembers(&self, key: &str) -> Option<Vec<Bytes>> {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return None;
            }
            if let Value::Set(ref set) = entry.value {
                return Some(set.iter().cloned().collect());
            }
        }
        None
    }

    pub async fn sismember(&self, key: &str, member: &Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return 0;
            }
            if let Value::Set(ref set) = entry.value {
                return if set.contains(member) { 1 } else { 0 };
            }
        }
        0
    }

    pub async fn srem(&self, key: &str, member: &Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);
        if let Some(entry) = entries.get_mut(key) {
            if let Value::Set(ref mut set) = entry.value {
                return if set.remove(member) { 1 } else { 0 };
            }
        }
        0
    }

    pub async fn sinter(&self, key1: &str, key2: &str) -> Vec<Bytes> {
        let _gate = self.0.operation_gate.write().await;

        let set1 = {
            let entries = self.shard(key1).read().await;

            match entries.get(key1) {
                Some(entry) if !is_expired(entry) => match &entry.value {
                    Value::Set(set) => set.clone(),
                    _ => return vec![],
                },
                _ => return vec![],
            }
        };

        let set2 = {
            let entries = self.shard(key2).read().await;

            match entries.get(key2) {
                Some(entry) if !is_expired(entry) => match &entry.value {
                    Value::Set(set) => set.clone(),
                    _ => return vec![],
                },
                _ => return vec![],
            }
        };

        set1.intersection(&set2).cloned().collect()
    }

    // Sorted sets engine

    pub async fn zadd(&self, key: String, score: i64, value: Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);
        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::SortedSet(BTreeSet::new()),
            expires_at: None,
        });
        if let Value::SortedSet(ref mut zset) = entry.value {
            zset.retain(|m| m.value != value);
            return if zset.insert(ZSetMember { score, value }) {
                1
            } else {
                0
            };
        }
        0
    }

    pub async fn zrange(
        &self,
        key: &str,
        start: i64,
        end: i64,
        reverse: bool,
    ) -> Option<Vec<ZSetMember>> {
        let _gate = self.0.operation_gate.read().await;
        let entries = self.shard(key).read().await;
        if let Some(entry) = entries.get(key) {
            if is_expired(entry) {
                return None;
            }
            if let Value::SortedSet(ref zset) = entry.value {
                let len = zset.len() as i64;
                if len == 0 {
                    return Some(vec![]);
                }

                let mut start_index = if start < 0 { len + start } else { start };
                let mut stop_index = if end < 0 { len + end } else { end };

                if start_index < 0 {
                    start_index = 0;
                }
                if stop_index >= len {
                    stop_index = len - 1;
                }
                if start_index > stop_index {
                    return Some(vec![]);
                };

                let mut members: Vec<ZSetMember> = zset.iter().cloned().collect();
                if reverse {
                    members.reverse();
                }
                return Some(
                    members
                        .into_iter()
                        .skip(start_index as usize)
                        .take((stop_index - start_index + 1) as usize)
                        .collect(),
                );
            }
        }
        None
    }

    pub async fn zrem(&self, key: &str, value: &Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);
        if let Some(entry) = entries.get_mut(key) {
            if let Value::SortedSet(ref mut zset) = entry.value {
                let before = zset.len();
                zset.retain(|m| m.value != value);
                return before - zset.len();
            }
        }
        0
    }

    // Pub-Sub engine
    pub async fn publish(&self, channel: &str, message: Bytes) -> usize {
        let pubsub = self.0.pubsub.read().await;
        if let Some(sender) = pubsub.get(channel) {
            sender.send(message).unwrap_or(0)
        } else {
            0
        }
    }

    pub async fn subscribe(&self, channel: String) -> broadcast::Receiver<Bytes> {
        let mut pubsub = self.0.pubsub.write().await;
        let sender = pubsub.entry(channel).or_insert_with(|| {
            let (tx, _) = broadcast::channel(1024);
            tx
        });
        sender.subscribe()
    }

    pub async fn remove_expired(&self) -> usize {
        let _gate = self.0.operation_gate.write().await;
        let now = Instant::now();
        let mut removed = 0;

        for shard in &self.0.shards {
            let mut entries = shard.write().await;
            let previous_count = entries.len();

            entries.retain(|_, entry| match entry.expires_at {
                Some(expiration) => now < expiration,
                None => true,
            });
            removed += previous_count - entries.len();
        }
        removed
    }

    pub async fn pexpire(&self, key: &str, milliseconds: u64) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;

        remove_if_expired(&mut entries, key);

        if let Some(entry) = entries.get_mut(key) {
            entry.expires_at =
                Some(Instant::now() + std::time::Duration::from_millis(milliseconds));

            return 1;
        }

        0
    }

    pub async fn persist(&self, key: &str) -> usize {
        let _gate = self.0.operation_gate.read().await;
        let mut entries = self.shard(key).write().await;

        remove_if_expired(&mut entries, key);

        if let Some(entry) = entries.get_mut(key) {
            if entry.expires_at.is_some() {
                entry.expires_at = None;
                return 1;
            }
        }

        0
    }
}
