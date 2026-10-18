use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::Instant;

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
    pub entries: RwLock<HashMap<String, DbEntry>>,
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
        SharedDb(Arc::new(Db {
            entries: RwLock::new(HashMap::new()),
            pubsub: RwLock::new(HashMap::new()),
        }))
    }

    // Genral key operations

    pub async fn exists(&self, key: &str) -> bool {
        let mut entries = self.0.entries.write().await;
        if let Some(entry) = entries.get(key) {
            if let Some(exp) = entry.expires_at {
                if Instant::now() >= exp {
                    entries.remove(key);
                    return false;
                }
            }
            return true;
        }
        false
    }

    pub async fn del(&self, key: &str) -> usize {
        let mut entries = self.0.entries.write().await;
        remove_if_expired(&mut entries, key);
        entries.remove(key).map(|_| 1).unwrap_or(0)
    }

    pub async fn expire(&self, key: &str, seconds: u64) -> usize {
        let mut entries = self.0.entries.write().await;
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
        let mut entries = self.0.entries.write().await;
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

    pub async fn scan(&self) -> Vec<String> {
        let mut entries = self.0.entries.write().await;
        let now = Instant::now();
        let mut keys = Vec::new();
        let mut expired = Vec::new();

        for (k, entry) in entries.iter() {
            if let Some(exp) = entry.expires_at {
                if now >= exp {
                    expired.push(k.clone());
                    continue;
                }
            }
            keys.push(k.clone());
        }
        for k in expired {
            entries.remove(&k);
        }
        keys
    }

    pub async fn flushall(&self) {
        let mut entries = self.0.entries.write().await;
        entries.clear();
    }

    // Strings and numeric operations

    pub async fn set(&self, key: String, value: Bytes) {
        let mut entries = self.0.entries.write().await;
        entries.insert(
            key,
            DbEntry {
                value: Value::String(value),
                expires_at: None,
            },
        );
    }

    pub async fn get(&self, key: &str) -> Option<Bytes> {
        let mut entries = self.0.entries.write().await;
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
        let mut entries = self.0.entries.write().await;
        if entries.contains_key(&key) {
            if let Some(exp) = entries.get(&key).unwrap().expires_at {
                if Instant::now() >= exp {
                    entries.insert(
                        key,
                        DbEntry {
                            value: Value::String(value),
                            expires_at: None,
                        },
                    );
                    return 1;
                }
            }
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
        let mut entries = self.0.entries.write().await;
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
        let mut entries = self.0.entries.write().await;
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
        let mut entries = self.0.entries.write().await;
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
        let entries = self.0.entries.read().await;
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
        let entries = self.0.entries.read().await;
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
        let mut entries = self.0.entries.write().await;
        remove_if_expired(&mut entries, key);
        if let Some(entry) = entries.get_mut(key) {
            if let Value::Hash(ref mut map) = entry.value {
                return map.remove(field).map(|_| 1).unwrap_or(0);
            }
        }
        0
    }

    pub async fn hkeys(&self, key: &str) -> Option<Vec<String>> {
        let entries = self.0.entries.read().await;
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
        let mut entries = self.0.entries.write().await;
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

    pub async fn pop_list(&self, key: &str, front: bool) -> Option<Bytes> {
        let mut entries = self.0.entries.write().await;
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
        let entries = self.0.entries.read().await;
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
        let entries = self.0.entries.read().await;
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
        let mut entries = self.0.entries.write().await;
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
        let mut entries = self.0.entries.write().await;
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
        let entries = self.0.entries.read().await;
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
        let entries = self.0.entries.read().await;
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
        let mut entries = self.0.entries.write().await;
        remove_if_expired(&mut entries, key);
        if let Some(entry) = entries.get_mut(key) {
            if let Value::Set(ref mut set) = entry.value {
                return if set.remove(member) { 1 } else { 0 };
            }
        }
        0
    }

    pub async fn sinter(&self, key1: &str, key2: &str) -> Vec<Bytes> {
        let entries = self.0.entries.read().await;
        let set1 = match entries.get(key1) {
            Some(entry) if !is_expired(entry) => match &entry.value {
                Value::Set(set) => set,
                _ => return vec![],
            },
            _ => return vec![],
        };
        let set2 = match entries.get(key2) {
            Some(entry) if !is_expired(entry) => match &entry.value {
                Value::Set(set) => set,
                _ => return vec![],
            },
            _ => return vec![],
        };
        set1.intersection(set2).cloned().collect()
    }

    // Sorted sets engine

    pub async fn zadd(&self, key: String, score: i64, value: Bytes) -> usize {
        let mut entries = self.0.entries.write().await;
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
        let entries = self.0.entries.read().await;
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
        let mut entries = self.0.entries.write().await;
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
        let mut entries = self.0.entries.write().await;

        let now = Instant::now();
        let previous_count = entries.len();

        entries.retain(|_, entry| match entry.expires_at {
            Some(expiration) => now < expiration,
            None => true,
        });

        previous_count - entries.len()
    }
}
