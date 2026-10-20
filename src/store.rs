use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio::time::Instant;

const SHARED_COUNT: usize = 64;

#[derive(Debug, Clone)]
pub struct ZSetMember {
    pub score: f64,
    pub value: Bytes,
}

type RankLink = Option<Box<RankNode>>;

#[derive(Debug, Clone)]
struct RankNode {
    member: ZSetMember,
    height: i32,
    size: usize,
    left: RankLink,
    right: RankLink,
}

impl RankNode {
    fn new(member: ZSetMember) -> Box<Self> {
        Box::new(Self {
            member,
            height: 1,
            size: 1,
            left: None,
            right: None,
        })
    }
}

fn height(node: &RankLink) -> i32 {
    node.as_ref().map_or(0, |node| node.height)
}

fn subtree_size(node: &RankLink) -> usize {
    node.as_ref().map_or(0, |node| node.size)
}

fn update_node(node: &mut Box<RankNode>) {
    node.height = 1 + height(&node.left).max(height(&node.right));

    node.size = 1 + subtree_size(&node.left) + subtree_size(&node.right);
}

fn rotate_right(mut root: Box<RankNode>) -> Box<RankNode> {
    let mut new_root = root
        .left
        .take()
        .expect("right rotation requires a left child");

    root.left = new_root.right.take();

    update_node(&mut root);

    new_root.right = Some(root);

    update_node(&mut new_root);

    new_root
}

fn rotate_left(mut root: Box<RankNode>) -> Box<RankNode> {
    let mut new_root = root
        .right
        .take()
        .expect("left rotation requires a right child");

    root.right = new_root.left.take();

    update_node(&mut root);

    new_root.left = Some(root);

    update_node(&mut new_root);

    new_root
}

fn balance_node(mut node: Box<RankNode>) -> Box<RankNode> {
    update_node(&mut node);

    let balance = height(&node.left) - height(&node.right);

    // Left-heavy subtree.
    if balance > 1 {
        let left = node.left.as_ref().unwrap();

        // Left-right case.
        if height(&left.left) < height(&left.right) {
            node.left = node.left.take().map(rotate_left);
        }

        return rotate_right(node);
    }

    // Right-heavy subtree.
    if balance < -1 {
        let right = node.right.as_ref().unwrap();

        // Right-left case.
        if height(&right.right) < height(&right.left) {
            node.right = node.right.take().map(rotate_right);
        }

        return rotate_left(node);
    }

    node
}

fn insert_node(root: RankLink, member: ZSetMember) -> RankLink {
    match root {
        None => Some(RankNode::new(member)),

        Some(mut node) => {
            match member.cmp(&node.member) {
                std::cmp::Ordering::Less => {
                    node.left = insert_node(node.left.take(), member);
                }

                std::cmp::Ordering::Greater => {
                    node.right = insert_node(node.right.take(), member);
                }

                std::cmp::Ordering::Equal => {
                    node.member = member;
                    return Some(node);
                }
            }

            Some(balance_node(node))
        }
    }
}

fn take_min(mut node: Box<RankNode>) -> (RankLink, ZSetMember) {
    match node.left.take() {
        None => {
            let right = node.right.take();

            (right, node.member)
        }

        Some(left) => {
            let (new_left, minimum) = take_min(left);

            node.left = new_left;

            (Some(balance_node(node)), minimum)
        }
    }
}

fn remove_node(root: RankLink, key: &ZSetMember) -> RankLink {
    let mut node = root?;

    match key.cmp(&node.member) {
        std::cmp::Ordering::Less => {
            node.left = remove_node(node.left.take(), key);
        }

        std::cmp::Ordering::Greater => {
            node.right = remove_node(node.right.take(), key);
        }

        std::cmp::Ordering::Equal => {
            if node.left.is_none() {
                return node.right;
            }

            if node.right.is_none() {
                return node.left;
            }

            let right = node.right.take().unwrap();

            let (new_right, successor) = take_min(right);

            node.member = successor;
            node.right = new_right;
        }
    }

    Some(balance_node(node))
}

fn select_by_rank(root: &RankLink, rank: usize) -> Option<&ZSetMember> {
    let node = root.as_ref()?;

    let left_size = subtree_size(&node.left);

    if rank < left_size {
        select_by_rank(&node.left, rank)
    } else if rank == left_size {
        Some(&node.member)
    } else {
        select_by_rank(&node.right, rank - left_size - 1)
    }
}

#[derive(Debug, Clone)]
pub struct SortedSet {
    root: RankLink,
    pub scores: HashMap<Bytes, f64>,
}

impl SortedSet {
    pub fn new() -> Self {
        Self {
            root: None,
            scores: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        subtree_size(&self.root)
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    pub fn insert_member(&mut self, member: ZSetMember) {
        self.root = insert_node(self.root.take(), member);
    }

    pub fn remove_member(&mut self, member: &ZSetMember) {
        self.root = remove_node(self.root.take(), member);
    }

    pub fn member_at_rank(&self, rank: usize) -> Option<&ZSetMember> {
        select_by_rank(&self.root, rank)
    }
}

impl PartialEq for ZSetMember {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal && self.value == other.value
    }
}

impl Eq for ZSetMember {}

impl Ord for ZSetMember {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.value.cmp(&other.value))
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
    SortedSet(SortedSet),
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
    pub pattern_pubsub: RwLock<HashMap<String, broadcast::Sender<(String, Bytes)>>>,
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
            pattern_pubsub: RwLock::new(HashMap::new()),
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

    pub async fn zadd(&self, key: String, score: f64, value: Bytes) -> usize {
        self.zadd_many(key, vec![(score, value)]).await
    }

    pub async fn zadd_many(&self, key: String, members: Vec<(f64, Bytes)>) -> usize {
        let _gate = self.0.operation_gate.read().await;

        let mut entries = self.shard(&key).write().await;

        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert(DbEntry {
            value: Value::SortedSet(SortedSet::new()),
            expires_at: None,
        });

        let Value::SortedSet(zset) = &mut entry.value else {
            return 0;
        };

        let mut added = 0;

        for (score, value) in members {
            // NaN is rejected by the command parser.
            // Normalize negative zero for consistent ordering.
            let score = if score == 0.0 { 0.0 } else { score };

            let previous_score = zset.scores.insert(value.clone(), score);

            if let Some(previous_score) = previous_score {
                // Remove the old score-index entry.
                zset.remove_member(&ZSetMember {
                    score: previous_score,
                    value: value.clone(),
                });
            } else {
                // Only a previously absent member increases the count.
                added += 1;
            }

            // Insert the member using its updated score.
            zset.insert_member(ZSetMember { score, value });
        }

        added
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

        let entry = entries.get(key)?;

        if is_expired(entry) {
            return None;
        }

        let Value::SortedSet(zset) = &entry.value else {
            return None;
        };

        let len = zset.len() as i64;

        if len == 0 {
            return Some(Vec::new());
        }

        // Normalize negative ranks.
        // A negative start below the valid range is clamped to zero.
        let start_index = if start < 0 {
            len.saturating_add(start).max(0)
        } else {
            start
        };

        let end_index = if end < 0 {
            len.saturating_add(end)
        } else {
            end
        };

        if start_index >= len || end_index < 0 || start_index > end_index {
            return Some(Vec::new());
        }

        let end_index = end_index.min(len - 1);

        let mut result = Vec::with_capacity((end_index - start_index + 1) as usize);

        for rank in start_index..=end_index {
            let actual_rank = if reverse {
                (len - 1 - rank) as usize
            } else {
                rank as usize
            };

            if let Some(member) = zset.member_at_rank(actual_rank) {
                result.push(member.clone());
            }
        }

        Some(result)
    }

    pub async fn zrem(&self, key: &str, value: &Bytes) -> usize {
        let _gate = self.0.operation_gate.read().await;

        let mut entries = self.shard(key).write().await;

        remove_if_expired(&mut entries, key);

        if let Some(entry) = entries.get_mut(key) {
            if let Value::SortedSet(zset) = &mut entry.value {
                if let Some(score) = zset.scores.remove(value) {
                    zset.remove_member(&ZSetMember {
                        score,
                        value: value.clone(),
                    });

                    return 1;
                }
            }
        }

        0
    }
    // Pub-Sub engine

    pub async fn publish(&self, channel: &str, message: Bytes) -> usize {
        let mut delivered = 0;

        // Direct channel subscribers.
        {
            let pubsub = self.0.pubsub.read().await;

            if let Some(sender) = pubsub.get(channel) {
                delivered += sender.send(message.clone()).unwrap_or(0);
            }
        }

        // Pattern subscribers.
        {
            let patterns = self.0.pattern_pubsub.read().await;

            for (pattern, sender) in patterns.iter() {
                if glob_matches(pattern.as_bytes(), channel.as_bytes()) {
                    delivered += sender
                        .send((channel.to_string(), message.clone()))
                        .unwrap_or(0);
                }
            }
        }

        delivered
    }

    pub async fn subscribe(&self, channel: String) -> broadcast::Receiver<Bytes> {
        let mut pubsub = self.0.pubsub.write().await;

        let sender = pubsub.entry(channel).or_insert_with(|| {
            let (tx, _) = broadcast::channel(1024);
            tx
        });

        sender.subscribe()
    }

    pub async fn subscribe_pattern(&self, pattern: String) -> broadcast::Receiver<(String, Bytes)> {
        let mut patterns = self.0.pattern_pubsub.write().await;

        let sender = patterns.entry(pattern).or_insert_with(|| {
            let (tx, _) = broadcast::channel(1024);
            tx
        });

        sender.subscribe()
    }

    pub async fn cleanup_channel_subscription(&self, channel: &str) {
        let mut pubsub = self.0.pubsub.write().await;

        let remove = pubsub
            .get(channel)
            .is_some_and(|sender| sender.receiver_count() == 0);

        if remove {
            pubsub.remove(channel);
        }
    }

    pub async fn cleanup_pattern_subscription(&self, pattern: &str) {
        let mut patterns = self.0.pattern_pubsub.write().await;

        let remove = patterns
            .get(pattern)
            .is_some_and(|sender| sender.receiver_count() == 0);

        if remove {
            patterns.remove(pattern);
        }
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

fn glob_matches(pattern: &[u8], text: &[u8]) -> bool {
    fn class_matches(pattern: &[u8], start: usize, value: u8) -> Option<(bool, usize)> {
        if pattern.get(start) != Some(&b'[') {
            return None;
        }

        let mut i = start + 1;
        let mut negated = false;

        if pattern.get(i) == Some(&b'^') {
            negated = true;
            i += 1;
        }

        let mut matched = false;
        let mut has_item = false;

        while i < pattern.len() && pattern[i] != b']' {
            let first = pattern[i];
            has_item = true;

            if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
                let last = pattern[i + 2];

                if first <= value && value <= last {
                    matched = true;
                }

                i += 3;
            } else {
                if first == value {
                    matched = true;
                }

                i += 1;
            }
        }

        if i >= pattern.len() || !has_item {
            return None;
        }

        Some((matched != negated, i + 1))
    }

    fn matches(
        pattern: &[u8],
        text: &[u8],
        p: usize,
        t: usize,
        memo: &mut [Vec<Option<bool>>],
    ) -> bool {
        if let Some(result) = memo[p][t] {
            return result;
        }

        let result = if p == pattern.len() {
            t == text.len()
        } else {
            match pattern[p] {
                b'*' => {
                    // Collapse consecutive stars.
                    let mut next = p + 1;

                    while next < pattern.len() && pattern[next] == b'*' {
                        next += 1;
                    }

                    (t..=text.len()).any(|index| matches(pattern, text, next, index, memo))
                }

                b'?' => t < text.len() && matches(pattern, text, p + 1, t + 1, memo),

                b'\\' if p + 1 < pattern.len() => {
                    t < text.len()
                        && pattern[p + 1] == text[t]
                        && matches(pattern, text, p + 2, t + 1, memo)
                }

                b'[' => {
                    if let Some((matched, next)) =
                        class_matches(pattern, p, text.get(t).copied().unwrap_or(0))
                    {
                        t < text.len() && matched && matches(pattern, text, next, t + 1, memo)
                    } else {
                        t < text.len()
                            && pattern[p] == text[t]
                            && matches(pattern, text, p + 1, t + 1, memo)
                    }
                }

                literal => {
                    t < text.len()
                        && literal == text[t]
                        && matches(pattern, text, p + 1, t + 1, memo)
                }
            }
        };

        memo[p][t] = Some(result);
        result
    }

    let mut memo = vec![vec![None; text.len() + 1]; pattern.len() + 1];

    matches(pattern, text, 0, 0, &mut memo)
}
