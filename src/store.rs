use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock, RwLockWriteGuard};
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

fn rank_of(root: &RankLink, target: &ZSetMember) -> Option<usize> {
    let mut node = root.as_ref();
    let mut rank = 0;

    while let Some(n) = node {
        match target.cmp(&n.member) {
            Ordering::Less => node = n.left.as_ref(),
            Ordering::Greater => {
                rank += subtree_size(&n.left) + 1;
                node = n.right.as_ref();
            }
            Ordering::Equal => return Some(rank + subtree_size(&n.left)),
        }
    }

    None
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
    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        let score = *self.scores.get(member)?;

        let target = ZSetMember {
            score,
            value: member.clone(),
        };

        rank_of(&self.root, &target)
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

pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
pub const NOT_INT: &str = "ERR value is not an integer or out of range";
pub const OVERFLOW: &str = "ERR increment or decrement would overflow";

pub type DbResult<T> = Result<T, &'static str>;

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Hash(_) => "hash",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::SortedSet(_) => "zset",
        }
    }

    fn as_string(&self) -> DbResult<&Bytes> {
        match self {
            Value::String(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_hash(&self) -> DbResult<&HashMap<String, Bytes>> {
        match self {
            Value::Hash(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_hash_mut(&mut self) -> DbResult<&mut HashMap<String, Bytes>> {
        match self {
            Value::Hash(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_list(&self) -> DbResult<&VecDeque<Bytes>> {
        match self {
            Value::List(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_list_mut(&mut self) -> DbResult<&mut VecDeque<Bytes>> {
        match self {
            Value::List(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_set(&self) -> DbResult<&HashSet<Bytes>> {
        match self {
            Value::Set(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_set_mut(&mut self) -> DbResult<&mut HashSet<Bytes>> {
        match self {
            Value::Set(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_zset(&self) -> DbResult<&SortedSet> {
        match self {
            Value::SortedSet(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
    fn as_zset_mut(&mut self) -> DbResult<&mut SortedSet> {
        match self {
            Value::SortedSet(v) => Ok(v),
            _ => Err(WRONGTYPE),
        }
    }
}
#[derive(Debug, Clone)]
pub struct DbEntry {
    pub value: Value,
    pub expires_at: Option<Instant>,
}

#[derive(Debug)]
pub struct Db {
    pub shards: Vec<RwLock<HashMap<String, DbEntry>>>,
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

fn live<'a>(entries: &'a HashMap<String, DbEntry>, key: &str) -> Option<&'a DbEntry> {
    entries.get(key).filter(|entry| !is_expired(entry))
}

// Redis removes a collection key as soon as it becomes empty.
fn drop_if_empty(entries: &mut HashMap<String, DbEntry>, key: &str) {
    let empty = entries.get(key).is_some_and(|entry| match &entry.value {
        Value::Hash(v) => v.is_empty(),
        Value::List(v) => v.is_empty(),
        Value::Set(v) => v.is_empty(),
        Value::SortedSet(v) => v.is_empty(),
        Value::String(_) => false,
    });

    if empty {
        entries.remove(key);
    }
}

pub fn key_hash(key: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

// Write guards for a set of shards, always acquired in ascending shard order.
pub struct LockedShards<'a> {
    guards: Vec<(usize, RwLockWriteGuard<'a, HashMap<String, DbEntry>>)>,
}

impl<'a> LockedShards<'a> {
    fn map(&mut self, key: &str) -> &mut HashMap<String, DbEntry> {
        let index = SharedDb::shard_index(key);
        let pos = self
            .guards
            .binary_search_by_key(&index, |(i, _)| *i)
            .expect("shard was not locked");

        &mut self.guards[pos].1
    }
}

impl SharedDb {
    pub fn new() -> Self {
        let shards = (0..SHARED_COUNT)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self(Arc::new(Db {
            shards,
            pubsub: RwLock::new(HashMap::new()),
            pattern_pubsub: RwLock::new(HashMap::new()),
        }))
    }

    fn shard_index(key: &str) -> usize {
        (key_hash(key) as usize) % SHARED_COUNT
    }

    // Lock only the shards these keys live in, in sorted order (no deadlock).
    async fn lock_keys<'a>(&'a self, keys: &[String]) -> LockedShards<'a> {
        let mut indexes: Vec<usize> = keys.iter().map(|k| Self::shard_index(k)).collect();
        indexes.sort_unstable();
        indexes.dedup();

        let mut guards = Vec::with_capacity(indexes.len());
        for index in indexes {
            guards.push((index, self.0.shards[index].write().await));
        }

        LockedShards { guards }
    }

    async fn lock_all(&self) -> LockedShards<'_> {
        let mut guards = Vec::with_capacity(SHARED_COUNT);
        for (index, shard) in self.0.shards.iter().enumerate() {
            guards.push((index, shard.write().await));
        }

        LockedShards { guards }
    }

    fn shard(&self, key: &str) -> &RwLock<HashMap<String, DbEntry>> {
        &self.0.shards[Self::shard_index(key)]
    }

    // General key operations

    pub async fn exists(&self, keys: &[String]) -> usize {
        let mut shards = self.lock_keys(keys).await;
        let mut count = 0;

        for key in keys {
            let entries = shards.map(key);
            remove_if_expired(entries, key);

            if entries.contains_key(key) {
                count += 1;
            }
        }

        count
    }

    pub async fn del(&self, keys: &[String]) -> usize {
        let mut shards = self.lock_keys(keys).await;
        let mut deleted = 0;

        for key in keys {
            let entries = shards.map(key);
            remove_if_expired(entries, key);

            if entries.remove(key).is_some() {
                deleted += 1;
            }
        }

        deleted
    }

    pub async fn expire(&self, key: &str, seconds: u64) -> usize {
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

    pub async fn type_of(&self, key: &str) -> &'static str {
        let entries = self.shard(key).read().await;

        live(&entries, key).map_or("none", |entry| entry.value.type_name())
    }

    pub async fn keys(&self, pattern: &str) -> Vec<String> {
        let shards = self.lock_all().await;
        let now = Instant::now();
        let mut out = Vec::new();

        for (_, entries) in shards.guards.iter() {
            for (key, entry) in entries.iter() {
                let alive = entry.expires_at.map_or(true, |exp| now < exp);

                if alive && glob_matches(pattern.as_bytes(), key.as_bytes()) {
                    out.push(key.clone());
                }
            }
        }

        out
    }

    // Cursor = 64-bit key hash. Keys are visited in (hash, key) order, so a key
    // that exists for the whole iteration is always returned exactly once,
    // even while other keys are added or removed.
    pub async fn scan(
        &self,
        cursor: u64,
        pattern: Option<&str>,
        count: usize,
    ) -> (u64, Vec<String>) {
        let shards = self.lock_all().await;
        let now = Instant::now();
        let mut candidates: Vec<(u64, String)> = Vec::new();

        for (_, entries) in shards.guards.iter() {
            for (key, entry) in entries.iter() {
                let alive = entry.expires_at.map_or(true, |exp| now < exp);
                let hash = key_hash(key);

                if alive && hash >= cursor {
                    candidates.push((hash, key.clone()));
                }
            }
        }

        drop(shards);
        candidates.sort();

        let mut end = count.max(1).min(candidates.len());

        // Never split keys that share one hash across two pages.
        while end > 0 && end < candidates.len() && candidates[end].0 == candidates[end - 1].0 {
            end += 1;
        }

        let next = if end >= candidates.len() {
            0
        } else {
            candidates[end - 1].0 + 1
        };

        let keys = candidates[..end]
            .iter()
            .filter(|(_, key)| pattern.map_or(true, |p| glob_matches(p.as_bytes(), key.as_bytes())))
            .map(|(_, key)| key.clone())
            .collect();

        (next, keys)
    }

    pub async fn flushall(&self) {
        let mut shards = self.lock_all().await;

        for (_, entries) in shards.guards.iter_mut() {
            entries.clear();
        }
    }

    // Consistent copy of the whole DB for AOF rewrite: (key, value, remaining ms).
    pub async fn snapshot(&self) -> Vec<(String, Value, Option<u64>)> {
        let shards = self.lock_all().await;
        let now = Instant::now();
        let mut out = Vec::new();

        for (_, entries) in shards.guards.iter() {
            for (key, entry) in entries.iter() {
                let ttl_ms = match entry.expires_at {
                    Some(exp) if now >= exp => continue,
                    Some(exp) => Some((exp - now).as_millis() as u64),
                    None => None,
                };

                out.push((key.clone(), entry.value.clone(), ttl_ms));
            }
        }

        out
    }

    // Strings and numeric operations

    pub async fn set(&self, key: String, value: Bytes) {
        let mut entries = self.shard(&key).write().await;
        entries.insert(
            key,
            DbEntry {
                value: Value::String(value),
                expires_at: None,
            },
        );
    }

    // Atomic across every key it touches.
    pub async fn mset(&self, keys: Vec<String>, values: Vec<Bytes>) {
        let mut shards = self.lock_keys(&keys).await;

        for (key, value) in keys.into_iter().zip(values) {
            shards.map(&key).insert(
                key,
                DbEntry {
                    value: Value::String(value),
                    expires_at: None,
                },
            );
        }
    }

    pub async fn mget(&self, keys: &[String]) -> Vec<Option<Bytes>> {
        let mut shards = self.lock_keys(keys).await;
        let mut out = Vec::with_capacity(keys.len());

        for key in keys {
            let entries = shards.map(key);
            remove_if_expired(entries, key);

            // Like Redis: wrong type reads as nil inside MGET.
            out.push(match entries.get(key).map(|e| &e.value) {
                Some(Value::String(bytes)) => Some(bytes.clone()),
                _ => None,
            });
        }

        out
    }

    pub async fn set_with_options(
        &self,
        key: String,
        value: Bytes,
        expiry: Option<std::time::Duration>,
        nx: bool,
        xx: bool,
    ) -> bool {
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

    pub async fn get(&self, key: &str) -> DbResult<Option<Bytes>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(None),
            Some(entry) => Ok(Some(entry.value.as_string()?.clone())),
        }
    }

    pub async fn setnx(&self, key: String, value: Bytes) -> usize {
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

    pub async fn incrby(&self, key: String, amount: i64) -> DbResult<i64> {
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert_with(|| DbEntry {
            value: Value::String(Bytes::from_static(b"0")),
            expires_at: None,
        });

        let current = entry.value.as_string()?;

        let value: i64 = std::str::from_utf8(current)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(NOT_INT)?;

        let updated = value.checked_add(amount).ok_or(OVERFLOW)?;

        entry.value = Value::String(Bytes::from(updated.to_string()));

        Ok(updated)
    }

    // Hashes Engine

    pub async fn hset_many(&self, key: String, fields: Vec<(String, Bytes)>) -> DbResult<usize> {
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert_with(|| DbEntry {
            value: Value::Hash(HashMap::new()),
            expires_at: None,
        });

        let map = entry.value.as_hash_mut()?;
        let mut added = 0;

        for (field, value) in fields {
            if map.insert(field, value).is_none() {
                added += 1;
            }
        }

        Ok(added)
    }

    pub async fn hget(&self, key: &str, field: &str) -> DbResult<Option<Bytes>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(None),
            Some(entry) => Ok(entry.value.as_hash()?.get(field).cloned()),
        }
    }

    pub async fn hgetall(&self, key: &str) -> DbResult<Vec<(String, Bytes)>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(Vec::new()),
            Some(entry) => Ok(entry
                .value
                .as_hash()?
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()),
        }
    }

    pub async fn hdel(&self, key: &str, fields: &[String]) -> DbResult<usize> {
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);

        let Some(entry) = entries.get_mut(key) else {
            return Ok(0);
        };

        let map = entry.value.as_hash_mut()?;
        let removed = fields
            .iter()
            .filter(|f| map.remove(f.as_str()).is_some())
            .count();

        drop_if_empty(&mut entries, key);

        Ok(removed)
    }

    pub async fn hkeys(&self, key: &str) -> DbResult<Vec<String>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(Vec::new()),
            Some(entry) => Ok(entry.value.as_hash()?.keys().cloned().collect()),
        }
    }

    // List engine

    pub async fn push_list_many(
        &self,
        key: String,
        values: Vec<Bytes>,
        front: bool,
    ) -> DbResult<usize> {
        let mut entries = self.shard(&key).write().await;

        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert_with(|| DbEntry {
            value: Value::List(VecDeque::new()),
            expires_at: None,
        });

        let deque = entry.value.as_list_mut()?;

        for value in values {
            if front {
                deque.push_front(value);
            } else {
                deque.push_back(value);
            }
        }

        Ok(deque.len())
    }

    pub async fn pop_list(&self, key: &str, front: bool) -> DbResult<Option<Bytes>> {
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);

        let Some(entry) = entries.get_mut(key) else {
            return Ok(None);
        };

        let deque = entry.value.as_list_mut()?;

        let popped = if front {
            deque.pop_front()
        } else {
            deque.pop_back()
        };

        drop_if_empty(&mut entries, key);

        Ok(popped)
    }

    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> DbResult<Vec<Bytes>> {
        let entries = self.shard(key).read().await;

        let Some(entry) = live(&entries, key) else {
            return Ok(Vec::new());
        };

        let deque = entry.value.as_list()?;
        let len = deque.len() as i64;

        let start_idx = if start < 0 {
            len.saturating_add(start).max(0)
        } else {
            start
        };
        let stop_idx = if stop < 0 {
            len.saturating_add(stop)
        } else {
            stop.min(len - 1)
        };

        if start_idx > stop_idx || start_idx >= len {
            return Ok(Vec::new());
        }

        Ok(deque
            .iter()
            .skip(start_idx as usize)
            .take((stop_idx - start_idx + 1) as usize)
            .cloned()
            .collect())
    }

    pub async fn llen(&self, key: &str) -> DbResult<usize> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(0),
            Some(entry) => Ok(entry.value.as_list()?.len()),
        }
    }

    // Sets engine

    pub async fn sadd_many(&self, key: String, members: Vec<Bytes>) -> DbResult<usize> {
        let mut entries = self.shard(&key).write().await;
        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert_with(|| DbEntry {
            value: Value::Set(HashSet::new()),
            expires_at: None,
        });

        let set = entry.value.as_set_mut()?;

        Ok(members
            .into_iter()
            .filter(|m| set.insert(m.clone()))
            .count())
    }

    pub async fn smembers(&self, key: &str) -> DbResult<Vec<Bytes>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(Vec::new()),
            Some(entry) => Ok(entry.value.as_set()?.iter().cloned().collect()),
        }
    }

    pub async fn sismember(&self, key: &str, member: &Bytes) -> DbResult<usize> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(0),
            Some(entry) => Ok(entry.value.as_set()?.contains(member) as usize),
        }
    }

    pub async fn srem(&self, key: &str, members: &[Bytes]) -> DbResult<usize> {
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);

        let Some(entry) = entries.get_mut(key) else {
            return Ok(0);
        };

        let set = entry.value.as_set_mut()?;
        let removed = members.iter().filter(|m| set.remove(*m)).count();

        drop_if_empty(&mut entries, key);

        Ok(removed)
    }

    // Reads every key's set under its shard lock. Missing key = empty set.
    async fn collect_sets(
        shards: &mut LockedShards<'_>,
        keys: &[String],
    ) -> DbResult<Vec<HashSet<Bytes>>> {
        let mut sets = Vec::with_capacity(keys.len());

        for key in keys {
            let entries = shards.map(key);
            remove_if_expired(entries, key);

            sets.push(match entries.get(key) {
                None => HashSet::new(),
                Some(entry) => entry.value.as_set()?.clone(),
            });
        }

        Ok(sets)
    }

    pub async fn sinter(&self, keys: &[String]) -> DbResult<Vec<Bytes>> {
        let mut shards = self.lock_keys(keys).await;
        let mut sets = Self::collect_sets(&mut shards, keys).await?.into_iter();

        let mut result = sets.next().unwrap_or_default();

        for other in sets {
            result.retain(|member| other.contains(member));
        }

        Ok(result.into_iter().collect())
    }

    pub async fn sunion(&self, keys: &[String]) -> DbResult<Vec<Bytes>> {
        let mut shards = self.lock_keys(keys).await;
        let sets = Self::collect_sets(&mut shards, keys).await?;

        let mut result = HashSet::new();

        for set in sets {
            result.extend(set);
        }

        Ok(result.into_iter().collect())
    }

    // Sorted sets engine

    pub async fn zadd_many(&self, key: String, members: Vec<(f64, Bytes)>) -> DbResult<usize> {
        let mut entries = self.shard(&key).write().await;

        remove_if_expired(&mut entries, &key);

        let entry = entries.entry(key).or_insert_with(|| DbEntry {
            value: Value::SortedSet(SortedSet::new()),
            expires_at: None,
        });

        let zset = entry.value.as_zset_mut()?;

        let mut added = 0;

        for (score, value) in members {
            // NaN is rejected by the command parser.
            // Normalize negative zero for consistent ordering.
            let score = if score == 0.0 { 0.0 } else { score };

            let previous_score = zset.scores.insert(value.clone(), score);

            if let Some(previous_score) = previous_score {
                zset.remove_member(&ZSetMember {
                    score: previous_score,
                    value: value.clone(),
                });
            } else {
                added += 1;
            }

            zset.insert_member(ZSetMember { score, value });
        }

        Ok(added)
    }

    pub async fn zrange(
        &self,
        key: &str,
        start: i64,
        end: i64,
        reverse: bool,
    ) -> DbResult<Vec<ZSetMember>> {
        let entries = self.shard(key).read().await;

        let Some(entry) = live(&entries, key) else {
            return Ok(Vec::new());
        };

        let zset = entry.value.as_zset()?;
        let len = zset.len() as i64;

        if len == 0 {
            return Ok(Vec::new());
        }

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
            return Ok(Vec::new());
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

        Ok(result)
    }

    pub async fn zrem(&self, key: &str, values: &[Bytes]) -> DbResult<usize> {
        let mut entries = self.shard(key).write().await;
        remove_if_expired(&mut entries, key);

        let Some(entry) = entries.get_mut(key) else {
            return Ok(0);
        };

        let zset = entry.value.as_zset_mut()?;
        let mut removed = 0;

        for value in values {
            if let Some(score) = zset.scores.remove(value) {
                zset.remove_member(&ZSetMember {
                    score,
                    value: value.clone(),
                });
                removed += 1;
            }
        }

        drop_if_empty(&mut entries, key);

        Ok(removed)
    }

    pub async fn zscore(&self, key: &str, member: &Bytes) -> DbResult<Option<f64>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(None),
            Some(entry) => Ok(entry.value.as_zset()?.scores.get(member).copied()),
        }
    }

    pub async fn zcard(&self, key: &str) -> DbResult<usize> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(0),
            Some(entry) => Ok(entry.value.as_zset()?.len()),
        }
    }

    pub async fn zrank(&self, key: &str, member: &Bytes) -> DbResult<Option<usize>> {
        let entries = self.shard(key).read().await;

        match live(&entries, key) {
            None => Ok(None),
            Some(entry) => Ok(entry.value.as_zset()?.rank(member)),
        }
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
        let mut shards = self.lock_all().await;
        let now = Instant::now();
        let mut removed = 0;

        for (_, entries) in shards.guards.iter_mut() {
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
