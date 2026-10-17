use crate::store::ZSetMember;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum Commands {
    // General commands
    Exists {
        key: String,
    },
    Del {
        key: String,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    Scan,
    FlushAll,

    // String and numeric counters
    Set {
        key: String,
        value: Bytes,
    },
    Get {
        key: String,
    },
    SetNx {
        key: String,
        value: Bytes,
    },
    IncrBy {
        key: String,
        amount: i64,
    },
    DecrBy {
        key: String,
        amount: i64,
    },
    Incr {
        key: String,
    },
    Decr {
        key: String,
    },
    MSet {
        keys: Vec<String>,
        values: Vec<Bytes>,
    },
    MGet {
        keys: Vec<String>,
    },

    // Hashes
    HSet {
        key: String,
        field: String,
        value: Bytes,
    },
    HGet {
        key: String,
        field: String,
    },
    HGetAll {
        key: String,
    },
    HDel {
        key: String,
        field: String,
    },
    HKeys {
        key: String,
    },

    // Lists
    LPush {
        key: String,
        value: Bytes,
    },
    LPop {
        key: String,
    },
    RPush {
        key: String,
        value: Bytes,
    },
    RPop {
        key: String,
    },
    LRange {
        key: String,
        start: i64,
        end: i64,
    },
    LLen {
        key: String,
    },

    // Sets
    SAdd {
        key: String,
        value: Bytes,
    },
    SRem {
        key: String,
        value: Bytes,
    },
    SMembers {
        key: String,
    },
    SIsMember {
        key: String,
        value: Bytes,
    },
    SInter {
        key1: String,
        key2: String,
    },

    // Sorted Sets
    ZAdd {
        key: String,
        score: i64,
        value: Bytes,
    },
    ZRem {
        key: String,
        value: Bytes,
    },
    ZRange {
        key: String,
        start: i64,
        end: i64,
        with_scores: bool,
    },
    ZRevRange {
        key: String,
        start: i64,
        end: i64,
        with_scores: bool,
    }, // <-- Fixed: Added missing trailing comma

    // Publish / Subscribe
    Publish {
        channel: String,
        message: Bytes,
    },
    Subscribe {
        channel: String,
    },

    Unknown(String),
}

pub struct RespSerializer;

impl RespSerializer {
    // <-- Fixed: Typed "RespSerializer" matching definition struct name
    pub fn simple_string(s: &str) -> Vec<u8> {
        format!("+{}\r\n", s).into_bytes()
    }

    pub fn error(msg: &str) -> Vec<u8> {
        format!("-{}\r\n", msg).into_bytes()
    }

    pub fn integer(i: i64) -> Vec<u8> {
        format!(":{}\r\n", i).into_bytes()
    }

    pub fn bulk_string(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("${}\r\n", bytes.len()).into_bytes()); // <-- Fixed: Added missing semicolon
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\r\n");
        out
    }

    pub fn null_bulk_string() -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }

    pub fn array(elements: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(format!("*{}\r\n", elements.len()).into_bytes());
        for el in elements {
            out.extend_from_slice(&el);
        }
        out
    }
}
