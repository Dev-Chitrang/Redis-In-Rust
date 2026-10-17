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

impl Commands {
    pub fn parse(src: &[u8]) -> Option<(Self, usize)> {
        if src.is_empty() {
            return None;
        }

        if src[0] != b'*' {
            let (line, consumed) = read_line(src)?;
            let cmd_str = String::from_utf8_lossy(line).into_owned();
            return Some((Commands::Unknown(cmd_str), consumed));
        }

        let mut idx = 1;

        let (count_bytes, consumed) = read_line(&src[idx..])?;
        idx += consumed;

        let count_str = std::str::from_utf8(count_bytes).ok()?;
        let array_len: usize = count_str.trim().parse().ok()?;

        let mut args: Vec<Bytes> = Vec::with_capacity(array_len);

        for _ in 0..array_len {
            if idx >= src.len() {
                return None;
            }
            if src[idx] != b'$' {
                return None;
            }
            idx += 1;

            let (len_bytes, consumed) = read_line(&src[idx..])?;
            idx += consumed;

            let len_str = std::str::from_utf8(len_bytes).ok()?;
            let bulk_len: usize = len_str.trim().parse().ok()?;

            if idx + bulk_len + 2 > src.len() {
                return None;
            }

            let payload = Bytes::copy_from_slice(&src[idx..(idx + bulk_len)]);
            idx += bulk_len + 2;

            args.push(payload);
        }
        if args.is_empty() {
            return Some((Commands::Unknown("Empty Array".to_string()), idx));
        }

        let total_consumed = idx;
        let command_verb = std::str::from_utf8(&args.get(0)?).ok()?.to_uppercase();

        let cmd = match command_verb.as_str() {
            "EXISTS" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Exists { key }
            }

            "DEL" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Del { key }
            }

            "EXPIRE" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let seconds_str = String::from_utf8(args.get(2)?.to_vec()).ok()?;
                let seconds = seconds_str.trim().parse().unwrap_or(0);
                Commands::Expire { key, seconds }
            }

            "TTL" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Ttl { key }
            }

            "SCAN" => Commands::Scan,

            "FLUSHALL" => Commands::FlushAll,

            "SET" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::Set { key, value }
            }

            "GET" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Get { key }
            }

            "SETNX" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::SetNx { key, value }
            }

            "INCR" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Incr { key }
            }

            "DECR" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Decr { key }
            }

            "INCRBY" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let amount_bytes = args.get(2)?;
                let amount_str = String::from_utf8(amount_bytes).ok()?;
                let amount = amount_str.trim().parse().unwrap_or(0);
                Commands::IncrBy { key, amount }
            }

            "DECRBY" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let amount_bytes = args.get(2)?;
                let amount_str = String::from_utf8(amount_bytes).ok()?;
                let amount = amount_str.trim().parse().unwrap_or(0);
                Commands::DecrBy { key, amount }
            }

            "MSET" => {
                let mut keys = Vec::new();
                let mut values = Vec::new();
                let mut i = 1;
                while i < args.len() {
                    keys.push(String::from_utf8_lossy(args.get(i)?).into_owned());
                    if i + 1 < args.len() {
                        values.push(args.get(i + 1)?.clone());
                    } else {
                        values.push(Bytes::new());
                    }
                    i += 2;
                }
                Commands::MSet { keys, values }
            }

            "MGET" => {
                let mut keys = Vec::new();
                for i in 1..args.len() {
                    keys.push(String::from_utf8_lossy(args.get(i)?).into_owned());
                }
                Commands::MGet { keys }
            }

            "HSET" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let field = String::from_utf8_lossy(&args.get(2)?).into_owned();
                let value = args.get(3)?.clone();
                Commands::HSet { key, field, value }
            }

            "HGET" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let field = String::from_utf8_lossy(&args.get(2)?).into_owned();
                Commands::HGet { key, field }
            }

            "HGETALL" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::HGetAll { key }
            }

            "HDEL" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let field = String::from_utf8_lossy(&args.get(2)?).into_owned();
                Commands::HDel { key, field }
            }

            "HKEYS" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::HKeys { key }
            }

            "LPUSH" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::LPush { key, value }
            }

            "RPUSH" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::RPush { key, value }
            }

            "LPOP" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::LPop { key }
            }

            "RPOP" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::RPop { key }
            }

            "LRANGE" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let start_str = std::str::from_utf8(&args.get(2)?).ok()?;
                let start = start_str.trim().parse().unwrap_or(0);
                let stop_str = std::str::from_utf8(&args.get(3)?).ok()?;
                let stop = stop_str.trim().parse().unwrap_or(0);
                Commands::LRange {
                    key,
                    start,
                    end: stop,
                }
            }

            "LLEN" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::LLen { key }
            }

            "SADD" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::SAdd { key, value }
            }

            "SREM" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::SRem { key, value }
            }

            "SMEMBERS" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::SMembers { key }
            }

            "SISMEMBER" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::SIsMember { key, value }
            }

            "SINTER" => {
                let key1 = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let key2 = String::from_utf8_lossy(&args.get(2)?).into_owned();
                Commands::SInter { key1, key2 }
            }

            "ZADD" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let score = std::str::from_utf8(&args.get(2)?)
                    .ok()?
                    .trim()
                    .parse()
                    .unwrap_or(0);
                let value = args.get(3)?.clone();
                Commands::ZAdd { key, score, value }
            }

            "ZREM" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args.get(2)?.clone();
                Commands::ZRem { key, value }
            }

            "ZRANGE" | "ZREVRANGE" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let start = std::str::from_utf8(&args.get(2)?)
                    .ok()?
                    .trim()
                    .parse()
                    .unwrap_or(0);
                let end = std::str::from_utf8(&args.get(3)?)
                    .ok()?
                    .trim()
                    .parse()
                    .unwrap_or(0);
                let mut with_scores = false;
                if let Some(arg4) = args.get(4) {
                    if std::str::from_utf8(arg4.as_ref()).ok()?.to_uppercase().trim() == "WITHSCORES" {
                        with_scores = true;
                    }
                }
                if command_verb == "ZRANGE" {
                    Commands::ZRange {
                        key,
                        start,
                        end,
                        with_scores,
                    }
                } else {
                    Commands::ZRevRange {
                        key,
                        start,
                        end,
                        with_scores,
                    }
                }
            }

            "PUBLISH" => {
                let channel = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let message = args.get(2)?.clone();
                Commands::Publish { channel, message }
            }

            "SUBSCRIBE" => {
                let channel = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Subscribe { channel }
            }

            _ => Commands::Unknown(command_verb),
        };
        Some((cmd, total_consumed))
    }
}

fn read_line(src: &[u8]) -> Option<(&[u8], usize)> {
    for i in 0..src.len() {
        if src[i] == b'\r' && i + 1 < src.len() && src[i + 1] == b'\n' {
            return Some((&src[0..i], i + 2));
        }
    }
    None
}
