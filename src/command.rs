use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum Commands {
    Hello,
    Ping {
        message: Option<Bytes>,
    },
    // General commands
    Exists {
        keys: Vec<String>,
    },
    Del {
        keys: Vec<String>,
    },
    Expire {
        key: String,
        seconds: u64,
    },
    Ttl {
        key: String,
    },
    PTtl {
        key: String,
    },
    Scan,
    FlushAll,

    // String and numeric counters
    Set {
        key: String,
        value: Bytes,
        expiry: Option<std::time::Duration>,
        nx: bool,
        xx: bool,
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
        fields: Vec<(String, Bytes)>,
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
        values: Vec<Bytes>,
    },
    LPop {
        key: String,
    },
    RPush {
        key: String,
        values: Vec<Bytes>,
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
        members: Vec<Bytes>,
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

    PExpire {
        key: String,
        milliseconds: u64,
    },

    Persist {
        key: String,
    },

    Error(String),
    Unknown(String),
}

pub struct RespSerializer;

#[derive(Debug)]
pub enum ParseOutcome {
    Complete(Commands, usize),
    Incomplete,
    Invalid(String),
}

impl RespSerializer {
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
        out.extend_from_slice(&format!("${}\r\n", bytes.len()).into_bytes());
        out.extend_from_slice(bytes);
        out.extend_from_slice(b"\r\n");
        out
    }

    pub fn null_bulk_string() -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }

    pub fn array(elements: Vec<Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&format!("*{}\r\n", elements.len()).into_bytes());
        for el in elements {
            out.extend_from_slice(&el);
        }
        out
    }
}

impl Commands {
    pub fn parse(src: &[u8]) -> ParseOutcome {
        match validate_resp_frame(src) {
            Ok(None) => ParseOutcome::Incomplete,

            Err(message) => ParseOutcome::Invalid(message),

            Ok(Some(frame_len)) => match Self::parse_complete_command(&src[..frame_len]) {
                Some((cmd, consumed)) if consumed == frame_len => {
                    ParseOutcome::Complete(cmd, consumed)
                }

                Some(_) => ParseOutcome::Invalid(
                    "Parser consumed an unexpected number of bytes".to_string(),
                ),

                None => ParseOutcome::Complete(
                    Commands::Error("ERR invalid command arguments".to_string()),
                    frame_len,
                ),
            },
        }
    }

    pub fn parse_complete_command(src: &[u8]) -> Option<(Self, usize)> {
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

        if let Err(message) = validate_command_args(&command_verb, args.len()) {
            return Some((Commands::Error(message), total_consumed));
        }

        let cmd = match command_verb.as_str() {
            "HELLO" => Commands::Hello,
            "PING" => {
                if let Some(arg1) = args.get(1) {
                    Commands::Ping {
                        message: Some(arg1.clone()),
                    }
                } else {
                    Commands::Ping { message: None }
                }
            }

            "EXISTS" => {
                let keys = args[1..]
                    .iter()
                    .map(|arg| String::from_utf8_lossy(arg).into_owned())
                    .collect();
                Commands::Exists { keys }
            }

            "DEL" => {
                let keys = args[1..]
                    .iter()
                    .map(|arg| String::from_utf8_lossy(arg).into_owned())
                    .collect();
                Commands::Del { keys }
            }

            "EXPIRE" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let seconds_str = String::from_utf8(args.get(2)?.to_vec()).ok()?;
                let seconds = seconds_str.trim().parse::<u64>().ok()?;
                Commands::Expire { key, seconds }
            }

            "PEXPIRE" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let milliseconds_str = String::from_utf8(args.get(2)?.to_vec()).ok()?;
                let milliseconds = milliseconds_str.trim().parse::<u64>().ok()? / 1000;
                Commands::PExpire { key, milliseconds }
            }

            "PERSIST" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Persist { key }
            }

            "TTL" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                Commands::Ttl { key }
            }

            "PTTL" => {
                let key = String::from_utf8_lossy(&args[1]).into_owned();
                Commands::PTtl { key }
            }

            "SCAN" => Commands::Scan,

            "FLUSHALL" => Commands::FlushAll,

            "SET" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let value = args[2].clone();

                let mut expiry = None;
                let mut nx = false;
                let mut xx = false;
                let mut i = 3;

                while i < args.len() {
                    let option = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match option.as_str() {
                        "EX" | "PX" => {
                            if expiry.is_some() || i + 1 >= args.len() {
                                return None;
                            }
                            let amount = std::str::from_utf8(&args[i + 1])
                                .ok()?
                                .parse::<u64>()
                                .ok()?;
                            if amount == 0 {
                                return None;
                            }
                            expiry = Some(if option == "EX" {
                                std::time::Duration::from_secs(amount)
                            } else {
                                std::time::Duration::from_millis(amount)
                            });
                            i += 2;
                        }
                        "NX" => {
                            if nx || xx {
                                return None;
                            }
                            nx = true;
                            i += 1
                        }
                        "XX" => {
                            if xx || nx {
                                return None;
                            }
                            xx = true;
                            i += 1;
                        }
                        _ => return None,
                    }
                }
                Commands::Set {
                    key,
                    value,
                    expiry,
                    nx,
                    xx,
                }
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
                let amount_str = String::from_utf8(amount_bytes.to_vec()).ok()?;
                let amount = amount_str.trim().parse::<i64>().ok()?;
                Commands::IncrBy { key, amount }
            }

            "DECRBY" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let amount_bytes = args.get(2)?;
                let amount_str = String::from_utf8(amount_bytes.to_vec()).ok()?;
                let amount = amount_str.trim().parse::<i64>().ok()?;
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

                if args.len() < 4 || args.len() % 2 != 0 {
                    return None;
                }

                let mut fields = Vec::new();
                let mut i = 2;

                while i < args.len() {
                    let field = String::from_utf8_lossy(&args[i]).into_owned();
                    let value = args[i + 1].clone();

                    fields.push((field, value));
                    i += 2;
                }

                Commands::HSet { key, fields }
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
                let values = args[2..].to_vec();
                Commands::LPush { key, values }
            }

            "RPUSH" => {
                let key = String::from_utf8_lossy(&args.get(1)?).into_owned();
                let values = args[2..].to_vec();
                Commands::RPush { key, values }
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
                let start = start_str.trim().parse::<i64>().ok()?;
                let stop_str = std::str::from_utf8(&args.get(3)?).ok()?;
                let stop = stop_str.trim().parse::<i64>().ok()?;
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

                if args.len() < 3 {
                    return None;
                }

                let members = args[2..].to_vec();

                Commands::SAdd { key, members }
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
                    if std::str::from_utf8(arg4.as_ref())
                        .ok()?
                        .to_uppercase()
                        .trim()
                        == "WITHSCORES"
                    {
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

fn validate_resp_frame(src: &[u8]) -> Result<Option<usize>, String> {
    if src.is_empty() {
        return Ok(None);
    }

    // Preserve your existing support for simple, non-array lines.
    if src[0] != b'*' {
        return match read_line(src) {
            Some((_, consumed)) => Ok(Some(consumed)),
            None => Ok(None),
        };
    }

    // Read the RESP array length.
    let (count_bytes, count_consumed) = match read_line(&src[1..]) {
        Some(line) => line,
        None => return Ok(None),
    };

    let count_str = std::str::from_utf8(count_bytes)
        .map_err(|_| "Invalid array length encoding".to_string())?;

    let array_len: usize = count_str
        .trim()
        .parse()
        .map_err(|_| "Invalid RESP array length".to_string())?;

    // A practical limit for this server to prevent unreasonable input.
    if array_len > 10_000 {
        return Err("RESP array contains too many elements".to_string());
    }

    let mut idx = 1 + count_consumed;

    for _ in 0..array_len {
        if idx >= src.len() {
            return Ok(None);
        }

        if src[idx] != b'$' {
            return Err("Expected a RESP bulk string".to_string());
        }

        idx += 1;

        // Read the bulk-string length.
        let (len_bytes, len_consumed) = match read_line(&src[idx..]) {
            Some(line) => line,
            None => return Ok(None),
        };

        let len_str = std::str::from_utf8(len_bytes)
            .map_err(|_| "Invalid bulk string length encoding".to_string())?;

        let bulk_len: usize = len_str
            .trim()
            .parse()
            .map_err(|_| "Invalid bulk string length".to_string())?;

        // Prevent unreasonable payloads.
        if bulk_len > 16 * 1024 * 1024 {
            return Err("RESP bulk string is too large".to_string());
        }

        idx += len_consumed;

        let payload_end = idx
            .checked_add(bulk_len)
            .ok_or_else(|| "Bulk string length overflow".to_string())?;

        let frame_end = payload_end
            .checked_add(2)
            .ok_or_else(|| "Frame length overflow".to_string())?;

        if frame_end > src.len() {
            return Ok(None);
        }

        // Every bulk-string payload must end with CRLF.
        if &src[payload_end..frame_end] != b"\r\n" {
            return Err("Bulk string is missing its CRLF terminator".to_string());
        }

        idx = frame_end;
    }

    Ok(Some(idx))
}

fn validate_command_args(command: &str, argc: usize) -> Result<(), String> {
    let valid = match command {
        // Zero or one argument
        "HELLO" | "SCAN" | "FLUSHALL" => argc == 1,
        "PING" => (1..=2).contains(&argc),

        // Exactly one key
        "TTL" | "PTTL" | "PERSIST" | "GET" | "INCR" | "DECR" | "HGETALL" | "HKEYS" | "LPOP"
        | "RPOP" | "LLEN" | "SMEMBERS" => argc == 2,

        // One or more keys
        "EXISTS" | "DEL" | "MGET" => argc >= 2,

        // Exactly two arguments after the command
        "EXPIRE" | "PEXPIRE" | "SETNX" | "INCRBY" | "DECRBY" | "HGET" | "HDEL" | "SREM"
        | "SISMEMBER" | "ZREM" | "PUBLISH" => argc == 3,

        "SUBSCRIBE" => argc == 2,

        "SET" => argc >= 3,

        "HSET" => argc >= 4 && argc % 2 == 0,

        "LPUSH" | "RPUSH" | "SADD" => argc >= 3,

        "LRANGE" | "ZADD" | "SINTER" => argc == 4,

        "ZRANGE" | "ZREVRANGE" => argc == 4 || (argc == 5 && argc > 1),

        "MSET" => argc >= 3 && argc % 2 == 1,

        // Commands that accept an optional WITHSCORES argument
        _ => true,
    };

    if valid {
        Ok(())
    } else {
        Err(format!(
            "ERR wrong number of arguments for '{}'",
            command.to_lowercase()
        ))
    }
}
