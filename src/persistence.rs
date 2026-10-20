use bytes::Bytes;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};

use crate::command::{Commands, ParseOutcome};
use crate::store::{SharedDb, Value};
use tokio::fs;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    Always,
    EverySec,
    No,
}

#[derive(Clone)]
pub struct Aof {
    file: Arc<Mutex<File>>,
    path: Arc<String>,
    policy: FsyncPolicy,
    dirty: Arc<AtomicBool>,
    // Writers hold read; rewrite holds write. Keeps "execute + append" atomic
    // with respect to the snapshot taken by a rewrite.
    rewrite_gate: Arc<RwLock<()>>,
}

impl Aof {
    pub async fn open(path: &str, policy: FsyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;

        let aof = Self {
            file: Arc::new(Mutex::new(file)),
            path: Arc::new(path.to_string()),
            policy,
            dirty: Arc::new(AtomicBool::new(false)),
            rewrite_gate: Arc::new(RwLock::new(())),
        };

        if policy == FsyncPolicy::EverySec {
            let file = aof.file.clone();
            let dirty = aof.dirty.clone();

            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));

                loop {
                    tick.tick().await;

                    if dirty.swap(false, Ordering::AcqRel) {
                        let file = file.lock().await;

                        if let Err(error) = file.sync_data().await {
                            eprintln!("[AOF] fsync failed: {}", error);
                        }
                    }
                }
            });
        }

        Ok(aof)
    }

    pub async fn append(&self, command: &[u8]) -> io::Result<()> {
        let mut file = self.file.lock().await;

        file.write_all(command).await?;
        file.flush().await?;

        match self.policy {
            FsyncPolicy::Always => file.sync_data().await?,
            FsyncPolicy::EverySec => self.dirty.store(true, Ordering::Release),
            FsyncPolicy::No => {}
        }

        Ok(())
    }

    // Final flush to disk (call on shutdown).
    pub async fn sync(&self) -> io::Result<()> {
        let file = self.file.lock().await;
        file.sync_all().await
    }

    // Hold this around "execute command + append to AOF".
    pub async fn begin_write(&self) -> OwnedRwLockReadGuard<()> {
        self.rewrite_gate.clone().read_owned().await
    }

    // Compaction: replace the log with the minimal commands that rebuild the DB.
    pub async fn rewrite(&self, db: &SharedDb) -> io::Result<()> {
        // Wait for in-flight writers, block new ones until the swap is done.
        let _stop_writers = self.rewrite_gate.write().await;

        let snapshot = db.snapshot().await;
        let mut buffer = Vec::new();

        for (key, value, ttl_ms) in &snapshot {
            encode_entry(&mut buffer, key, value, *ttl_ms);
        }

        let tmp_path = format!("{}.rewrite.tmp", self.path);

        {
            let mut tmp = File::create(&tmp_path).await?;
            tmp.write_all(&buffer).await?;
            tmp.flush().await?;
            tmp.sync_all().await?;
        }

        let mut file = self.file.lock().await;

        fs::rename(&tmp_path, self.path.as_str()).await?;

        *file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path.as_str())
            .await?;

        println!(
            "[AOF] Rewrite finished: {} keys, {} bytes",
            snapshot.len(),
            buffer.len()
        );

        Ok(())
    }

    pub async fn replay(path: &str, db: &SharedDb) -> std::io::Result<usize> {
        let data = match fs::read(path).await {
            Ok(data) => data,

            // First startup: the AOF file may not exist yet.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(0);
            }

            Err(error) => return Err(error),
        };

        let mut offset = 0;
        let mut commands = Vec::new();

        while offset < data.len() {
            match Commands::parse(&data[offset..]) {
                ParseOutcome::Complete(command, consumed) => {
                    if consumed == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "AOF parser consumed zero bytes",
                        ));
                    }

                    validate_replay_command(&command)?;

                    commands.push(command);
                    offset += consumed;
                }

                ParseOutcome::Incomplete => {
                    // Preserve all complete records and discard the
                    // incomplete final record.
                    let file = fs::OpenOptions::new().write(true).open(path).await?;

                    file.set_len(offset as u64).await?;

                    println!("[AOF] Discarded incomplete final record at byte {}", offset);

                    break;
                }

                ParseOutcome::Invalid(message) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid AOF record at byte {}: {}", offset, message),
                    ));
                }
            }
        }

        let replayed = commands.len();

        for command in commands {
            apply_replay_command(db, command).await?;
        }

        println!("[AOF] Replayed {} commands", replayed);

        Ok(replayed)
    }
}

async fn apply_replay_command(db: &SharedDb, command: Commands) -> std::io::Result<()> {
    match command {
        Commands::Set {
            key,
            value,
            expiry,
            nx,
            xx,
        } => {
            let applied = db.set_with_options(key, value, expiry, nx, xx).await;

            if !applied {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "AOF SET conditional write failed during replay",
                ));
            }
        }

        Commands::SetNx { key, value } => {
            let applied = db.setnx(key, value).await;

            if applied == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "AOF SETNX failed during replay",
                ));
            }
        }

        Commands::Del { keys } => {
            db.del(&keys).await;
        }

        Commands::FlushAll => {
            db.flushall().await;
        }

        Commands::Expire { key, seconds } => {
            db.expire(&key, seconds).await;
        }

        Commands::PExpire { key, milliseconds } => {
            db.pexpire(&key, milliseconds).await;
        }

        Commands::Persist { key } => {
            db.persist(&key).await;
        }

        Commands::Incr { key } => {
            db.incrby(key, 1).await.map_err(replay_error)?;
        }

        Commands::Decr { key } => {
            db.incrby(key, -1).await.map_err(replay_error)?;
        }

        Commands::IncrBy { key, amount } => {
            db.incrby(key, amount).await.map_err(replay_error)?;
        }

        Commands::DecrBy { key, amount } => {
            let amount = amount.checked_neg().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "DECRBY amount overflow in AOF",
                )
            })?;

            db.incrby(key, amount).await.map_err(replay_error)?;
        }

        Commands::MSet { keys, values } => {
            db.mset(keys, values).await;
        }

        Commands::HSet { key, fields } => {
            let _ = db.hset_many(key, fields).await;
        }

        Commands::HDel { key, fields } => {
            let _ = db.hdel(&key, &fields).await;
        }

        Commands::LPush { key, values } => {
            db.push_list_many(key, values, true)
                .await
                .map_err(replay_error)?;
        }

        Commands::RPush { key, values } => {
            db.push_list_many(key, values, false)
                .await
                .map_err(replay_error)?;
        }

        Commands::LPop { key } => {
            let _ = db.pop_list(&key, true).await;
        }

        Commands::RPop { key } => {
            let _ = db.pop_list(&key, false).await;
        }

        Commands::SAdd { key, members } => {
            let _ = db.sadd_many(key, members).await;
        }

        Commands::SRem { key, values } => {
            let _ = db.srem(&key, &values).await;
        }

        Commands::ZAdd { key, members } => {
            let _ = db.zadd_many(key, members).await;
        }

        Commands::ZRem { key, values } => {
            let _ = db.zrem(&key, &values).await;
        }

        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "AOF contains a command that is not supported for replay",
            ));
        }
    }

    Ok(())
}

fn replay_error(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("Failed to replay AOF command: {}", error),
    )
}

fn validate_replay_command(command: &Commands) -> io::Result<()> {
    match command {
        Commands::Set { .. }
        | Commands::SetNx { .. }
        | Commands::Del { .. }
        | Commands::FlushAll
        | Commands::Expire { .. }
        | Commands::PExpire { .. }
        | Commands::Persist { .. }
        | Commands::Incr { .. }
        | Commands::Decr { .. }
        | Commands::IncrBy { .. }
        | Commands::DecrBy { .. }
        | Commands::MSet { .. }
        | Commands::HSet { .. }
        | Commands::HDel { .. }
        | Commands::LPush { .. }
        | Commands::RPush { .. }
        | Commands::LPop { .. }
        | Commands::RPop { .. }
        | Commands::SAdd { .. }
        | Commands::SRem { .. }
        | Commands::ZAdd { .. }
        | Commands::ZRem { .. } => Ok(()),

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "AOF contains an invalid or unsupported command",
        )),
    }
}

fn encode_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());

    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

fn encode_entry(out: &mut Vec<u8>, key: &str, value: &Value, ttl_ms: Option<u64>) {
    let k = key.as_bytes();

    match value {
        Value::String(v) => encode_command(out, &[b"SET".as_slice(), k, &v[..]]),

        Value::Hash(map) if !map.is_empty() => {
            let mut args: Vec<&[u8]> = vec![b"HSET".as_slice(), k];

            for (field, v) in map {
                args.push(field.as_bytes());
                args.push(&v[..]);
            }

            encode_command(out, &args);
        }

        Value::List(list) if !list.is_empty() => {
            let mut args: Vec<&[u8]> = vec![b"RPUSH".as_slice(), k];
            args.extend(list.iter().map(|v| &v[..]));

            encode_command(out, &args);
        }

        Value::Set(set) if !set.is_empty() => {
            let mut args: Vec<&[u8]> = vec![b"SADD".as_slice(), k];
            args.extend(set.iter().map(|v| &v[..]));

            encode_command(out, &args);
        }

        Value::SortedSet(zset) if !zset.is_empty() => {
            let pairs: Vec<(String, &Bytes)> = zset
                .scores
                .iter()
                .map(|(member, score)| (score.to_string(), member))
                .collect();

            let mut args: Vec<&[u8]> = vec![b"ZADD".as_slice(), k];

            for (score, member) in &pairs {
                args.push(score.as_bytes());
                args.push(&member[..]);
            }

            encode_command(out, &args);
        }

        // Empty collections cannot be expressed as a command; skip them.
        _ => return,
    }

    if let Some(ms) = ttl_ms {
        let ms = ms.to_string();
        encode_command(out, &[b"PEXPIRE".as_slice(), k, ms.as_bytes()]);
    }
}
