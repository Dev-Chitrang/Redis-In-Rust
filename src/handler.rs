use crate::command::{Commands, ParseOutcome, RespSerializer};
use crate::store::{DbResult, SharedDb};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use std::sync::Arc;
use tokio::sync::Mutex;

use std::collections::HashMap;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::persistence::Aof;

pub struct Handler {
    db: SharedDb,
    aof: Aof,
    stream: TcpStream,
    buffer: Vec<u8>,
    command_lock: Option<Arc<Mutex<()>>>,

    channel_subscriptions: HashMap<String, JoinHandle<()>>,
    pattern_subscriptions: HashMap<String, JoinHandle<()>>,

    pubsub_tx: mpsc::UnboundedSender<PubSubEvent>,
    pubsub_rx: mpsc::UnboundedReceiver<PubSubEvent>,
}

enum PubSubEvent {
    Message {
        channel: String,
        message: Bytes,
    },
    PatternMessage {
        pattern: String,
        channel: String,
        message: Bytes,
    },
}

impl Handler {
    pub fn new(
        db: SharedDb,
        aof: Aof,
        stream: TcpStream,
        command_lock: Option<Arc<Mutex<()>>>,
    ) -> Self {
        let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();
        Self {
            db,
            aof,
            stream,
            buffer: Vec::with_capacity(4096),
            command_lock,

            channel_subscriptions: HashMap::new(),
            pattern_subscriptions: HashMap::new(),

            pubsub_tx,
            pubsub_rx,
        }
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), std::io::Error> {
        let result = self.run_loop(&mut shutdown).await;

        // Always clean up Pub/Sub tasks on normal exit,
        // connection errors, or a graceful shutdown signal.
        self.cleanup_subscriptions().await;

        // Close the TCP connection.
        let _ = self.stream.shutdown().await;

        result
    }

    async fn run_loop(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), std::io::Error> {
        let mut read_buf = [0u8; 1024];

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        println!("Connection received shutdown signal.");
                        return Ok(());
                    }
                }
                read_result = self.stream.read(&mut read_buf) => {
                    let bytes_read = read_result?;

                    if bytes_read == 0 {
                        return Ok(());
                    }

                    self.buffer.extend_from_slice(
                        &read_buf[..bytes_read]
                    );

                    self.process_buffered_commands().await?;
                }

                event = self.pubsub_rx.recv() => {
                    if let Some(event) = event {
                        self.write_pubsub_event(event).await?;
                    }
                }
            }
        }
    }

    async fn process_buffered_commands(&mut self) -> Result<(), std::io::Error> {
        loop {
            match Commands::parse(&self.buffer) {
                ParseOutcome::Complete(cmd, consumed) => {
                    if consumed == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Parser consumed zero bytes",
                        ));
                    }

                    let raw_command = self.buffer[..consumed].to_vec();
                    let should_persist = cmd.should_persist();
                    let is_setnx = matches!(&cmd, Commands::SetNx { .. });

                    self.buffer.drain(..consumed);

                    // Keep "execute + append" atomic w.r.t. AOF rewrite.
                    let aof_guard = if should_persist {
                        Some(self.aof.begin_write().await)
                    } else {
                        None
                    };

                    let response = if let Some(command_lock) = self.command_lock.clone() {
                        let _guard = command_lock.lock().await;
                        self.execute_command(cmd).await
                    } else {
                        self.execute_command(cmd).await
                    };

                    let failed_conditional =
                        response == b"$-1\r\n" || (is_setnx && response == b":0\r\n");

                    if should_persist
                        && !response.starts_with(b"-")
                        && response != b"$-1\r\n"
                        && !failed_conditional
                    {
                        self.aof.append(&raw_command).await?;
                        drop(aof_guard);
                    }

                    if !response.is_empty() {
                        self.stream.write_all(&response).await?;
                        self.stream.flush().await?;
                    }
                }

                ParseOutcome::Incomplete => break,

                ParseOutcome::Invalid(message) => {
                    let error = RespSerializer::error(&format!("ERR Protocol error: {}", message));

                    self.stream.write_all(&error).await?;
                    self.stream.flush().await?;

                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid RESP protocol frame",
                    ));
                }
            }
        }
        Ok(())
    }

    async fn subscribe_channels(&mut self, channels: Vec<String>) -> Vec<u8> {
        let mut output = Vec::new();

        for channel in channels {
            if !self.channel_subscriptions.contains_key(&channel) {
                let mut rx = self.db.subscribe(channel.clone()).await;
                let tx = self.pubsub_tx.clone();
                let subscribed_channel = channel.clone();

                let task = tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok(message) => {
                                if tx
                                    .send(PubSubEvent::Message {
                                        channel: subscribed_channel.clone(),
                                        message,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }

                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,

                            Err(_) => break,
                        }
                    }
                });

                self.channel_subscriptions.insert(channel.clone(), task);
            }

            let count = self.subscription_count();

            output.extend(RespSerializer::array(vec![
                RespSerializer::bulk_string(b"subscribe"),
                RespSerializer::bulk_string(channel.as_bytes()),
                RespSerializer::integer(count as i64),
            ]));
        }

        output
    }

    async fn unsubscribe_channels(&mut self, channels: Vec<String>) -> Vec<u8> {
        let channels = if channels.is_empty() {
            let mut existing: Vec<String> = self.channel_subscriptions.keys().cloned().collect();

            existing.sort();

            if existing.is_empty() {
                return RespSerializer::array(vec![
                    RespSerializer::bulk_string(b"unsubscribe"),
                    RespSerializer::null_bulk_string(),
                    RespSerializer::integer(self.subscription_count() as i64),
                ]);
            }

            existing
        } else {
            channels
        };

        let mut output = Vec::new();

        for channel in channels {
            if let Some(task) = self.channel_subscriptions.remove(&channel) {
                task.abort();
                let _ = task.await;

                self.db.cleanup_channel_subscription(&channel).await;
            }

            let count = self.subscription_count();

            output.extend(RespSerializer::array(vec![
                RespSerializer::bulk_string(b"unsubscribe"),
                RespSerializer::bulk_string(channel.as_bytes()),
                RespSerializer::integer(count as i64),
            ]));
        }

        output
    }

    async fn subscribe_patterns(&mut self, patterns: Vec<String>) -> Vec<u8> {
        let mut output = Vec::new();

        for pattern in patterns {
            if !self.pattern_subscriptions.contains_key(&pattern) {
                let mut rx = self.db.subscribe_pattern(pattern.clone()).await;

                let tx = self.pubsub_tx.clone();
                let subscribed_pattern = pattern.clone();

                let task = tokio::spawn(async move {
                    loop {
                        match rx.recv().await {
                            Ok((channel, message)) => {
                                if tx
                                    .send(PubSubEvent::PatternMessage {
                                        pattern: subscribed_pattern.clone(),
                                        channel,
                                        message,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }

                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,

                            Err(_) => break,
                        }
                    }
                });

                self.pattern_subscriptions.insert(pattern.clone(), task);
            }

            let count = self.subscription_count();

            output.extend(RespSerializer::array(vec![
                RespSerializer::bulk_string(b"psubscribe"),
                RespSerializer::bulk_string(pattern.as_bytes()),
                RespSerializer::integer(count as i64),
            ]));
        }

        output
    }

    async fn unsubscribe_patterns(&mut self, patterns: Vec<String>) -> Vec<u8> {
        let patterns = if patterns.is_empty() {
            let mut existing: Vec<String> = self.pattern_subscriptions.keys().cloned().collect();

            existing.sort();

            if existing.is_empty() {
                return RespSerializer::array(vec![
                    RespSerializer::bulk_string(b"punsubscribe"),
                    RespSerializer::null_bulk_string(),
                    RespSerializer::integer(self.subscription_count() as i64),
                ]);
            }

            existing
        } else {
            patterns
        };

        let mut output = Vec::new();

        for pattern in patterns {
            if let Some(task) = self.pattern_subscriptions.remove(&pattern) {
                task.abort();
                let _ = task.await;

                self.db.cleanup_pattern_subscription(&pattern).await;
            }

            let count = self.subscription_count();

            output.extend(RespSerializer::array(vec![
                RespSerializer::bulk_string(b"punsubscribe"),
                RespSerializer::bulk_string(pattern.as_bytes()),
                RespSerializer::integer(count as i64),
            ]));
        }

        output
    }

    fn subscription_count(&self) -> usize {
        self.channel_subscriptions.len() + self.pattern_subscriptions.len()
    }

    async fn cleanup_subscriptions(&mut self) {
        let channels: Vec<String> = self.channel_subscriptions.keys().cloned().collect();

        let patterns: Vec<String> = self.pattern_subscriptions.keys().cloned().collect();

        let channel_tasks: Vec<_> = self.channel_subscriptions.drain().collect();

        let pattern_tasks: Vec<_> = self.pattern_subscriptions.drain().collect();

        for (_, task) in channel_tasks {
            task.abort();
            let _ = task.await;
        }

        for (_, task) in pattern_tasks {
            task.abort();
            let _ = task.await;
        }

        for channel in channels {
            self.db.cleanup_channel_subscription(&channel).await;
        }

        for pattern in patterns {
            self.db.cleanup_pattern_subscription(&pattern).await;
        }
    }

    async fn write_pubsub_event(&mut self, event: PubSubEvent) -> Result<(), std::io::Error> {
        let packet = match event {
            PubSubEvent::Message { channel, message } => RespSerializer::array(vec![
                RespSerializer::bulk_string(b"message"),
                RespSerializer::bulk_string(channel.as_bytes()),
                RespSerializer::bulk_string(&message),
            ]),

            PubSubEvent::PatternMessage {
                pattern,
                channel,
                message,
            } => RespSerializer::array(vec![
                RespSerializer::bulk_string(b"pmessage"),
                RespSerializer::bulk_string(pattern.as_bytes()),
                RespSerializer::bulk_string(channel.as_bytes()),
                RespSerializer::bulk_string(&message),
            ]),
        };

        self.stream.write_all(&packet).await?;
        self.stream.flush().await?;

        Ok(())
    }

    async fn execute_command(&mut self, cmd: Commands) -> Vec<u8> {
        if self.subscription_count() > 0 && !cmd.allowed_while_subscribed() {
            return RespSerializer::error(
                "ERR only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING are allowed in this context",
            );
        }
        match cmd {
            Commands::Error(message) => RespSerializer::error(&message),
            // Add this match arm inside execute_command:
            Commands::Hello => RespSerializer::array(vec![
                RespSerializer::bulk_string(b"server"),
                RespSerializer::bulk_string(b"redis-clone"),
                RespSerializer::bulk_string(b"proto"),
                RespSerializer::bulk_string(b"2"),
            ]),
            Commands::Ping { message } => {
                if self.subscription_count() > 0 {
                    RespSerializer::array(vec![
                        RespSerializer::bulk_string(b"pong"),
                        RespSerializer::bulk_string(message.as_deref().unwrap_or(b"")),
                    ])
                } else if let Some(msg) = message {
                    RespSerializer::bulk_string(&msg)
                } else {
                    RespSerializer::simple_string("PONG")
                }
            }

            // --- General Key Operations ---
            Commands::Exists { keys } => {
                let count = self.db.exists(&keys).await;
                RespSerializer::integer(count as i64)
            }
            Commands::Del { keys } => {
                let count = self.db.del(&keys).await;
                RespSerializer::integer(count as i64)
            }
            Commands::Expire { key, seconds } => {
                let res = self.db.expire(&key, seconds).await;
                RespSerializer::integer(res as i64)
            }
            Commands::Ttl { key } => {
                let remaining = self.db.ttl(&key).await;
                RespSerializer::integer(remaining)
            }
            Commands::PTtl { key } => {
                let remaining = self.db.pttl(&key).await;
                RespSerializer::integer(remaining)
            }
            Commands::Scan {
                cursor,
                pattern,
                count,
            } => {
                let (next, keys) = self.db.scan(cursor, pattern.as_deref(), count).await;

                RespSerializer::array(vec![
                    RespSerializer::bulk_string(next.to_string().as_bytes()),
                    RespSerializer::array(
                        keys.iter()
                            .map(|k| RespSerializer::bulk_string(k.as_bytes()))
                            .collect(),
                    ),
                ])
            }
            Commands::Keys { pattern } => {
                let keys = self.db.keys(&pattern).await;

                RespSerializer::array(
                    keys.iter()
                        .map(|k| RespSerializer::bulk_string(k.as_bytes()))
                        .collect(),
                )
            }
            Commands::Type { key } => RespSerializer::simple_string(self.db.type_of(&key).await),
            Commands::BgRewriteAof => {
                if !self.aof.is_enabled() {
                    return RespSerializer::error("AOF is disabled");
                }
                let aof = self.aof.clone();
                let db = self.db.clone();

                tokio::spawn(async move {
                    if let Err(error) = aof.rewrite(&db).await {
                        eprintln!("[AOF] rewrite failed: {}", error);
                    }
                });

                RespSerializer::simple_string("Background append only file rewriting started")
            }
            Commands::FlushAll => {
                self.db.flushall().await;
                RespSerializer::simple_string("OK")
            }

            // --- Strings & Numeric Operations ---
            Commands::Set {
                key,
                value,
                expiry,
                nx,
                xx,
            } => {
                let result = self.db.set_with_options(key, value, expiry, nx, xx).await;

                if result {
                    RespSerializer::simple_string("OK")
                } else {
                    RespSerializer::null_bulk_string()
                }
            }
            Commands::Get { key } => opt_bulk(self.db.get(&key).await),
            Commands::SetNx { key, value } => {
                let count = self.db.setnx(key, value).await;
                RespSerializer::integer(count as i64)
            }
            Commands::Incr { key } => self.handle_incrby(key, 1).await,
            Commands::Decr { key } => self.handle_incrby(key, -11).await,
            Commands::DecrBy { key, amount } => match amount.checked_neg() {
                Some(negated) => self.handle_incrby(key, negated).await,
                None => RespSerializer::error("ERR decrement would overflow"),
            },
            Commands::IncrBy { key, amount } => self.handle_incrby(key, amount).await,
            Commands::MSet { keys, values } => {
                self.db.mset(keys, values).await;
                self.simple_ok()
            }
            Commands::MGet { keys } => {
                let values = self.db.mget(&keys).await;

                RespSerializer::array(
                    values
                        .into_iter()
                        .map(|value| match value {
                            Some(bytes) => RespSerializer::bulk_string(&bytes),
                            None => RespSerializer::null_bulk_string(),
                        })
                        .collect(),
                )
            }

            // --- Hashes ---
            // --- Hashes ---
            Commands::HSet { key, fields } => int_reply(self.db.hset_many(key, fields).await),
            Commands::HGet { key, field } => opt_bulk(self.db.hget(&key, &field).await),
            Commands::HGetAll { key } => match self.db.hgetall(&key).await {
                Ok(pairs) => {
                    let mut elements = Vec::with_capacity(pairs.len() * 2);
                    for (field, value) in pairs {
                        elements.push(RespSerializer::bulk_string(field.as_bytes()));
                        elements.push(RespSerializer::bulk_string(&value));
                    }
                    RespSerializer::array(elements)
                }
                Err(error) => RespSerializer::error(error),
            },
            Commands::HDel { key, fields } => int_reply(self.db.hdel(&key, &fields).await),
            Commands::HKeys { key } => match self.db.hkeys(&key).await {
                Ok(keys) => RespSerializer::array(
                    keys.iter()
                        .map(|k| RespSerializer::bulk_string(k.as_bytes()))
                        .collect(),
                ),
                Err(error) => RespSerializer::error(error),
            },

            // --- Lists ---
            Commands::LPush { key, values } => {
                int_reply(self.db.push_list_many(key, values, true).await)
            }
            Commands::RPush { key, values } => {
                int_reply(self.db.push_list_many(key, values, false).await)
            }
            Commands::LPop { key } => self.handle_pop_list(&key, true).await,
            Commands::RPop { key } => self.handle_pop_list(&key, false).await,
            Commands::LRange { key, start, end } => {
                bulk_array(self.db.lrange(&key, start, end).await)
            }
            Commands::LLen { key } => int_reply(self.db.llen(&key).await),

            // --- Sets ---
            Commands::SAdd { key, members } => int_reply(self.db.sadd_many(key, members).await),
            Commands::SRem { key, values } => int_reply(self.db.srem(&key, &values).await),
            Commands::SMembers { key } => bulk_array(self.db.smembers(&key).await),
            Commands::SIsMember { key, value } => int_reply(self.db.sismember(&key, &value).await),
            Commands::SInter { keys } => bulk_array(self.db.sinter(&keys).await),
            Commands::SUnion { keys } => bulk_array(self.db.sunion(&keys).await),

            // --- Sorted Sets (ZSets) ---
            Commands::ZAdd { key, members } => int_reply(self.db.zadd_many(key, members).await),
            Commands::ZRem { key, values } => int_reply(self.db.zrem(&key, &values).await),
            Commands::ZScore { key, member } => match self.db.zscore(&key, &member).await {
                Ok(Some(score)) => RespSerializer::bulk_string(score.to_string().as_bytes()),
                Ok(None) => RespSerializer::null_bulk_string(),
                Err(error) => RespSerializer::error(error),
            },
            Commands::ZCard { key } => int_reply(self.db.zcard(&key).await),
            Commands::ZRank { key, member } => match self.db.zrank(&key, &member).await {
                Ok(Some(rank)) => RespSerializer::integer(rank as i64),
                Ok(None) => RespSerializer::null_bulk_string(),
                Err(error) => RespSerializer::error(error),
            },
            Commands::ZRange {
                key,
                start,
                end,
                with_scores,
            } => {
                self.handle_zrange(&key, start, end, with_scores, false)
                    .await
            }
            Commands::ZRevRange {
                key,
                start,
                end,
                with_scores,
            } => {
                self.handle_zrange(&key, start, end, with_scores, true)
                    .await
            }

            // --- Realtime Messaging Pub/Sub ---
            Commands::Publish { channel, message } => {
                let count = self.db.publish(&channel, message).await;
                RespSerializer::integer(count as i64)
            }

            Commands::Subscribe { channels } => self.subscribe_channels(channels).await,

            Commands::Unsubscribe { channels } => self.unsubscribe_channels(channels).await,

            Commands::PSubscribe { patterns } => self.subscribe_patterns(patterns).await,

            Commands::PUnsubscribe { patterns } => self.unsubscribe_patterns(patterns).await,

            Commands::PExpire { key, milliseconds } => {
                let result = self.db.pexpire(&key, milliseconds).await;
                RespSerializer::integer(result as i64)
            }

            Commands::Persist { key } => {
                let result = self.db.persist(&key).await;
                RespSerializer::integer(result as i64)
            }

            Commands::Unknown(cmd_name) => {
                RespSerializer::error(&format!("Unknown command: {}", cmd_name))
            }
        }
    }

    fn simple_ok(&self) -> Vec<u8> {
        RespSerializer::simple_string("OK")
    }

    async fn handle_incrby(&self, key: String, amount: i64) -> Vec<u8> {
        match self.db.incrby(key, amount).await {
            Ok(res) => RespSerializer::integer(res),
            Err(e) => RespSerializer::error(e),
        }
    }

    async fn handle_pop_list(&self, key: &str, front: bool) -> Vec<u8> {
        opt_bulk(self.db.pop_list(key, front).await)
    }

    async fn handle_zrange(
        &self,
        key: &str,
        start: i64,
        end: i64,
        with_scores: bool,
        reverse: bool,
    ) -> Vec<u8> {
        match self.db.zrange(key, start, end, reverse).await {
            Ok(members) => {
                let mut elements = Vec::new();

                for m in members {
                    elements.push(RespSerializer::bulk_string(&m.value));

                    if with_scores {
                        elements.push(RespSerializer::bulk_string(m.score.to_string().as_bytes()));
                    }
                }

                RespSerializer::array(elements)
            }
            Err(error) => RespSerializer::error(error),
        }
    }
}

fn int_reply(result: DbResult<usize>) -> Vec<u8> {
    match result {
        Ok(n) => RespSerializer::integer(n as i64),
        Err(error) => RespSerializer::error(error),
    }
}

fn opt_bulk(result: DbResult<Option<Bytes>>) -> Vec<u8> {
    match result {
        Ok(Some(bytes)) => RespSerializer::bulk_string(&bytes),
        Ok(None) => RespSerializer::null_bulk_string(),
        Err(error) => RespSerializer::error(error),
    }
}

fn bulk_array(result: DbResult<Vec<Bytes>>) -> Vec<u8> {
    match result {
        Ok(items) => RespSerializer::array(
            items
                .iter()
                .map(|item| RespSerializer::bulk_string(item))
                .collect(),
        ),
        Err(error) => RespSerializer::error(error),
    }
}
