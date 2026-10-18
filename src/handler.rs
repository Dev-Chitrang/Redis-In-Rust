use crate::command::{Commands, ParseOutcome, RespSerializer};
use crate::store::SharedDb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Handler {
    db: SharedDb,
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Handler {
    pub fn new(db: SharedDb, stream: TcpStream) -> Self {
        Self {
            db,
            stream,
            buffer: Vec::with_capacity(4096),
        }
    }

    pub async fn run(mut self) -> Result<(), std::io::Error> {
        let mut read_buf = [0u8; 1024];

        loop {
            // Read data from the client.
            let bytes_read = self.stream.read(&mut read_buf).await?;

            // Client disconnected.
            if bytes_read == 0 {
                return Ok(());
            }

            // Append newly received bytes to the existing buffer.
            self.buffer.extend_from_slice(&read_buf[..bytes_read]);

            // Process all complete commands in the buffer.
            loop {
                match Commands::parse(&self.buffer) {
                    ParseOutcome::Complete(cmd, consumed) => {
                        if consumed == 0 {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "Parser consumed zero bytes",
                            ));
                        }

                        // Remove the processed command.
                        self.buffer.drain(..consumed);

                        // Execute the command.
                        let response_bytes = self.execute_command(cmd).await;

                        // Send the response to the client.
                        if !response_bytes.is_empty() {
                            self.stream.write_all(&response_bytes).await?;
                            self.stream.flush().await?;
                        }
                    }

                    ParseOutcome::Incomplete => {
                        // Keep buffered bytes and wait for more data.
                        break;
                    }

                    ParseOutcome::Invalid(message) => {
                        let error =
                            RespSerializer::error(&format!("ERR Protocol error: {}", message));

                        self.stream.write_all(&error).await?;
                        self.stream.flush().await?;

                        // Close the connection after malformed input.
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn execute_command(&mut self, cmd: Commands) -> Vec<u8> {
        match cmd {
            // Add this match arm inside execute_command:
            Commands::Hello => RespSerializer::array(vec![
                RespSerializer::bulk_string(b"server"),
                RespSerializer::bulk_string(b"redis-clone"),
                RespSerializer::bulk_string(b"proto"),
                RespSerializer::bulk_string(b"2"),
            ]),
            Commands::Ping { message } => {
                if let Some(msg) = message {
                    RespSerializer::bulk_string(&msg)
                } else {
                    RespSerializer::simple_string("PONG")
                }
            }

            // --- General Key Operations ---
            Commands::Exists { key } => {
                let res = self.db.exists(&key).await;
                RespSerializer::integer(if res { 1 } else { 0 })
            }
            Commands::Del { key } => {
                let count = self.db.del(&key).await;
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
            Commands::Scan => {
                let keys = self.db.scan().await;
                let mut mapped = Vec::new();
                for k in keys {
                    mapped.push(RespSerializer::bulk_string(k.as_bytes()));
                }
                RespSerializer::array(mapped)
            }
            Commands::FlushAll => {
                self.db.flushall().await;
                RespSerializer::simple_string("OK")
            }

            // --- Strings & Numeric Operations ---
            Commands::Set { key, value } => {
                self.db.set(key, value).await;
                self.simple_ok()
            }
            Commands::Get { key } => {
                if let Some(bytes) = self.db.get(&key).await {
                    RespSerializer::bulk_string(&bytes)
                } else {
                    RespSerializer::null_bulk_string()
                }
            }
            Commands::SetNx { key, value } => {
                let count = self.db.setnx(key, value).await;
                RespSerializer::integer(count as i64)
            }
            Commands::Incr { key } => self.handle_incrby(key, 1).await,
            Commands::Decr { key } => self.handle_incrby(key, -1).await,
            Commands::IncrBy { key, amount } => self.handle_incrby(key, amount).await,
            Commands::DecrBy { key, amount } => self.handle_incrby(key, -amount).await,
            Commands::MSet { keys, values } => {
                for (i, key) in keys.into_iter().enumerate() {
                    if let Some(val) = values.get(i) {
                        self.db.set(key, val.clone()).await;
                    }
                }
                self.simple_ok()
            }
            Commands::MGet { keys } => {
                let mut items = Vec::new();
                for key in keys {
                    if let Some(bytes) = self.db.get(&key).await {
                        items.push(RespSerializer::bulk_string(&bytes));
                    } else {
                        items.push(RespSerializer::null_bulk_string());
                    }
                }
                RespSerializer::array(items)
            }

            // --- Hashes ---
            Commands::HSet { key, fields } => {
                let added = self.db.hset_many(key, fields).await;
                RespSerializer::integer(added as i64)
            }

            Commands::HGet { key, field } => {
                if let Some(bytes) = self.db.hget(&key, &field).await {
                    RespSerializer::bulk_string(&bytes)
                } else {
                    RespSerializer::null_bulk_string()
                }
            }
            Commands::HGetAll { key } => {
                if let Some(pairs) = self.db.hgetall(&key).await {
                    let mut elements = Vec::new();
                    for (f, v) in pairs {
                        elements.push(RespSerializer::bulk_string(f.as_bytes()));
                        elements.push(RespSerializer::bulk_string(&v));
                    }
                    RespSerializer::array(elements)
                } else {
                    RespSerializer::array(vec![])
                }
            }
            Commands::HDel { key, field } => {
                let res = self.db.hdel(&key, &field).await;
                RespSerializer::integer(res as i64)
            }
            Commands::HKeys { key } => {
                if let Some(keys) = self.db.hkeys(&key).await {
                    let elements = keys
                        .into_iter()
                        .map(|k| RespSerializer::bulk_string(k.as_bytes()))
                        .collect();
                    RespSerializer::array(elements)
                } else {
                    RespSerializer::array(vec![])
                }
            }

            // --- Lists ---
            Commands::LPush { key, value } => {
                let len = self.db.push_list(key, value, true).await;
                RespSerializer::integer(len as i64)
            }
            Commands::RPush { key, value } => {
                let len = self.db.push_list(key, value, false).await;
                RespSerializer::integer(len as i64)
            }
            Commands::LPop { key } => self.handle_pop_list(&key, true).await,
            Commands::RPop { key } => self.handle_pop_list(&key, false).await,
            Commands::LRange { key, start, end } => {
                if let Some(slice) = self.db.lrange(&key, start, end).await {
                    let elements = slice
                        .into_iter()
                        .map(|b| RespSerializer::bulk_string(&b))
                        .collect();
                    RespSerializer::array(elements)
                } else {
                    RespSerializer::array(vec![])
                }
            }
            Commands::LLen { key } => {
                let len = self.db.llen(&key).await;
                RespSerializer::integer(len as i64)
            }

            // --- Sets ---
            Commands::SAdd { key, members } => {
                let added = self.db.sadd_many(key, members).await;
                RespSerializer::integer(added as i64)
            }

            Commands::SRem { key, value } => {
                let res = self.db.srem(&key, &value).await;
                RespSerializer::integer(res as i64)
            }
            Commands::SMembers { key } => {
                if let Some(members) = self.db.smembers(&key).await {
                    let mut elements = Vec::new();
                    for m in members {
                        elements.push(RespSerializer::bulk_string(&m));
                    }
                    RespSerializer::array(elements)
                } else {
                    RespSerializer::array(vec![])
                }
            }
            Commands::SIsMember { key, value } => {
                let res = self.db.sismember(&key, &value).await;
                RespSerializer::integer(res as i64)
            }
            Commands::SInter { key1, key2 } => {
                let matches = self.db.sinter(&key1, &key2).await;
                let elements = matches
                    .into_iter()
                    .map(|b| RespSerializer::bulk_string(&b))
                    .collect();
                RespSerializer::array(elements)
            }

            // --- Sorted Sets (ZSets) ---
            Commands::ZAdd { key, score, value } => {
                let res = self.db.zadd(key, score, value).await;
                RespSerializer::integer(res as i64)
            }
            Commands::ZRem { key, value } => {
                let res = self.db.zrem(&key, &value).await;
                RespSerializer::integer(res as i64)
            }
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
                let subscriber_count = self.db.publish(&channel, message).await;
                RespSerializer::integer(subscriber_count as i64)
            }
            Commands::Subscribe { channel } => {
                // 1. Generate the confirmation array packet
                let setup_reply = RespSerializer::array(vec![
                    RespSerializer::bulk_string(b"subscribe"),
                    RespSerializer::bulk_string(channel.as_bytes()),
                    RespSerializer::integer(1),
                ]);

                // 2. Write it back to the socket immediately before entering the loop
                if let Err(e) = self.stream.write_all(&setup_reply).await {
                    return RespSerializer::error(&format!(
                        "ERR Subscription Write Failure: {}",
                        e
                    ));
                }
                let _ = self.stream.flush().await;

                // 3. Drop into a one-way reactive broadcast stream reader loop
                if let Err(e) = self.handle_subscribe_stream_only(channel).await {
                    eprintln!("PubSub Stream Error: {}", e);
                }

                vec![] // Return empty to signal the command execution is done
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
        if let Some(bytes) = self.db.pop_list(key, front).await {
            RespSerializer::bulk_string(&bytes)
        } else {
            RespSerializer::null_bulk_string()
        }
    }

    async fn handle_zrange(
        &self,
        key: &str,
        start: i64,
        end: i64,
        with_scores: bool,
        reverse: bool,
    ) -> Vec<u8> {
        if let Some(res) = self.db.zrange(key, start, end, reverse).await {
            let mut elements = Vec::new();
            for m in res {
                // Treat m as a ZSetMember struct directly
                elements.push(RespSerializer::bulk_string(&m.value));
                if with_scores {
                    elements.push(RespSerializer::bulk_string(m.score.to_string().as_bytes()));
                }
            }
            RespSerializer::array(elements)
        } else {
            RespSerializer::array(vec![])
        }
    }

    async fn handle_subscribe_stream_only(
        &mut self,
        channel: String,
    ) -> Result<(), std::io::Error> {
        let mut rx = self.db.subscribe(channel.clone()).await;

        loop {
            tokio::select! {
                msg_res = rx.recv() => {
                    match msg_res {
                        Ok(msg_bytes) => {
                            let broadcast_packet = RespSerializer::array(vec![
                                RespSerializer::bulk_string(b"message"),
                                RespSerializer::bulk_string(channel.as_bytes()),
                                RespSerializer::bulk_string(&msg_bytes),
                            ]);
                            self.stream.write_all(&broadcast_packet).await?;
                            self.stream.flush().await?;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
                _ = self.stream.readable() => {
                    let mut probe = [0u8; 1];
                    if self.stream.try_read(&mut probe).is_ok() && probe[0] == 0 {
                        break; // Client closed the connection
                    }
                }
            }
        }
        Ok(())
    }
}
