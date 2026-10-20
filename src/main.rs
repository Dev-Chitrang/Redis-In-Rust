pub mod command;
pub mod handler;
pub mod persistence;
pub mod server;
pub mod store;

use crate::persistence::{Aof, FsyncPolicy};
use crate::server::{ExecutionMode, Server};
use crate::store::SharedDb;

use std::env;
use std::error::Error;
use tokio::runtime::Builder;

struct Config {
    host: String,
    port: u16,
    mode: ExecutionMode,
    fsync: FsyncPolicy,
}

impl Config {
    fn from_args() -> Result<Self, String> {
        let mut host = "127.0.0.1".to_string();
        let mut port = 6379_u16;
        let mut mode = ExecutionMode::Multi;
        let mut fsync = FsyncPolicy::EverySec;

        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--host" => {
                    host = args.next().ok_or("--host requires a value")?;
                }

                "--port" => {
                    let value = args.next().ok_or("--port requires a value")?;

                    port = value
                        .parse::<u16>()
                        .map_err(|_| "Invalid port. Expected 0-65535")?;
                }

                "--mode" => {
                    let value = args.next().ok_or("--mode requires single or multi")?;

                    mode = match value.to_lowercase().as_str() {
                        "single" => ExecutionMode::Single,
                        "multi" => ExecutionMode::Multi,
                        _ => {
                            return Err("Invalid mode. Use single or multi".to_string());
                        }
                    };
                }

                "--appendfsync" => {
                    let value = args
                        .next()
                        .ok_or("--appendfsync requires always, everysec or no")?;

                    fsync = match value.to_lowercase().as_str() {
                        "always" => FsyncPolicy::Always,
                        "everysec" => FsyncPolicy::EverySec,
                        "no" => FsyncPolicy::No,
                        _ => {
                            return Err(
                                "Invalid appendfsync. Use always, everysec or no".to_string()
                            );
                        }
                    };
                }

                "--help" | "-h" => {
                    println!(
                        "Usage: redis-clone \
                         [--host HOST] [--port PORT] \
                         [--mode single|multi]"
                    );

                    std::process::exit(0);
                }

                _ => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
        }

        Ok(Self { host, port, mode, fsync })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_args().map_err(std::io::Error::other)?;

    // Select the runtime based on the user's input.
    let mut builder = match config.mode {
        ExecutionMode::Single => Builder::new_current_thread(),

        ExecutionMode::Multi => Builder::new_multi_thread(),
    };

    builder.enable_all();

    let runtime = builder.build()?;

    let address = format!("{}:{}", config.host, config.port);

    println!("Execution mode: {:?}", config.mode);
    println!("Binding address: {}", address);

    runtime.block_on(async {
        let db = SharedDb::new();

        // Restore the database before accepting client connections.
        Aof::replay("appendonly.aof", &db).await?;

        // Open the file for new commands after recovery.
        let aof = Aof::open("appendonly.aof", config.fsync).await?;

        let server = Server::new(&address, db, config.mode, aof).await?;

        server.run().await
    })?;

    Ok(())
}
