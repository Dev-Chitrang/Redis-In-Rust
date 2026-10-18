pub mod command;
pub mod handler;
pub mod server;
pub mod store;

use crate::server::Server;
use crate::store::SharedDb;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    // 1. Instantiate the singular thread-safe cluster storage database master record
    let db = SharedDb::new();

    // 2. Bind network server process loop structures to standard localhost line
    let server_address = "127.0.0.1:6379";
    let app_server = Server::new(server_address, db).await?;

    println!(
        "Redis Clone active! Local host connection port: {}",
        server_address
    );

    // 3. Launch processing loop execution state lifetimes
    app_server.run().await?;

    Ok(())
}
