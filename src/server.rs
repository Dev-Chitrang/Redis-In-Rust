use crate::handler::Handler;
use crate::store::SharedDb;
use tokio::net::TcpListener;

/// The central server engine running our non-blocking listen loop framework.
pub struct Server {
    db: SharedDb,
    listener: TcpListener,
}

impl Server {
    /// Binds to a local port line and hooks up our shared memory storage engine pointer reference.
    pub async fn new(addr: &str, db: SharedDb) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Server { db, listener })
    }

    /// Infinite execution loop taking incoming raw sockets and spawning async worker pipelines.
    pub async fn run(&self) -> Result<(), std::io::Error> {
        println!("Server processing node online. Monitoring loop open on port...");

        loop {
            // Await a new connection request socket from a client terminal
            let (stream, addr) = self.listener.accept().await?;

            // Clone the database state pointer for the incoming connection task instance
            let db_ref = self.db.clone();

            // Offload session processing loops to separate concurrent threads safely
            tokio::spawn(async move {
                let handler = Handler::new(db_ref, stream);
                if let Err(e) = handler.run().await {
                    eprintln!("Session loop connection dropped with host: {}: {}", addr, e);
                }
            });
        }
    }
}
