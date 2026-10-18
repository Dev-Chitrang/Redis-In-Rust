use crate::handler::Handler;
use crate::store::SharedDb;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Single,
    Multi,
}

pub struct Server {
    db: SharedDb,
    listener: TcpListener,
    mode: ExecutionMode,
    command_lock: Option<Arc<Mutex<()>>>,
}

impl Server {
    pub async fn new(
        addr: &str,
        db: SharedDb,
        mode: ExecutionMode,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;

        // In single mode, all normal commands share one lock.
        // In multi mode, handlers use the existing concurrent model.
        let command_lock = match mode {
            ExecutionMode::Single => Some(Arc::new(Mutex::new(()))),

            ExecutionMode::Multi => None,
        };

        Ok(Self {
            db,
            listener,
            mode,
            command_lock,
        })
    }

    pub async fn run(&self) -> Result<(), std::io::Error> {
        println!("Redis Clone listening on {}", self.listener.local_addr()?);

        println!("Execution mode: {:?}", self.mode);

        // Start background expiration cleanup.
        let cleanup_db = self.db.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                let removed = cleanup_db.remove_expired().await;

                if removed > 0 {
                    println!("[TTL] Removed {} expired keys", removed);
                }
            }
        });

        loop {
            let (stream, addr) = self.listener.accept().await?;

            let db_ref = self.db.clone();
            let command_lock = self.command_lock.clone();

            tokio::spawn(async move {
                let handler = Handler::new(db_ref, stream, command_lock);

                if let Err(error) = handler.run().await {
                    eprintln!("Connection {} closed: {}", addr, error);
                }
            });
        }
    }
}
