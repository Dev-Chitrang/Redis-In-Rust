use crate::handler::Handler;
use crate::store::SharedDb;

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinSet;

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

    pub async fn run(self) -> Result<(), std::io::Error> {
        let Server {
            db,
            listener,
            mode,
            command_lock,
        } = self;

        println!("Redis Clone listening on {}", listener.local_addr()?);
        println!("Execution mode: {:?}", mode);

        // Signal active connections when shutdown begins.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Track all connection tasks.
        let mut connections = JoinSet::new();

        // Start the background TTL cleanup task.
        let cleanup_db = db.clone();

        let cleanup_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                interval.tick().await;

                let removed = cleanup_db.remove_expired().await;

                if removed > 0 {
                    println!("[TTL] Removed {} expired keys", removed);
                }
            }
        });

        println!("Press Ctrl+C to shut down gracefully.");

        // Accept connections until Ctrl+C is received.
        loop {
            tokio::select! {
                signal_result = tokio::signal::ctrl_c() => {
                    if let Err(error) = signal_result {
                        eprintln!("Failed to listen for Ctrl+C: {}", error);
                    }

                    println!("\nShutdown signal received.");
                    break;
                }

                accept_result = listener.accept() => {
                    let (stream, addr) = accept_result?;

                    println!("Connection established: {}", addr);

                    let connection_db = db.clone();
                    let connection_lock = command_lock.clone();
                    let connection_shutdown = shutdown_rx.clone();

                    connections.spawn(async move {
                        let handler = Handler::new(
                            connection_db,
                            stream,
                            connection_lock,
                        );

                        if let Err(error) =
                            handler.run(connection_shutdown).await
                        {
                            eprintln!(
                                "Connection {} closed: {}",
                                addr,
                                error
                            );
                        } else {
                            println!("Connection {} closed.", addr);
                        }
                    });
                }

                Some(result) = connections.join_next(),
                    if !connections.is_empty() =>
                {
                    if let Err(error) = result {
                        eprintln!("Connection task failed: {}", error);
                    }
                }
            }
        }

        // Stop accepting connections immediately.
        drop(listener);

        println!("Stopping background tasks...");

        // Signal active handlers to stop.
        let _ = shutdown_tx.send(true);

        // Stop and await the TTL cleanup task.
        cleanup_task.abort();
        let _ = cleanup_task.await;

        // Wait for active connections to clean up and finish.
        println!("Waiting for active connections to finish...");

        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                eprintln!("Connection task failed: {}", error);
            }
        }

        println!("All connection tasks finished.");
        println!("Redis Clone shut down successfully.");

        Ok(())
    }
}
