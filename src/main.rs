#![type_length_limit = "152202854"]

use futures_util::StreamExt;
use log::{error, info};
use ntex::server::Server;
use ntex_broker::{Publication, SessionManager};
use ntex_mqtt::MqttServer;
use tokio::sync::broadcast::RecvError;

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");

    env_logger::init();

    let (tx, _) = tokio::sync::broadcast::channel(10_0000);

    Server::build()
        .bind("mqtt", "0.0.0.0:1883", move || {
            let sessions = make_session_manager(tx.clone());
            MqttServer::new().v3(ntex_broker::v3::server(sessions))
        })?
        .run()
        .await
}

fn make_session_manager(sender: tokio::sync::broadcast::Sender<Publication>) -> SessionManager {
    let manager = SessionManager::new(sender.clone());

    let mut receiver = sender.subscribe();
    let sessions = manager.clone();

    ntex::rt::spawn(async move {
        while let Some(publication) = receiver.next().await {
            match publication {
                Ok(publication) => {
                    sessions.dispatch(publication);
                }
                Err(RecvError::Closed) => info!("Drained all publications"),
                Err(RecvError::Lagged(lag)) => {
                    error!("Receiver started to lag by {} publications", lag)
                }
            }
        }

        info!("Session manager stopped receiving neighboring publications");
    });

    manager
}

// [x] make a session manager per thread
// [x] maintain a session locally
// [x] when publish don't make a filter but a publish
// [x] publish first makes a broadcast to session manager on other threads
//     then matches topics and dispatch packets to appropriate sinks
// [x] since sinks are for local sessions, we can use ntex::channel
// [ ] make inversed search index { Topic -> [Session] }
