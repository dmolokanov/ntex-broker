#![type_length_limit = "152202854"]

use futures_util::{future, StreamExt};
use ntex::{channel::mpsc, server::Server, ServiceFactory};
use ntex_broker::{Publication, QualityOfService, Session, SessionManager};
use ntex_mqtt::{v3, v5, MqttServer};
use tokio::sync::broadcast::RecvError;

#[derive(Debug)]
struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

impl std::convert::TryFrom<ServerError> for v5::PublishAck {
    type Error = ServerError;

    fn try_from(err: ServerError) -> Result<Self, Self::Error> {
        Err(err)
    }
}

fn publish_v3(
    sessions: SessionManager,
) -> impl ServiceFactory<
    Config = v3::Session<Session>,
    Request = v3::Publish,
    Response = (),
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |publish: v3::Publish| {
            let sessions = sessions.clone();
            async move {
                let qos = match publish.qos() {
                    v3::QoS::AtMostOnce => QualityOfService::AtMostOnce,
                    _ => QualityOfService::AtLeastOnce,
                };

                let publication = Publication::new(
                    &publish.packet().topic,
                    qos,
                    publish.retain(),
                    publish.payload(),
                );

                sessions.publish(publication);

                Ok(())
            }
        }))
    })
}

fn connect_v3<Io>(
    sessions: SessionManager,
) -> impl ServiceFactory<
    Config = (),
    Request = v3::Connect<Io>,
    Response = v3::ConnectAck<Io, Session>,
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |connect: v3::Connect<Io>| {
            let sessions = sessions.clone();

            async move {
                let client_id = connect.packet().client_id.clone();

                let (tx, mut rx) = mpsc::channel();

                let session = sessions.open_session(client_id.clone(), tx);
                log::info!("Client {} connected", client_id);

                let sink = connect.sink().clone();

                ntex::rt::spawn(async move {
                    while let Some(publication) = rx.next().await {
                        if sink.ready().await.is_err() {
                            log::warn!("Connection is closed");
                            break;
                        }

                        let (topic, qos, retain, payload) = publication.into_parts();
                        let mut publisher = sink.publish(topic, payload);

                        if retain {
                            publisher = publisher.retain();
                        }

                        match qos {
                            QualityOfService::AtMostOnce => publisher.send_at_most_once(),
                            QualityOfService::AtLeastOnce => {
                                if let Err(e) = publisher.send_at_least_once().await {
                                    log::error!("Unable to send publish. {}", e);
                                }
                            }
                        }
                    }

                    log::info!("Sink worker for {} stopped", client_id);
                });

                Ok(connect.ack(session, false))
            }
        }))
    })
}

fn control_v3(
    sessions: SessionManager,
) -> impl ServiceFactory<
    Config = v3::Session<Session>,
    Request = v3::ControlPacket,
    Response = v3::ControlResult,
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory_with_config(move |session: v3::Session<Session>| {
        let sessions = sessions.clone();

        future::ok(ntex::fn_service(move |req: v3::ControlPacket| {
            let session = session.clone();
            let sessions = sessions.clone();

            // todo remove async
            async move {
                match req {
                    v3::ControlPacket::Subscribe(mut subscribe) => {
                        let subs = subscribe
                            .iter_mut()
                            .map(|sub| (sub.topic().clone(), from_qos(sub.qos())))
                            .collect();

                        if let Some(acks) = sessions.subscribe(session.state().client_id(), subs) {
                            for (mut sub, ack) in subscribe.iter_mut().zip(acks) {
                                match ack {
                                    Ok(qos) => sub.subscribe(to_qos(qos)),
                                    Err(e) => {
                                        panic!("error in topic filter {} {:?}", sub.topic(), e)
                                    }
                                }
                            }
                        }

                        Ok(subscribe.ack())
                    }
                    v3::ControlPacket::Unsubscribe(unsub) => {
                        // todo unsubscribe
                        Ok(unsub.ack())
                    }
                    v3::ControlPacket::Ping(ping) => Ok(ping.ack()),
                    v3::ControlPacket::Disconnect(disconnect) => {
                        // TODO close session
                        let client_id = session.state().client_id();
                        sessions.close_session(client_id.clone());
                        log::info!("Client {} disconnected", client_id);

                        Ok(disconnect.ack())
                    }
                    v3::ControlPacket::Closed(closed) => {
                        // TODO close session
                        let client_id = session.state().client_id();
                        sessions.close_session(client_id.clone());
                        log::info!("Client {} closed connection", client_id);

                        Ok(closed.ack())
                    }
                }
            }
        }))
    })
}

fn to_qos(qos: QualityOfService) -> v3::QoS {
    match qos {
        QualityOfService::AtMostOnce => v3::QoS::AtMostOnce,
        QualityOfService::AtLeastOnce => v3::QoS::AtLeastOnce,
    }
}

fn from_qos(qos: v3::QoS) -> QualityOfService {
    match qos {
        v3::QoS::AtMostOnce => QualityOfService::AtMostOnce,
        _ => QualityOfService::AtLeastOnce,
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");

    env_logger::init();

    // let sessions = SessionManagerWorker::new();
    // let handle = sessions.handle();

    // ntex::rt::spawn(sessions.run());

    let (tx, _) = tokio::sync::broadcast::channel(10_0000);

    Server::build()
        .bind("mqtt", "0.0.0.0:1883", move || {
            let sessions = make_session_manager(tx.clone());

            MqttServer::new().v3({
                v3::MqttServer::new(connect_v3(sessions.clone()))
                    .publish(publish_v3(sessions.clone()))
                    .control(control_v3(sessions))
            })
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
                Err(RecvError::Closed) => log::info!("Drained all publications"),
                Err(RecvError::Lagged(lag)) => {
                    log::error!("Receiver started to lag by {} publications", lag)
                }
            }
        }

        log::info!("Session manager stopped receiving neighboring publications");
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
