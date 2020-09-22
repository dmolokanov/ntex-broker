#![type_length_limit = "152202854"]

use futures_util::{future, StreamExt};
use ntex::ServiceFactory;
use ntex_mqtt::{v3, v5, MqttServer};
use v3::QoS;

use ntex_broker::{MqttSession, Publication, QualityOfService, SessionManager};

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
    sessions: &SessionManager,
) -> impl ServiceFactory<
    Config = v3::Session<MqttSession>,
    Request = v3::Publish,
    Response = (),
    Error = ServerError,
    InitError = ServerError,
> {
    let sessions = sessions.clone();

    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |publish: v3::Publish| {
            let qos = match publish.qos() {
                QoS::AtMostOnce => QualityOfService::AtMostOnce,
                QoS::AtLeastOnce => QualityOfService::AtLeastOnce,
                QoS::ExactlyOnce => QualityOfService::AtLeastOnce,
            };

            for (qos, session) in sessions.filter(publish.publish_topic(), qos) {
                let publication = Publication::new(
                    &publish.packet().topic,
                    qos,
                    publish.retain(),
                    publish.payload(),
                );

                session.publish(publication).expect("publish to session")
            }

            future::ok(())
        }))
    })
}

fn connect_v3<Io>(
    sessions: &SessionManager,
) -> impl ServiceFactory<
    Config = (),
    Request = v3::Connect<Io>,
    Response = v3::ConnectAck<Io, MqttSession>,
    Error = ServerError,
    InitError = ServerError,
> {
    let sessions = sessions.clone();

    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |connect: v3::Connect<Io>| {
            let client_id = connect.packet().client_id.clone();

            let (tx, mut rx) = ntex::channel::mpsc::channel::<Publication>();

            let session = sessions.open_session(&client_id, tx);
            log::info!("Client {} connected", client_id);

            let sink = connect.sink().clone();

            ntex::rt::spawn(async move {
                while let Some(publication) = rx.next().await {
                    let (topic, qos, retain, payload) = publication.into_parts();
                    let mut publisher = sink.publish(topic, payload);

                    if retain {
                        publisher = publisher.retain();
                    }

                    match qos {
                        QualityOfService::AtMostOnce => publisher.send_at_most_once(),
                        QualityOfService::AtLeastOnce => {
                            if let Err(e) = publisher.send_at_least_once().await {
                                log::error!("unable to send publish. {}", e);
                            }
                        }
                    }
                }

                log::info!("Sink worker for {} stopped", client_id);
            });

            future::ok(connect.ack(session, false))
        }))
    })
}

fn control_v3(
    sessions: &SessionManager,
) -> impl ServiceFactory<
    Config = v3::Session<MqttSession>,
    Request = v3::ControlPacket,
    Response = v3::ControlResult,
    Error = ServerError,
    InitError = ServerError,
> {
    let sessions = sessions.clone();

    ntex::fn_factory_with_config(move |session: v3::Session<MqttSession>| {
        let sessions = sessions.clone();

        future::ok(ntex::fn_service(move |req: v3::ControlPacket| {
            match req {
                v3::ControlPacket::Subscribe(mut subscribe) => {
                    for mut sub in subscribe.iter_mut() {
                        let qos = match sub.qos() {
                            QoS::AtMostOnce => QualityOfService::AtMostOnce,
                            QoS::AtLeastOnce => QualityOfService::AtLeastOnce,
                            QoS::ExactlyOnce => {
                                return future::err(ServerError); //todo refactor to separate error
                            }
                        };

                        match session.state().subscribe(sub.topic(), qos) {
                            Ok(_) => {
                                sub.subscribe(sub.qos());
                            }
                            Err(_) => {
                                return future::err(ServerError); //todo refactor to separate error
                            }
                        }
                    }
                    future::ok(subscribe.ack())
                }
                v3::ControlPacket::Unsubscribe(unsub) => {
                    // todo unsubscribe
                    future::ok(unsub.ack())
                }
                v3::ControlPacket::Ping(ping) => future::ok(ping.ack()),
                v3::ControlPacket::Disconnect(disconnect) => {
                    // TODO close session
                    let client_id = session.state().client_id();
                    sessions.close_session(&client_id);
                    log::info!("Client {} disconnected", client_id);

                    future::ok(disconnect.ack())
                }
                v3::ControlPacket::Closed(closed) => {
                    // TODO close session
                    let client_id = session.state().client_id();
                    sessions.close_session(&client_id);
                    log::info!("Client {} closed connection", client_id);

                    future::ok(closed.ack())
                }
            }
        }))
    })
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");

    env_logger::init();

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:1883", || {
            // WARNING session manager is per thread!
            let sessions = SessionManager::new();

            MqttServer::new().v3({
                v3::MqttServer::new(connect_v3(&sessions))
                    .publish(publish_v3(&sessions))
                    .control(control_v3(&sessions))
            })
            // .v5(v5::MqttServer::new(connect_v5).publish(publish_v5))
        })?
        .workers(1)
        .run()
        .await
}
