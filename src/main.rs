#![type_length_limit = "152202854"]

use futures_util::{future, StreamExt};
use ntex::ServiceFactory;
use ntex_mqtt::{v3, v5, MqttServer};
use v3::QoS;

use ntex_broker::{
    Publication, QualityOfService, SessionLite, SessionManagerHandle, SessionManagerWorker,
};
use tokio::sync::mpsc;

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
    sessions: SessionManagerHandle,
) -> impl ServiceFactory<
    Config = v3::Session<SessionLite>,
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
                    QoS::AtMostOnce => QualityOfService::AtMostOnce,
                    _ => QualityOfService::AtLeastOnce,
                };

                for (qos, session) in sessions.filter(publish.publish_topic(), qos).await {
                    let publication = Publication::new(
                        &publish.packet().topic,
                        qos,
                        publish.retain(),
                        publish.payload(),
                    );

                    if session.publish(publication).is_err() {
                        panic!("publish to session");
                    }
                }

                Ok(())
            }
        }))
    })
}

fn connect_v3<Io>(
    sessions: SessionManagerHandle,
) -> impl ServiceFactory<
    Config = (),
    Request = v3::Connect<Io>,
    Response = v3::ConnectAck<Io, SessionLite>,
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |connect: v3::Connect<Io>| {
            let sessions = sessions.clone();

            async move {
                let client_id = connect.packet().client_id.clone();

                let (tx, mut rx) = mpsc::unbounded_channel::<Publication>();

                let session = sessions.open_session(client_id.clone(), tx).await;
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
    sessions: SessionManagerHandle,
) -> impl ServiceFactory<
    Config = v3::Session<SessionLite>,
    Request = v3::ControlPacket,
    Response = v3::ControlResult,
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory_with_config(move |session: v3::Session<SessionLite>| {
        let sessions = sessions.clone();

        future::ok(ntex::fn_service(move |req: v3::ControlPacket| {
            let session = session.clone();
            let sessions = sessions.clone();

            async move {
                match req {
                    v3::ControlPacket::Subscribe(mut subscribe) => {
                        let subs = subscribe
                            .iter_mut()
                            .map(|sub| (sub.topic().clone(), from_qos(sub.qos())))
                            .collect();

                        if let Some(acks) =
                            sessions.subscribe(session.state().client_id(), subs).await
                        {
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
                        sessions.close_session(client_id.clone()).await;
                        log::info!("Client {} disconnected", client_id);

                        Ok(disconnect.ack())
                    }
                    v3::ControlPacket::Closed(closed) => {
                        // TODO close session
                        let client_id = session.state().client_id();
                        sessions.close_session(client_id.clone()).await;
                        log::info!("Client {} closed connection", client_id);

                        Ok(closed.ack())
                    }
                }
            }
        }))
    })
}

fn to_qos(qos: QualityOfService) -> QoS {
    match qos {
        QualityOfService::AtMostOnce => QoS::AtMostOnce,
        QualityOfService::AtLeastOnce => QoS::AtLeastOnce,
    }
}

fn from_qos(qos: QoS) -> QualityOfService {
    match qos {
        QoS::AtMostOnce => QualityOfService::AtMostOnce,
        _ => QualityOfService::AtLeastOnce,
    }
}

#[ntex::main]
async fn main() -> std::io::Result<()> {
    // std::env::set_var("RUST_LOG", "ntex=trace,ntex_mqtt=trace,basic=trace");

    env_logger::init();

    let sessions = SessionManagerWorker::new();
    let handle = sessions.handle();

    ntex::rt::spawn(sessions.run());

    ntex::server::Server::build()
        .bind("mqtt", "127.0.0.1:1883", move || {
            MqttServer::new().v3({
                v3::MqttServer::new(connect_v3(handle.clone()))
                    .publish(publish_v3(handle.clone()))
                    .control(control_v3(handle.clone()))
            })
        })?
        .run()
        .await
}
