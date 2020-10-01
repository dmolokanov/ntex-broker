use futures_util::future;
use log::{debug, info};
use ntex::{server::FromStream, ServiceFactory};
use ntex_mqtt::v3::{
    Connect, ConnectAck, ControlPacket, ControlResult, MqttServer, Publish, QoS,
    Session as NtexSession,
};

use crate::{OpenSession, Publication, QualityOfService, Session, SessionManager};

pub fn server<Io: FromStream + Unpin + 'static>(
    sessions: SessionManager,
    // ) -> MqttServer<Io, Session, impl ServiceFactory, impl ServiceFactory, impl ServiceFactory> {
) -> MqttServer<
    Io,
    Session,
    impl ServiceFactory<
        Config = (),
        Request = Connect<Io>,
        Response = ConnectAck<Io, Session>,
        Error = ServerError,
        InitError = ServerError,
    >,
    impl ServiceFactory<
        Config = NtexSession<Session>,
        Request = ControlPacket,
        Response = ControlResult,
        Error = ServerError,
        InitError = ServerError,
    >,
    impl ServiceFactory<
        Config = NtexSession<Session>,
        Request = Publish,
        Response = (),
        Error = ServerError,
        InitError = ServerError,
    >,
> {
    MqttServer::new(connect_v3(sessions.clone()))
        .publish(publish_v3(sessions.clone()))
        .control(control_v3(sessions))
}

#[derive(Debug)]
pub struct ServerError;

impl From<()> for ServerError {
    fn from(_: ()) -> Self {
        ServerError
    }
}

// impl std::convert::TryFrom<ServerError> for v5::PublishAck {
//     type Error = ServerError;

//     fn try_from(err: ServerError) -> Result<Self, Self::Error> {
//         Err(err)
//     }
// }

fn publish_v3(
    sessions: SessionManager,
) -> impl ServiceFactory<
    Config = NtexSession<Session>,
    Request = Publish,
    Response = (),
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |publish: Publish| {
            let sessions = sessions.clone();
            async move {
                let qos = match publish.qos() {
                    QoS::AtMostOnce => QualityOfService::AtMostOnce,
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
    Request = Connect<Io>,
    Response = ConnectAck<Io, Session>,
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory(move || {
        let sessions = sessions.clone();
        future::ok(ntex::fn_service(move |connect: Connect<Io>| {
            let sessions = sessions.clone();

            async move {
                let client_id = connect.packet().client_id.clone();

                // disconnecting session may live in another session manager

                let session = match sessions.open_session(connect.packet()) {
                    OpenSession::OpenedSession(session) => {
                        info!("Opened a new {} session for client {}", session, client_id);
                        Ok(connect.ack(session, false))
                    }
                    OpenSession::ExistingSession(session) => {
                        info!(
                            "Re-opened a existing {} session for client {}",
                            session, client_id
                        );
                        Ok(connect.ack(session, true))
                    }
                    OpenSession::DuplicateSession(existing, session) => {
                        // TODO disconnect a client previosly connected client
                        todo!()
                    }
                };
                info!("Client {} connected", client_id);
                session

                // let sink = connect.sink().clone();

                // let mut queue = VecDeque::new();
                // ntex::rt::spawn(async move {
                //     while let Some(publication) = rx.next().await {
                //         if session.is_active() {
                //             sink.publish(publication).await;
                //         }
                //     }
                // });
            }
        }))
    })
}

// struct SessionQueue<I> {
//     queue: VecDeque<I>,
//     rx: Receiver<I>,
// }

// impl<I> Stream for SessionQueue<I> {
//     type Item = I;

//     fn poll_next(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         if let Some(item) = self.queue.pop_back() {
//             Poll::Ready(Some(item))
//         } else {
//             match self.rx.poll_next(cx) {
//                 Poll::Ready(Some(item)) => {
//                     self.queue.push_front(item);
//                     Poll::Pending
//                 }
//                 Poll::Ready(None) => Poll::Ready(None),
//                 Poll::Pending => Poll::Pending,
//             }
//         }
//     }
// }

fn control_v3(
    sessions: SessionManager,
) -> impl ServiceFactory<
    Config = NtexSession<Session>,
    Request = ControlPacket,
    Response = ControlResult,
    Error = ServerError,
    InitError = ServerError,
> {
    ntex::fn_factory_with_config(move |session: NtexSession<Session>| {
        let sessions = sessions.clone();

        future::ok(ntex::fn_service(move |req: ControlPacket| {
            let session = session.clone();
            let sessions = sessions.clone();

            // todo remove async
            async move {
                match req {
                    ControlPacket::Subscribe(mut subscribe) => {
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
                    ControlPacket::Unsubscribe(unsub) => {
                        // todo unsubscribe
                        Ok(unsub.ack())
                    }
                    ControlPacket::Ping(ping) => Ok(ping.ack()),
                    ControlPacket::Disconnect(disconnect) => {
                        // TODO close session
                        let client_id = session.state().client_id();
                        sessions.close_session(client_id);
                        info!("Client {} disconnected", client_id);

                        Ok(disconnect.ack())
                    }
                    ControlPacket::Closed(closed) => {
                        // TODO close session
                        let client_id = session.state().client_id();
                        sessions.close_session(client_id);
                        info!("Client {} closed connection", client_id);

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
