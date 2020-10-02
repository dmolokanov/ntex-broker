use std::{cell::RefCell, collections::VecDeque, fmt::Formatter, rc::Rc};

use bytestring::ByteString;
use futures_util::StreamExt;
use fxhash::FxHashMap;
use log::{debug, info, warn};
use ntex::channel::mpsc::{self, Sender};
use ntex_mqtt::{
    v3::codec::Connect, v3::codec::TopicError, v3::error::SendPacketError, v3::MqttSink, Topic,
};
use tokio::{sync::broadcast::Sender as BroadcastSender, task::JoinHandle};
use uuid::Uuid;

use crate::{Publication, QualityOfService};

#[derive(Debug, Clone)]
pub enum Session {
    Transient(ConnectedSession),
    Persistent(ConnectedSession),
    Disconnecting(DisconnectingSession),
    Offline(OfflineSession),
}

impl Session {
    pub fn new_transient(client_id: ByteString) -> Self {
        let connected = ConnectedSession::new(client_id);
        Self::Transient(connected)
    }

    pub fn new_persistent(client_id: ByteString) -> Self {
        let connected = ConnectedSession::new(client_id);
        Self::Persistent(connected)
    }

    pub fn client_id(&self) -> &ByteString {
        match self {
            Session::Transient(connected) => connected.client_id(),
            Session::Persistent(connected) => connected.client_id(),
            Session::Disconnecting(disconnecting) => disconnecting.client_id(),
            Session::Offline(offline) => offline.client_id(),
        }
    }

    pub fn subscribe(
        &self,
        filter: &ByteString,
        qos: QualityOfService,
    ) -> Result<QualityOfService, SubscribeError> {
        match self {
            Session::Transient(connected) | Session::Persistent(connected) => connected
                .subscribe(filter, qos)
                .map_err(|_| SubscribeError::Topic(filter.clone())),
            Session::Disconnecting(disconnecting) => Err(SubscribeError::NoSession),
            Session::Offline(offline) => Err(SubscribeError::NoSession),
        }
    }

    pub fn filter(
        &self,
        topic: &str,
        publication_qos: QualityOfService,
    ) -> Option<QualityOfService> {
        todo!()
    }

    pub fn publish(&self, publication: Publication) {
        todo!()
    }

    // pub async fn close(&self) -> VecDeque<Publication> {
    //     match self {
    //         Session::Transient(connected) => connected.close().await,
    //     }
    // }
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error("invalid topic {0}")]
    Topic(ByteString),

    #[error("Disconnecting session")]
    NoSession,
}

impl std::fmt::Display for Session {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Session::Transient(_) => write!(f, "transient"),
            Session::Persistent(_) => write!(f, "persistent"),
            Session::Disconnecting(_) => write!(f, "disconnecting"),
            Session::Offline(_) => write!(f, "offline"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectedSession {
    client_id: ByteString,
    // state: SessionState,
    // sink: ConnectedSessionSink,
    // join_handle: JoinHandle<VecDeque<Publication>>,
}

impl ConnectedSession {
    fn new(client_id: ByteString) -> Self {
        Self {
            client_id
            // state: SessionState::new(client_id),
            // sink: (),
            // join_hande: (),
        }
    }

    fn client_id(&self) -> &ByteString {
        &self.client_id
    }

    fn subscribe(
        &self,
        filter: &ByteString,
        qos: QualityOfService,
    ) -> Result<QualityOfService, TopicError> {
        todo!()
    }

    fn into_disconnecting(self) -> DisconnectingSession {
        DisconnectingSession {
            client_id: self.client_id,
        }
    }

    fn into_offline(self) -> OfflineSession {
        OfflineSession {
            client_id: self.client_id,
        }
    }

    // pub async fn close(&self) -> VecDeque<Publication> {
    //     self.state.close();
    //     self.join_handle.await.expect("join on close")
    // }
}

#[derive(Debug, Clone)]
pub struct DisconnectingSession {
    client_id: ByteString,
    // state: SessionState,
    // sink: ConnectedSessionSink,
    // join_handle: JoinHandle<VecDeque<Publication>>,
}

impl DisconnectingSession {
    fn new(client_id: ByteString) -> Self {
        Self {
            client_id
            // state: SessionState::new(client_id),
            // sink: (),
            // join_hande: (),
        }
    }

    fn client_id(&self) -> &ByteString {
        &self.client_id
    }

    // pub async fn close(&self) -> VecDeque<Publication> {
    //     self.state.close();
    //     self.join_handle.await.expect("join on close")
    // }
}

#[derive(Debug, Clone)]
pub struct OfflineSession {
    client_id: ByteString,
    // state: SessionState,
    // sink: ConnectedSessionSink,
    // join_handle: JoinHandle<VecDeque<Publication>>,
}

impl OfflineSession {
    // fn new(client_id: ByteString) -> Self {
    //     Self {
    //         client_id
    //         // state: SessionState::new(client_id),
    //         // sink: (),
    //         // join_hande: (),
    //     }
    // }

    fn client_id(&self) -> &ByteString {
        &self.client_id
    }

    // pub async fn close(&self) -> VecDeque<Publication> {
    //     self.state.close();
    //     self.join_handle.await.expect("join on close")
    // }

    fn into_online(self) -> ConnectedSession {
        ConnectedSession {
            client_id: self.client_id,
        }
    }
}

struct SessionInner {
    client_id: ByteString,
    subscriptions: FxHashMap<ByteString, Subscription>,
    sender: Sender<Publication>,
}

#[derive(Clone)]
pub struct SessionState;

impl SessionState {
    fn new(_client_id: ByteString) -> Self {
        Self
    }

    fn close(&self) {
        todo!()
    }
}

fn drain_connected(
    client_id: ByteString,
    sink: MqttSink,
    mut queue: VecDeque<Publication>,
) -> (Sender<Publication>, JoinHandle<VecDeque<Publication>>) {
    let (tx, mut rx) = mpsc::channel();

    let sink = ConnectedSessionSink { sink };

    let join = ntex::rt::spawn(async move {
        // TODO to support persisted session we need to store message queue
        // when client disconnected we still need to drain publications from the channel
        // 1. where to store the queue?
        // 2. when client disconnected, this task should continue to work
        // 3. on a shutdown, drain the channel? do we need the channel in the first place?

        // drain previously queued items
        while let Some(publication) = queue.pop_back() {
            match sink.send(publication).await {
                Ok(_) => log::debug!("Publication successfully sent"),
                Err(SendError::Closed) => {
                    log::warn!("Connection is closed");
                    break;
                }
                Err(SendError::SendPublication(e)) => {
                    log::error!("Unable to send publish. {}", e);
                    break;
                }
            }
        }

        // if there are still some items in the queue
        if !queue.is_empty() {
            let remaining = rx.fold(queue, |mut queue, p| async {
                queue.push_front(p);
                queue
            });
            return remaining.await;
        }

        log::debug!("Drained backlog of publications");

        // drain messages until sender closes the channel on its side
        while let Some(publication) = rx.next().await {
            match sink.send(publication).await {
                Ok(_) => log::debug!("Publication successfully sent"),
                Err(SendError::Closed) => {
                    log::warn!("Connection is closed");
                }
                Err(SendError::SendPublication(e)) => {
                    log::error!("Unable to send publish. {}", e);
                }
            }
        }

        log::info!("Sink worker for {} stopped", client_id);
        rx.collect().await
    });
    (tx, join)
}

struct ConnectedSessionSink {
    sink: MqttSink,
}

impl ConnectedSessionSink {
    pub async fn send(&self, publication: Publication) -> Result<(), SendError> {
        // TODO do not send for offline/disconnecting session
        self.sink.ready().await.map_err(|_| SendError::Closed)?;

        let (topic, qos, retain, payload) = publication.into_parts();
        let mut publisher = self.sink.publish(topic, payload);

        if retain {
            publisher = publisher.retain();
        }

        match qos {
            QualityOfService::AtMostOnce => {
                publisher.send_at_most_once();
                Ok(())
            }
            QualityOfService::AtLeastOnce => publisher
                .send_at_least_once()
                .await
                .map_err(SendError::SendPublication),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("Connection is closed")]
    Closed,

    #[error("Error sending a publication. {0}")]
    SendPublication(SendPacketError),
}

// #[derive(Clone)]
// pub struct SessionState(Rc<RefCell<SessionInner>>);

// impl SessionState {
//     pub fn new(client_id: ByteString, sender: Sender<Publication>) -> Self {
//         Self(Rc::new(RefCell::new(SessionInner {
//             client_id,
//             sender,
//             subscriptions: Default::default(),
//         })))
//     }

//     pub fn client_id(&self) -> ByteString {
//         self.0.borrow().client_id.clone()
//     }

//     pub fn subscribe(
//         &self,
//         filter: &ByteString,
//         qos: QualityOfService,
//     ) -> Result<QualityOfService, TopicError> {
//         let mut inner = self.0.borrow_mut();
//         if inner.subscriptions.contains_key(filter) {
//             Ok(qos)
//         } else {
//             let topic = filter.parse()?;
//             let subscription = Subscription::new(topic, qos);
//             inner.subscriptions.insert(filter.clone(), subscription);

//             Ok(qos)
//         }
//     }

//     pub fn filter(
//         &self,
//         topic: &str,
//         publication_qos: QualityOfService,
//     ) -> Option<QualityOfService> {
//         self.0
//             .borrow()
//             .subscriptions
//             .values()
//             .filter(|sub| sub.matches(topic))
//             .fold(None, |acc, sub| {
//                 acc.map(|qos| cmp::max(qos, cmp::min(sub.max_qos(), publication_qos)))
//                     .or_else(|| Some(cmp::min(sub.max_qos(), publication_qos)))
//             })
//     }

//     pub fn publish(&self, publication: Publication) {
//         if self.0.borrow().sender.send(publication).is_err() {
//             log::error!("Unable to dispatch publication. Receiving part is closed");
//         }
//     }
// }

struct SubscriptionInner {
    topic: Topic,
    max_qos: QualityOfService,
}

#[derive(Clone)]
pub struct Subscription(Rc<RefCell<SubscriptionInner>>);

impl Subscription {
    pub fn new(topic: Topic, max_qos: QualityOfService) -> Self {
        Self(Rc::new(RefCell::new(SubscriptionInner { topic, max_qos })))
    }

    pub fn max_qos(&self) -> QualityOfService {
        self.0.borrow().max_qos
    }

    pub fn matches(&self, topic: &str) -> bool {
        self.0.borrow().topic.matches_str(topic)
    }
}

struct SessionManagerInner {
    broadcast: BroadcastSender<Publication>,
    sessions: FxHashMap<ByteString, Session>,
}

#[derive(Clone)]
pub struct SessionManager(Rc<RefCell<SessionManagerInner>>);

impl SessionManager {
    pub fn new(sender: BroadcastSender<Publication>) -> Self {
        Self(Rc::new(RefCell::new(SessionManagerInner {
            broadcast: sender,
            sessions: Default::default(),
        })))
    }

    pub fn open_session(&self, connect: &Connect) -> OpenSession {
        // let mut inner = self.0.borrow_mut();

        // match inner.sessions.remove(&client_id) {
        //     Some(Session::Transient(connected)) => {
        //         // TODO check if the client send duplicate of CONNECT message
        //         // MQTT protocol requires to drop current session and disconnect the client
        //     }
        // }

        // let queue = if let Some(existing) = inner.sessions.remove(&client_id) {
        //     existing.close().await
        // } else {
        //     Default::default()
        // };

        // let _existing = sessions.remove(client_id);

        let client_id = make_client_id(&connect.client_id);

        let mut inner = self.0.borrow_mut();
        match inner.sessions.remove(&client_id) {
            Some(Session::Transient(connected)) | Some(Session::Persistent(connected)) => {
                debug!("Found existing online session for {}", client_id);
                let disconnecting = Session::Disconnecting(connected.into_disconnecting());
                let session = if connect.client_id.is_empty() {
                    info!("Cleaning offline session for {}", client_id);
                    Session::new_transient(client_id.clone())
                } else {
                    info!("Moving offline session into online for {}", client_id);
                    Session::Persistent(offline.into_online())
                };

                inner.sessions.insert(client_id.clone(), session.clone());
                OpenSession::DuplicateSession(disconnecting, session)
            }
            Some(Session::Offline(offline)) => {
                debug!("Found offline session for {}", client_id);
                let session = if connect.client_id.is_empty() {
                    info!("Cleaning offline session for {}", client_id);
                    Session::new_transient(client_id.clone())
                } else {
                    info!("Moving offline session into online for {}", client_id);
                    Session::Persistent(offline.into_online())
                };

                inner.sessions.insert(client_id.clone(), session.clone());
                OpenSession::ExistingSession(session)
            }
            _ => {
                let session = if connect.clean_session {
                    info!("Opening a new transient session for {}", client_id);
                    Session::new_transient(client_id.clone())
                } else {
                    info!("Opening a new persistent session for {}", client_id);
                    Session::new_persistent(client_id.clone())
                };

                inner
                    .sessions
                    .insert(session.client_id().clone(), session.clone());
                OpenSession::OpenedSession(session)
            }
        }
    }

    pub fn close_session(&self, client_id: &ByteString) -> Option<Session> {
        let mut inner = self.0.borrow_mut();
        match inner.sessions.remove(client_id) {
            Some(Session::Transient(connected)) => {
                debug!("Closing transient session for {}", client_id);
                Some(Session::Disconnecting(connected.into_disconnecting()))
            }
            Some(Session::Persistent(connected)) => {
                debug!("Moving persistent session to offline for {}", client_id);
                let offline = connected.clone().into_offline();
                let session = Session::Offline(offline);
                inner.sessions.insert(client_id.clone(), session);

                Some(Session::Disconnecting(connected.into_disconnecting()))
            }
            Some(Session::Offline(offline)) => {
                warn!("Closing already close session for {}", client_id);
                let session = Session::Offline(offline);
                inner.sessions.insert(client_id.clone(), session);
                None
            }
            _ => None,
        }
    }

    pub fn subscribe(
        &self,
        client_id: &ByteString,
        subscribe_to: Vec<(ByteString, QualityOfService)>,
    ) -> Option<Vec<Result<QualityOfService, SubscribeError>>> {
        let mut inner = self.0.borrow_mut();

        inner.sessions.get_mut(client_id).map(|session| {
            subscribe_to
                .into_iter()
                .map(|(topic, qos)| session.subscribe(&topic, qos))
                .collect()
        })
    }

    pub fn publish(&self, publication: Publication) {
        let inner = self.0.borrow();

        // notify all other managers
        if inner.broadcast.send(publication.clone()).is_err() {
            log::error!("No active session managers found");
        }

        self.dispatch(publication);
    }

    pub fn dispatch(&self, publication: Publication) {
        // filter local subscriptions that matched topic
        let inner = self.0.borrow();
        let sessions = inner.sessions.values().filter_map(|session| {
            session
                .filter(&publication.topic, publication.qos)
                .map(|qos| (qos, session))
        });

        // dispatch a copy of publication to each matched session
        for (qos, session) in sessions {
            let mut publication = publication.clone();
            publication.qos = qos;

            session.publish(publication);
        }
    }
}

fn make_client_id(client_id: &ByteString) -> ByteString {
    if client_id.is_empty() {
        Uuid::new_v4().to_hyphenated().to_string().into()
    } else {
        client_id.clone()
    }
}

#[derive(Debug)]
pub enum OpenSession {
    OpenedSession(Session),
    ExistingSession(Session),
    DuplicateSession(Session, Session),
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;

    use super::*;

    #[test]
    fn it_opens_transient_session() {
        let connect = connect("client");

        let sessions = sessions();
        let session = sessions.open_session(&connect);

        assert_matches!(session, OpenSession::OpenedSession(Session::Transient(_)));
        assert_eq!(sessions.0.borrow().sessions.len(), 1);

        let state = sessions.0.borrow();
        let transient = state.sessions.get(&connect.client_id);
        assert_matches!(transient, Some(Session::Transient(_)));
    }

    #[test]
    fn it_opens_persistent_session() {
        let mut connect = connect("client");
        connect.clean_session = false;

        let sessions = sessions();
        let session = sessions.open_session(&connect);

        assert_matches!(session, OpenSession::OpenedSession(Session::Persistent(_)));
        assert_eq!(sessions.0.borrow().sessions.len(), 1);

        let state = sessions.0.borrow();
        let persistent = state.sessions.get(&connect.client_id);
        assert_matches!(persistent, Some(Session::Persistent(_)));
    }

    #[test]
    fn it_reopens_offline_session() {
        let client_id = ByteString::from("client");
        let mut connect = connect(client_id.as_ref());
        connect.clean_session = false;

        let sessions = sessions();
        let _session = sessions.open_session(&connect);
        let _closed = sessions.close_session(&client_id);

        let session = sessions.open_session(&connect);

        assert_matches!(
            session,
            OpenSession::ExistingSession(Session::Persistent(_))
        );
        assert_eq!(sessions.0.borrow().sessions.len(), 1);

        let state = sessions.0.borrow();
        let persistent = state.sessions.get(&connect.client_id);
        assert_matches!(persistent, Some(Session::Persistent(_)));
    }

    #[test]
    fn it_closes_transient_session() {
        let client_id = ByteString::from("client");
        let connect = connect(client_id.as_ref());

        let sessions = sessions();
        let _ = sessions.open_session(&connect);

        let closed = sessions.close_session(&client_id);

        assert_matches!(closed, Some(_));
        assert_eq!(sessions.0.borrow().sessions.len(), 0);
    }

    #[test]
    fn it_closes_persistent_session() {
        let client_id = ByteString::from("client");
        let mut connect = connect(client_id.as_ref());
        connect.clean_session = false;

        let sessions = sessions();
        let _ = sessions.open_session(&connect);

        let closed = sessions.close_session(&client_id);

        assert_matches!(closed, Some(_));
        assert_eq!(sessions.0.borrow().sessions.len(), 1);

        let state = sessions.0.borrow();
        let persistent = state.sessions.get(&connect.client_id);
        assert_matches!(persistent, Some(Session::Offline(_)));
    }

    #[test]
    fn it_closes_offline_session() {
        let client_id = ByteString::from("client");
        let mut connect = connect(client_id.as_ref());
        connect.clean_session = false;
        let sessions = sessions();
        let _ = sessions.open_session(&connect);
        let _ = sessions.close_session(&client_id);

        let closed = sessions.close_session(&client_id);

        assert_matches!(closed, None);
        assert_eq!(sessions.0.borrow().sessions.len(), 1);
    }

    fn sessions() -> SessionManager {
        let (tx, _) = tokio::sync::broadcast::channel(10);
        SessionManager::new(tx)
    }

    fn connect(client_id: impl Into<ByteString>) -> Connect {
        Connect {
            clean_session: true,
            client_id: client_id.into(),
            ..Connect::default()
        }
    }
}
