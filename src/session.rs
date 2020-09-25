use std::{cell::RefCell, cmp, rc::Rc};

use bytestring::ByteString;
use fxhash::FxHashMap;
use ntex_mqtt::{v3::codec::TopicError, Topic};
use tokio::sync::{broadcast::Sender, mpsc::UnboundedSender};

use crate::{Publication, QualityOfService};

struct SessionInner {
    client_id: ByteString,
    subscriptions: FxHashMap<ByteString, Subscription>,
    sender: UnboundedSender<Publication>,
}

#[derive(Clone)]
pub struct Session(Rc<RefCell<SessionInner>>);

impl Session {
    pub fn new(client_id: ByteString, sender: UnboundedSender<Publication>) -> Self {
        Self(Rc::new(RefCell::new(SessionInner {
            client_id,
            sender,
            subscriptions: Default::default(),
        })))
    }

    pub fn client_id(&self) -> ByteString {
        self.0.borrow().client_id.clone()
    }

    // fn sender(&self) -> UnboundedSender<Publication> {
    //     self.0.borrow().sender.clone()
    // }

    pub fn subscribe(
        &self,
        filter: &ByteString,
        qos: QualityOfService,
    ) -> Result<QualityOfService, TopicError> {
        let mut inner = self.0.borrow_mut();
        if inner.subscriptions.contains_key(filter) {
            Ok(qos)
        } else {
            let topic = filter.parse()?;
            let subscription = Subscription::new(topic, qos);
            inner.subscriptions.insert(filter.clone(), subscription);

            Ok(qos)
        }
    }

    pub fn filter(
        &self,
        topic: &str,
        publication_qos: QualityOfService,
    ) -> Option<QualityOfService> {
        self.0
            .borrow()
            .subscriptions
            .values()
            .filter(|sub| sub.matches(topic))
            .fold(None, |acc, sub| {
                acc.map(|qos| cmp::max(qos, cmp::min(sub.max_qos(), publication_qos)))
                    .or_else(|| Some(cmp::min(sub.max_qos(), publication_qos)))
            })
    }

    pub fn publish(&self, publication: Publication) {
        if self.0.borrow().sender.send(publication).is_err() {
            log::error!("Unable to dispatch publication. Receiving part is closed");
        }
    }
}

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
    sender: Sender<Publication>,
    sessions: FxHashMap<ByteString, Session>,
}

#[derive(Clone)]
pub struct SessionManager(Rc<RefCell<SessionManagerInner>>);

impl SessionManager {
    pub fn new(sender: Sender<Publication>) -> Self {
        Self(Rc::new(RefCell::new(SessionManagerInner {
            sender,
            sessions: Default::default(),
        })))
    }

    pub fn open_session(
        &self,
        client_id: ByteString,
        sender: UnboundedSender<Publication>,
    ) -> Session {
        let mut inner = self.0.borrow_mut();
        let _existing = inner.sessions.remove(&client_id);
        // let _existing = sessions.remove(client_id);

        let session = Session::new(client_id.clone(), sender);
        inner.sessions.insert(client_id, session.clone());
        session
    }

    pub fn close_session(&self, client_id: ByteString) -> Option<Session> {
        let mut inner = self.0.borrow_mut();
        inner.sessions.remove(&client_id)
    }

    pub fn subscribe(
        &self,
        client_id: ByteString,
        subscribe_to: Vec<(ByteString, QualityOfService)>,
    ) -> Option<Vec<Result<QualityOfService, TopicError>>> {
        let mut inner = self.0.borrow_mut();

        inner.sessions.get_mut(&client_id).map(|session| {
            subscribe_to
                .into_iter()
                .map(|(topic, qos)| session.subscribe(&topic, qos))
                .collect()
        })
    }

    // pub fn filter(
    //     &self,
    //     topic: &str,
    //     qos: QualityOfService,
    // ) -> Vec<(QualityOfService, SessionLite)> {
    //     self.0
    //         .borrow()
    //         .sessions
    //         .values()
    //         .filter_map(|session| {
    //             session
    //                 .filter(topic, qos)
    //                 .map(|qos| (qos, SessionLite::from_session(session)))
    //         })
    //         .collect()
    // }

    pub fn publish(&self, publication: Publication) {
        let inner = self.0.borrow();

        // notify all other managers
        if inner.sender.send(publication.clone()).is_err() {
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

// pub struct SessionManagerWorker {
//     sender: UnboundedSender<SessionRequest>,
//     commands: UnboundedReceiver<SessionRequest>,
// }

// impl Default for SessionManagerWorker {
//     fn default() -> Self {
//         let (tx, rx) = mpsc::unbounded_channel();
//         Self {
//             sender: tx,
//             commands: rx,
//         }
//     }
// }

// impl SessionManagerWorker {
//     pub fn new() -> Self {
//         Self::default()
//     }

//     pub fn handle(&self) -> SessionManagerHandle {
//         SessionManagerHandle(self.sender.clone())
//     }

//     pub async fn run(mut self) {
//         let sessions = SessionManager::new();

//         while let Some(cmd) = self.commands.next().await {
//             match cmd {
//                 SessionRequest::OpenSession {
//                     client_id,
//                     sender,
//                     ack,
//                 } => {
//                     let session = sessions.open_session(client_id, sender);
//                     let session = SessionLite::from_session(&session);
//                     if ack.send(session).is_err() {
//                         log::error!("open_session cannot be sent",)
//                     }
//                 }
//                 SessionRequest::CloseSession { client_id, ack } => {
//                     let session = sessions.close_session(client_id);
//                     let session = session.map(|session| SessionLite::from_session(&session));
//                     if ack.send(session).is_err() {
//                         log::error!("close_session cannot be sent");
//                     }
//                 }
//                 SessionRequest::Subscribe {
//                     client_id,
//                     subscribe_to,
//                     ack,
//                 } => {
//                     let acks = sessions.subscribe(client_id, subscribe_to);
//                     if ack.send(acks).is_err() {
//                         log::error!("subscribe cannot be sent");
//                     }
//                 }
//                 SessionRequest::Filter { topic, qos, ack } => {
//                     let senders = sessions.filter(&topic, qos);
//                     if ack.send(senders).is_err() {
//                         log::error!("filter cannot be sent");
//                     }
//                 }
//             }
//         }
//     }
// }

// #[derive(Clone)]
// pub struct SessionManagerHandle(UnboundedSender<SessionRequest>);

// impl SessionManagerHandle {
//     pub async fn open_session(
//         &self,
//         client_id: ByteString,
//         sender: UnboundedSender<Publication>,
//     ) -> SessionLite {
//         let (ack, rx) = oneshot::channel();

//         let req = SessionRequest::OpenSession {
//             client_id,
//             sender,
//             ack,
//         };
//         self.0.send(req);

//         rx.await.unwrap()
//     }

//     pub async fn close_session(&self, client_id: ByteString) -> Option<SessionLite> {
//         let (ack, rx) = oneshot::channel();

//         self.0.send(SessionRequest::CloseSession { client_id, ack });

//         rx.await.unwrap()
//     }

//     pub async fn subscribe(
//         &self,
//         client_id: ByteString,
//         subscribe_to: Vec<(ByteString, QualityOfService)>,
//     ) -> Option<Vec<Result<QualityOfService, TopicError>>> {
//         let (ack, rx) = oneshot::channel();

//         self.0.send(SessionRequest::Subscribe {
//             client_id,
//             subscribe_to,
//             ack,
//         });

//         rx.await.unwrap()
//     }

//     pub async fn filter(
//         &self,
//         topic: &str,
//         qos: QualityOfService,
//     ) -> Vec<(QualityOfService, SessionLite)> {
//         let (ack, rx) = oneshot::channel();

//         let topic = topic.into();
//         self.0.send(SessionRequest::Filter { topic, qos, ack });

//         rx.await.unwrap()
//     }
// }

// enum SessionRequest {
//     OpenSession {
//         client_id: ByteString,
//         sender: UnboundedSender<Publication>,
//         ack: oneshot::Sender<SessionLite>,
//     },
//     CloseSession {
//         client_id: ByteString,
//         ack: oneshot::Sender<Option<SessionLite>>,
//     },
//     Subscribe {
//         client_id: ByteString,
//         subscribe_to: Vec<(ByteString, QualityOfService)>,
//         ack: oneshot::Sender<Option<Vec<Result<QualityOfService, TopicError>>>>,
//     },
//     Filter {
//         topic: String,
//         qos: QualityOfService,
//         ack: oneshot::Sender<Vec<(QualityOfService, SessionLite)>>,
//     },
// }

// pub struct SessionLite {
//     client_id: ByteString,
//     sender: UnboundedSender<Publication>,
// }

// impl SessionLite {
//     pub fn new(client_id: ByteString, sender: UnboundedSender<Publication>) -> Self {
//         Self { client_id, sender }
//     }

//     pub fn from_session(session: &Session) -> Self {
//         Self::new(session.client_id(), session.sender())
//     }

//     pub fn client_id(&self) -> ByteString {
//         self.client_id.clone()
//     }

// pub fn publish(&self, publication: Publication) -> Result<(), SendError<Publication>> {
//     self.sender.send(publication)
// }
// }
