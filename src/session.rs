use std::{cell::RefCell, cmp, rc::Rc};

use bytestring::ByteString;
use fxhash::FxHashMap;
use ntex::channel::mpsc::Sender;
use ntex_mqtt::{v3::codec::TopicError, Topic};
use tokio::sync::broadcast::Sender as BroadcastSender;

use crate::{Publication, QualityOfService};

struct SessionInner {
    client_id: ByteString,
    subscriptions: FxHashMap<ByteString, Subscription>,
    sender: Sender<Publication>,
}

#[derive(Clone)]
pub struct Session(Rc<RefCell<SessionInner>>);

impl Session {
    pub fn new(client_id: ByteString, sender: Sender<Publication>) -> Self {
        Self(Rc::new(RefCell::new(SessionInner {
            client_id,
            sender,
            subscriptions: Default::default(),
        })))
    }

    pub fn client_id(&self) -> ByteString {
        self.0.borrow().client_id.clone()
    }

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

    pub fn open_session(&self, client_id: ByteString, sender: Sender<Publication>) -> Session {
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
