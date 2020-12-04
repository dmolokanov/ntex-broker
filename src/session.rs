use std::{cmp, sync::Arc};

use bytestring::ByteString;
use fxhash::FxHashMap;
use ntex_mqtt::{v3::codec::TopicError, Topic};
use parking_lot::RwLock;
use tokio::sync::mpsc::UnboundedSender;

use crate::{Publication, QualityOfService};

#[derive(Debug, Clone)]
struct SessionInner {
    client_id: ByteString,
    subscriptions: FxHashMap<ByteString, Subscription>,
    sender: UnboundedSender<Publication>,
}

#[derive(Debug, Clone)]
pub struct Session(Arc<RwLock<SessionInner>>);

impl Session {
    pub fn new(client_id: ByteString, sender: UnboundedSender<Publication>) -> Self {
        Self(Arc::new(RwLock::new(SessionInner {
            client_id,
            sender,
            subscriptions: Default::default(),
        })))
    }

    pub fn client_id(&self) -> ByteString {
        self.0.read().client_id.clone()
    }

    pub fn subscribe(
        &self,
        filter: &ByteString,
        qos: QualityOfService,
    ) -> Result<QualityOfService, TopicError> {
        let mut inner = self.0.write();
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
            .read()
            .subscriptions
            .values()
            .filter(|sub| sub.matches(topic))
            .fold(None, |acc, sub| {
                acc.map(|qos| cmp::max(qos, cmp::min(sub.max_qos(), publication_qos)))
                    .or_else(|| Some(cmp::min(sub.max_qos(), publication_qos)))
            })
    }

    pub fn publish(&self, publication: Publication) {
        if self.0.read().sender.send(publication).is_err() {
            log::error!("Unable to dispatch publication. Receiving part is closed");
        }
    }
}

#[derive(Debug)]
struct SubscriptionInner {
    topic: Topic,
    max_qos: QualityOfService,
}

#[derive(Debug, Clone)]
pub struct Subscription(Arc<SubscriptionInner>);

impl Subscription {
    pub fn new(topic: Topic, max_qos: QualityOfService) -> Self {
        Self(Arc::new(SubscriptionInner { topic, max_qos }))
    }

    pub fn max_qos(&self) -> QualityOfService {
        self.0.max_qos
    }

    pub fn matches(&self, topic: &str) -> bool {
        self.0.topic.matches_str(topic)
    }
}

struct SessionManagerInner {
    sessions: FxHashMap<ByteString, Session>,
}

#[derive(Clone)]
pub struct SessionManager(Arc<RwLock<SessionManagerInner>>);

impl SessionManager {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(SessionManagerInner {
            sessions: Default::default(),
        })))
    }

    pub fn open_session(
        &self,
        client_id: ByteString,
        sender: UnboundedSender<Publication>,
    ) -> Session {
        let mut inner = self.0.write();
        let _existing = inner.sessions.remove(&client_id);
        // let _existing = sessions.remove(client_id);

        let session = Session::new(client_id.clone(), sender);
        inner.sessions.insert(client_id, session.clone());
        session
    }

    pub fn close_session(&self, client_id: ByteString) -> Option<Session> {
        let mut inner = self.0.write();
        inner.sessions.remove(&client_id)
    }

    pub fn subscribe(
        &self,
        client_id: ByteString,
        subscribe_to: Vec<(ByteString, QualityOfService)>,
    ) -> Option<Vec<Result<QualityOfService, TopicError>>> {
        let mut inner = self.0.write();

        inner.sessions.get_mut(&client_id).map(|session| {
            subscribe_to
                .into_iter()
                .map(|(topic, qos)| session.subscribe(&topic, qos))
                .collect()
        })
    }

    pub fn publish(&self, publication: Publication) {
        self.dispatch(publication);
    }

    pub fn dispatch(&self, publication: Publication) {
        // filter local subscriptions that matched topic
        let inner = self.0.read();
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
