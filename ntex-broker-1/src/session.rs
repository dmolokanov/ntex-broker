use std::{cell::RefCell, cmp, rc::Rc};

use bytes::Bytes;
use bytestring::ByteString;
use fxhash::FxHashMap;
use ntex::channel::mpsc::{SendError, Sender};
use ntex_mqtt::{v3::codec::TopicError, Topic};

struct MqttSessionInner {
    client_id: ByteString,
    subscriptions: FxHashMap<ByteString, Subscription>,
    sender: Sender<Publication>,
}

#[derive(Clone)]
pub struct MqttSession(Rc<RefCell<MqttSessionInner>>);

impl MqttSession {
    pub fn new(client_id: &ByteString, sender: Sender<Publication>) -> Self {
        Self(Rc::new(RefCell::new(MqttSessionInner {
            client_id: client_id.clone(),
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
    ) -> Result<bool, TopicError> {
        let mut inner = self.0.borrow_mut();
        if inner.subscriptions.contains_key(filter) {
            Ok(false)
        } else {
            let topic = filter.parse()?;
            let subscription = Subscription::new(topic, qos);
            inner.subscriptions.insert(filter.clone(), subscription);

            Ok(true)
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

    pub fn publish(&self, publication: Publication) -> Result<(), SendError<Publication>> {
        self.0.borrow_mut().sender.send(publication)
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

pub struct Publication {
    topic: ByteString,
    qos: QualityOfService,
    retain: bool,
    payload: Bytes,
}

impl Publication {
    pub fn new(topic: &ByteString, qos: QualityOfService, retain: bool, payload: &Bytes) -> Self {
        Self {
            topic: topic.clone(),
            qos,
            retain,
            payload: payload.clone(),
        }
    }

    // pub fn topic(&self) -> &ByteString {
    //     &self.topic
    // }

    // pub fn qos(&self) -> &ByteString {
    //     &self.topic
    // }

    // pub fn from_publish(publish: &Publish) -> Self {
    //     Self {
    //         topic: publish.packet().topic.clone(),
    //         qos: publish.qos(),
    //         retain: publish.retain(),
    //         payload: publish.payload().clone(),
    //     }
    // }

    pub fn into_parts(self) -> (ByteString, QualityOfService, bool, Bytes) {
        (self.topic, self.qos, self.retain, self.payload)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum QualityOfService {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    // ExactlyOnce = 2,
}

#[derive(Default)]
struct SessionManagerInner {
    sessions: FxHashMap<ByteString, MqttSession>,
}

#[derive(Clone)]
pub struct SessionManager(Rc<RefCell<SessionManagerInner>>);

impl SessionManager {
    pub fn new() -> Self {
        Self(Rc::new(RefCell::new(SessionManagerInner::default())))
    }

    pub fn open_session(&self, client_id: &ByteString, sender: Sender<Publication>) -> MqttSession {
        let mut inner = self.0.borrow_mut();
        let _existing = inner.sessions.remove(client_id);
        // let _existing = sessions.remove(client_id);

        let session = MqttSession::new(client_id, sender);
        inner.sessions.insert(client_id.clone(), session.clone());
        session
    }

    pub fn close_session(&self, client_id: &ByteString) -> Option<MqttSession> {
        let mut inner = self.0.borrow_mut();
        inner.sessions.remove(client_id)
    }

    pub fn filter(
        &self,
        topic: &str,
        qos: QualityOfService,
    ) -> Vec<(QualityOfService, MqttSession)> {
        self.0
            .borrow()
            .sessions
            .values()
            .filter_map(|session| session.filter(topic, qos).map(|qos| (qos, session.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_opens_session() {
        let sessions = SessionManager::new();
        assert!(false);
    }
}
