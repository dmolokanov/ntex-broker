mod session;

use bytes::Bytes;
use bytestring::ByteString;

pub use session::{Session, SessionManager};

#[derive(Clone)]
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
