#![type_length_limit = "2870573"]

use bytes::Bytes;
use bytestring::ByteString;
use futures_util::{future::ok, FutureExt, SinkExt, StreamExt};
use ntex_broker::{v3, SessionManager};
use ntex_codec::Framed;
use ntex_mqtt::v3::client;

#[ntex::test]
async fn connect_transient() {
    let (tx, _) = tokio::sync::broadcast::channel(10_0000);

    let srv = ntex::server::test_server(move || {
        let sessions = SessionManager::new(tx.clone());
        v3::server(sessions).finish()
    });

    // connect with clean session
    let client = client::MqttConnector::new(srv.addr())
        .client_id("client1")
        .clean_session()
        .connect()
        .await
        .unwrap();

    assert!(!client.session_present());

    // let sink = client.sink();

    // ntex::rt::spawn(client.start_default());

    // let res = sink
    //     .publish(ByteString::from_static("#"), Bytes::new())
    //     .send_at_least_once()
    //     .await;
    // assert!(res.is_ok());

    // sink.close();
}

#[ntex::test]
async fn connect_persistent() {
    let (tx, _) = tokio::sync::broadcast::channel(10_0000);

    let srv = ntex::server::test_server(move || {
        let sessions = SessionManager::new(tx.clone());
        v3::server(sessions).finish()
    });

    // connect with persistent session
    // let io = srv.connect().unwrap();
    // let mut framed = Framed::new(io, ntex_mqtt::v3::codec::Codec::default());
    // framed
    //     .send(ntex_mqtt::v3::codec::Packet::Connect(
    //         ntex_mqtt::v3::codec::Connect::default().client_id("user"),
    //     ))
    //     .await
    //     .unwrap();
    // let connack = framed.next().await.unwrap().unwrap();
    // dbg!(connack);
    let client = client::MqttConnector::new(srv.addr())
        .client_id("client1")
        .connect()
        .await
        .unwrap();

    assert!(!client.session_present());

    // reconnect and expect session present
    client.sink().close();
    let client = client::MqttConnector::new(srv.addr())
        .client_id("client1")
        .connect()
        .await
        .unwrap();

    assert!(client.session_present());

    // let sink = client.sink();

    // ntex::rt::spawn(client.start_default());

    // let res = sink
    //     .publish(ByteString::from_static("#"), Bytes::new())
    //     .send_at_least_once()
    //     .await;
    // assert!(res.is_ok());

    // sink.close();
}

// #[ntex::test]
// async fn test_ack_order() -> std::io::Result<()> {
//     let srv = server::test_server(move || {
//         MqttServer::new(connect)
//             .publish(|_| delay_for(Duration::from_millis(100)).map(|_| Ok::<_, ()>(())))
//             .control(move |msg| match msg {
//                 ControlMessage::Subscribe(mut msg) => {
//                     for mut sub in &mut msg {
//                         assert_eq!(sub.qos(), codec::QoS::AtLeastOnce);
//                         sub.topic();
//                         sub.subscribe(codec::QoS::AtLeastOnce);
//                     }
//                     ok(msg.ack())
//                 }
//                 _ => ok(msg.disconnect()),
//             })
//             .finish()
//     });

//     let io = srv.connect().unwrap();
//     let mut framed = Framed::new(io, codec::Codec::default());
//     framed
//         .send(codec::Packet::Connect(
//             codec::Connect::default().client_id("user"),
//         ))
//         .await
//         .unwrap();
//     let _ = framed.next().await.unwrap().unwrap();

//     framed
//         .send(
//             codec::Publish {
//                 dup: false,
//                 retain: false,
//                 qos: codec::QoS::AtLeastOnce,
//                 topic: ByteString::from("test"),
//                 packet_id: Some(NonZeroU16::new(1).unwrap()),
//                 payload: Bytes::new(),
//             }
//             .into(),
//         )
//         .await
//         .unwrap();
//     framed
//         .send(codec::Packet::Subscribe {
//             packet_id: NonZeroU16::new(2).unwrap(),
//             topic_filters: vec![(ByteString::from("topic1"), codec::QoS::AtLeastOnce)],
//         })
//         .await
//         .unwrap();

//     let pkt = framed.next().await.unwrap().unwrap();
//     assert_eq!(
//         pkt,
//         codec::Packet::PublishAck {
//             packet_id: NonZeroU16::new(1).unwrap()
//         }
//     );

//     let pkt = framed.next().await.unwrap().unwrap();
//     assert_eq!(
//         pkt,
//         codec::Packet::SubscribeAck {
//             packet_id: NonZeroU16::new(2).unwrap(),
//             status: vec![codec::SubscribeReturnCode::Success(codec::QoS::AtLeastOnce)],
//         }
//     );

//     Ok(())
// }
