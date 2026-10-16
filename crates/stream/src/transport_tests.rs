use super::*;
use axum::{routing::post, Json};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message as Frame};

async fn start(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), task)
}

async fn admit(Json(request): Json<Value>) -> Response {
    let peer = request["runner_id"].as_str().unwrap_or_default();
    let action = request["action"].as_str().unwrap_or_default();
    let ticket = request["ticket"].as_str().unwrap_or_default();
    let generation = if ticket == format!("{action}-{peer}-new") {
        "9007199254740994"
    } else {
        "9007199254740993"
    };
    if ticket != format!("{action}-{peer}") && ticket != format!("{action}-{peer}-new") {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(json!({"kind":"screen","actor_id":"owner","channel_id":"","owner_id":"owner","device_id":"device","runner_id":peer,"generation":generation,"source_id":"screen","action":action})).into_response()
}

fn stamped(body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0; 8];
    bytes.extend_from_slice(body);
    bytes
}

#[tokio::test]
async fn admission_fanout_and_disconnect_preserve_room_authority() {
    let (authority, authority_task) = start(
        Router::new()
            .route("/admit", post(admit))
            .route("/lease", post(|| async { StatusCode::NO_CONTENT })),
    )
    .await;
    let rooms: RoomMap = Arc::default();
    let state = AppState {
        rooms: rooms.clone(),
        admission: Admission::new(authority, "k".repeat(32)).unwrap(),
    };
    let (origin, server) = start(router(state)).await;
    let base = origin.replace("http://", "ws://");
    assert!(connect_async(format!("{base}/ws/view/peer?ticket=wrong"))
        .await
        .is_err());
    assert!(
        connect_async(format!("{base}/ws/view/peer?ticket=view-peer"))
            .await
            .is_err()
    );
    assert!(
        rooms.lock().await.is_empty(),
        "viewers must never allocate rooms"
    );
    let (mut publisher, _) = connect_async(format!("{base}/ws/stream/peer?ticket=publish-peer"))
        .await
        .unwrap();
    assert!(matches!(publisher.next().await, Some(Ok(Frame::Text(text))) if text == "ready"));
    let (mut duplicate, _) = connect_async(format!("{base}/ws/stream/peer?ticket=publish-peer"))
        .await
        .unwrap();
    let rejected = tokio::time::timeout(std::time::Duration::from_secs(2), duplicate.next())
        .await
        .unwrap();
    assert!(
        matches!(rejected, Some(Ok(Frame::Close(Some(frame)))) if frame.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy)
    );
    let vps = stamped(&[0, 0, 0, 1, 0x40, 1]);
    let idr = stamped(&[0, 0, 0, 1, 0x26, 1, 7]);
    publisher.send(Frame::Binary(vps)).await.unwrap();
    publisher.send(Frame::Binary(idr.clone())).await.unwrap();
    let (mut viewer, _) = connect_async(format!("{base}/ws/view/peer?ticket=view-peer"))
        .await
        .unwrap();
    let (mut second, _) = connect_async(format!("{base}/ws/view/peer?ticket=view-peer"))
        .await
        .unwrap();
    for viewer in [&mut viewer, &mut second] {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if let Some(Ok(Frame::Binary(frame))) = viewer.next().await {
                    if frame == idr {
                        break;
                    }
                }
            }
        })
        .await
        .expect("both viewers receive the keyframe");
    }
    assert!(
        connect_async(format!("{base}/ws/view/other?ticket=view-peer"))
            .await
            .is_err()
    );
    publisher.close(None).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if rooms.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        while let Some(Ok(Frame::Binary(_))) = viewer.next().await {}
    })
    .await
    .expect("publisher disconnect must remove its room and end viewing");
    assert!(
        connect_async(format!("{base}/ws/view/peer?ticket=view-peer"))
            .await
            .is_err(),
        "no stale warm-up cache survives disconnect"
    );
    server.abort();
    authority_task.abort();
}

#[tokio::test]
async fn missing_admission_configuration_never_enables_public_streaming() {
    assert!(Admission::new(String::new(), "k".repeat(32)).is_err());
    assert!(Admission::new("http://api/internal/media".into(), "dev-token".into()).is_err());
}

#[tokio::test]
async fn replacing_a_generation_closes_old_viewers_without_removing_new_room() {
    let (authority, authority_task) = start(
        Router::new()
            .route("/admit", post(admit))
            .route("/lease", post(|| async { StatusCode::NO_CONTENT })),
    )
    .await;
    let rooms: RoomMap = Arc::default();
    let (origin, server) = start(router(AppState {
        rooms: rooms.clone(),
        admission: Admission::new(authority, "k".repeat(32)).unwrap(),
    }))
    .await;
    let base = origin.replace("http://", "ws://");
    let (mut old, _) = connect_async(format!("{base}/ws/stream/peer?ticket=publish-peer"))
        .await
        .unwrap();
    let (mut viewer, _) = connect_async(format!("{base}/ws/view/peer?ticket=view-peer"))
        .await
        .unwrap();
    let (mut new, _) = connect_async(format!("{base}/ws/stream/peer?ticket=publish-peer-new"))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if rooms.lock().await.get("peer").unwrap().scope.generation == "9007199254740994" {
                break;
            }
            tokio::task::yield_now().await;
        }
        while let Some(Ok(frame)) = viewer.next().await {
            if matches!(frame, Frame::Close(_)) {
                break;
            }
        }
        while let Some(Ok(frame)) = old.next().await {
            if matches!(frame, Frame::Close(_)) {
                break;
            }
        }
    })
    .await
    .expect("superseded publisher and viewers must close promptly");
    assert_eq!(
        rooms.lock().await["peer"].scope.generation,
        "9007199254740994"
    );
    assert!(
        connect_async(format!("{base}/ws/view/peer?ticket=view-peer"))
            .await
            .is_err()
    );
    assert!(
        connect_async(format!("{base}/ws/view/peer?ticket=view-peer-new"))
            .await
            .is_ok()
    );
    new.close(None).await.unwrap();
    server.abort();
    authority_task.abort();
}
