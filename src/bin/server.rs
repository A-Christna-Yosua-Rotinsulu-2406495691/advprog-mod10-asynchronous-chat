use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::{Sender, channel};
use tokio::sync::Mutex;
use tokio_websockets::{Message, ServerBuilder, WebSocketStream};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
enum MsgTypes {
    Users,
    Register,
    Message,
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct WebSocketMessage {
    message_type: MsgTypes,
    data_array: Option<Vec<String>>,
    data: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct MessageData {
    from: String,
    message: String,
}

type ActiveUsers = Arc<Mutex<HashMap<SocketAddr, String>>>;

async fn handle_connection(
    addr: SocketAddr,
    mut ws_stream: WebSocketStream<TcpStream>,
    bcast_tx: Sender<String>,
    active_users: ActiveUsers,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut bcast_rx = bcast_tx.subscribe();

    loop {
        tokio::select! {
            incoming = ws_stream.next() => {
                match incoming {
                    Some(Ok(msg)) => {
                        if let Some(text) = msg.as_text() {
                            if let Ok(ws_msg) = serde_json::from_str::<WebSocketMessage>(text) {
                                match ws_msg.message_type {
                                    MsgTypes::Register => {
                                        if let Some(username) = ws_msg.data {
                                            println!("Registering user '{username}' for address {addr:?}");
                                            
                                            // Add to active users list
                                            let mut users = active_users.lock().await;
                                            users.insert(addr, username.clone());
                                            
                                            // Construct the updated user list
                                            let user_list: Vec<String> = users.values().cloned().collect();
                                            drop(users); // Release lock before broadcasting
                                            
                                            // Broadcast updated users list to all active clients
                                            let response = WebSocketMessage {
                                                message_type: MsgTypes::Users,
                                                data_array: Some(user_list),
                                                data: None,
                                            };
                                            let response_str = serde_json::to_string(&response).unwrap();
                                            bcast_tx.send(response_str).ok();
                                        }
                                    }
                                    MsgTypes::Message => {
                                        if let Some(message_text) = ws_msg.data {
                                            // Get the sender's username based on their SocketAddr
                                            let users = active_users.lock().await;
                                            let from = users.get(&addr).cloned().unwrap_or_else(|| "Unknown".to_string());
                                            drop(users);

                                            println!("Broadcasting message from '{from}': {message_text}");

                                            // Construct serialized MessageData
                                            let message_data = MessageData {
                                                from,
                                                message: message_text,
                                            };
                                            let message_data_str = serde_json::to_string(&message_data).unwrap();

                                            // Wrap in a standard WebSocketMessage
                                            let response = WebSocketMessage {
                                                message_type: MsgTypes::Message,
                                                data_array: None,
                                                data: Some(message_data_str),
                                            };
                                            let response_str = serde_json::to_string(&response).unwrap();
                                            bcast_tx.send(response_str).ok();
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    Some(Err(err)) => {
                        println!("Error receiving from {addr:?}: {err:?}");
                        break;
                    }
                    None => break,
                }
            }
            msg = bcast_rx.recv() => {
                match msg {
                    Ok(text) => {
                        if let Err(err) = ws_stream.send(Message::text(text)).await {
                            println!("Error sending to {addr:?}: {err:?}");
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        }
    }

    // Cleanup: Remove user when they disconnect
    let mut users = active_users.lock().await;
    if let Some(username) = users.remove(&addr) {
        println!("User '{username}' disconnected at address {addr:?}");
        let user_list: Vec<String> = users.values().cloned().collect();
        drop(users);

        // Broadcast the new online users list to all remaining active clients
        let response = WebSocketMessage {
            message_type: MsgTypes::Users,
            data_array: Some(user_list),
            data: None,
        };
        let response_str = serde_json::to_string(&response).unwrap();
        bcast_tx.send(response_str).ok();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let (bcast_tx, _) = channel(32);
    let active_users: ActiveUsers = Arc::new(Mutex::new(HashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("=== RUST WEBSOCKET SERVER FOR YEWCHAT RUNNING ===");
    println!("Listening for connections at ws://127.0.0.1:8080");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New TCP connection established from {addr:?}");
        
        let bcast_tx = bcast_tx.clone();
        let active_users = active_users.clone();
        
        tokio::spawn(async move {
            // Upgrade raw TCP stream into a WebSocket connection
            match ServerBuilder::new().accept(socket).await {
                Ok((_req, ws_stream)) => {
                    if let Err(e) = handle_connection(addr, ws_stream, bcast_tx, active_users).await {
                        eprintln!("Connection handling closed with error for {addr:?}: {:?}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to upgrade socket connection to WebSocket for {addr:?}: {:?}", e);
                }
            }
        });
    }
}