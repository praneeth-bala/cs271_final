use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc::Sender, Arc, Mutex};
use log::trace;
use std::thread;

use super::event::{Event, NetworkEvent};

pub struct Network {
    instance_connection_map: Arc<Mutex<HashMap<u64, TcpStream>>>,
    sender: Sender<Event>,
    proxy: bool,
    instance_id: u64
}

impl Network {
    pub fn new(instance_id: u64, sender: Sender<Event>, proxy: bool) -> Self {
        Self {
            instance_connection_map: Arc::new(Mutex::new(HashMap::new())),
            sender,
            proxy,
            instance_id
        }
    }

    pub fn connect_to_proxy(&mut self, port: u16) -> bool {
        let address = format!("127.0.0.1:{}", port);
        match TcpStream::connect(&address) {
            Ok(mut stream) => {
                trace!("Connected to proxy at {}", address);

                // Create and send the "hello" message
                let hello_message = NetworkEvent {
                    from: self.instance_id,
                    to: 0,
                    payload: Vec::new(),
                };
                let serialized_message = serde_json::to_string(&hello_message).unwrap();
                if let Err(e) = stream.write_all(serialized_message.as_bytes()) {
                    trace!("Failed to send hello message: {}", e);
                    return false;
                }
                
                let stream_clone = stream.try_clone().expect("Failed to clone TcpStream");
                let sender_clone = self.sender.clone();
                let map_clone = Arc::clone(&self.instance_connection_map);
                thread::spawn(move || {
                    handle_connection(0, stream_clone, sender_clone, map_clone);
                });
                self.instance_connection_map.lock().unwrap().insert(0, stream);
                true
            }
            Err(e) => {
                trace!("Failed to connect to {}: {}", address, e);
                false
            }
        }
    }

    pub fn listen_for_instances(&mut self, port: u16) {
        let address = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&address).unwrap();
        trace!("Listening on {}", address);
    
        let sender_clone = self.sender.clone();
        let map_clone = Arc::clone(&self.instance_connection_map);
    
        // Spawn a new thread to handle incoming connections
        thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let mut stream_clone = stream.try_clone().expect("Failed to clone TcpStream");
                        let sender_clone = sender_clone.clone();
                        let map_clone = Arc::clone(&map_clone);
    
                        thread::spawn(move || {
                            let mut buffer = [0; 512];
                            if let Ok(bytes_read) = stream_clone.read(&mut buffer) {
                                if bytes_read > 0 {
                                    if let Ok(message) = serde_json::from_slice::<NetworkEvent>(&buffer[..bytes_read]) {
                                        let instance_id = message.from;
                                        map_clone.lock().unwrap().insert(instance_id, stream_clone.try_clone().unwrap());
                                        trace!("Instance {} connected", instance_id);
                                        handle_connection(instance_id, stream_clone, sender_clone, map_clone);
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        trace!("Connection failed: {}", e);
                    }
                }
            }
        });
    }

    pub fn send_message(&mut self, message: NetworkEvent) {
        if message.to == self.instance_id {
            trace!("Dropping message sent to myself");
            return;
        }
        let instance_id = if self.proxy { message.to } else { 0 };
        if let Some(stream) = self.instance_connection_map.lock().unwrap().get_mut(&instance_id) {
            let serialized_message = serde_json::to_vec(&message).unwrap();

            // Add length prefix (4 bytes, big-endian)
            let mut serialized_message_with_len = (serialized_message.len() as u32).to_be_bytes().to_vec();
            serialized_message_with_len.extend_from_slice(&serialized_message);

            if let Err(e) = stream.write_all(&serialized_message_with_len.as_slice()) {
                trace!("Failed to send message to {}: {}", message.to, e);
                self.instance_connection_map.lock().unwrap().remove(&instance_id);
            } else {
                trace!("Sent message to {}", message.to);
            }
        } else {
            trace!("No peer found with id: {}", instance_id);
        }
    }
}

fn handle_connection(instance_id: u64, mut stream: TcpStream, sender: Sender<Event>, map: Arc<Mutex<HashMap<u64, TcpStream>>>) {
    let mut buffer = Vec::new();
    let mut temp = [0; 1024]; // Temporary buffer for reads
    loop {
        match stream.read(&mut temp) {
            Ok(0) => {
                trace!("Connection closed by peer {}", instance_id);
                map.lock().unwrap().remove(&instance_id);
                break;
            }
            Ok(bytes_read) => {
                buffer.extend_from_slice(&temp[..bytes_read]);
                // Process messages in a loop
                while buffer.len() >= 4 {
                    // Get the length prefix
                    let length = u32::from_be_bytes(buffer[..4].try_into().unwrap()) as usize;
                    // Check if the full message is available
                    if buffer.len() < 4 + length {
                        break; // Wait for more data
                    }

                    // Extract the message
                    let message_bytes = buffer[4..4 + length].to_vec();
                    buffer.drain(..4 + length); // Remove processed message from buffer

                    // Deserialize and handle the message
                    match serde_json::from_slice::<NetworkEvent>(&message_bytes) {
                        Ok(message) => {
                            trace!("Received message from {}", message.from);
                            sender.send(Event::Network(message)).expect("Failed to send message to mpsc channel");
                        }
                        Err(e) => {
                            trace!("Failed to deserialize message: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                trace!("Failed to read from stream: {} in instance {}", e, instance_id);
                map.lock().unwrap().remove(&instance_id);
                break;
            }
        }
    }
}