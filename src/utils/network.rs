use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc::Sender, Arc, Mutex};
use std::thread;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub from: u64,
    pub to: u64,
    pub payload: Vec<u8>,
}

pub struct Network {
    instance_connection_map: Arc<Mutex<HashMap<u64, TcpStream>>>,
    sender: Sender<Message>,
    proxy: bool,
    instance_id: u64
}

impl Network {
    pub fn new(instance_id: u64, sender: Sender<Message>, proxy: bool) -> Self {
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
                println!("Connected to proxy at {}", address);

                // Create and send the "hello" message
                let hello_message = Message {
                    from: self.instance_id,
                    to: 0,
                    payload: Vec::new(),
                };
                let serialized_message = serde_json::to_string(&hello_message).unwrap();
                if let Err(e) = stream.write_all(serialized_message.as_bytes()) {
                    println!("Failed to send hello message: {}", e);
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
                println!("Failed to connect to {}: {}", address, e);
                false
            }
        }
    }

    pub fn listen_for_instances(&mut self, port: u16) {
        let address = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&address).unwrap();
        println!("Listening on {}", address);
    
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
                                    if let Ok(message) = serde_json::from_slice::<Message>(&buffer[..bytes_read]) {
                                        let instance_id = message.from;
                                        map_clone.lock().unwrap().insert(instance_id, stream_clone.try_clone().unwrap());
                                        println!("Instance {} connected", instance_id);
                                        handle_connection(instance_id, stream_clone, sender_clone, map_clone);
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        println!("Connection failed: {}", e);
                    }
                }
            }
        });
    }

    pub fn send_message(&mut self, message: Message) {
        if message.to == self.instance_id {
            println!("Dropping message sent to myself");
            return;
        }
        let instance_id = if self.proxy { message.to } else { 0 };
        if let Some(stream) = self.instance_connection_map.lock().unwrap().get_mut(&instance_id) {
            let serialized_message = serde_json::to_string(&message).unwrap();
            if let Err(e) = stream.write_all(serialized_message.as_bytes()) {
                println!("Failed to send message to {}: {}", message.to, e);
                self.instance_connection_map.lock().unwrap().remove(&instance_id);
            } else {
                println!("Sent message to {}", message.to);
            }
        } else {
            println!("No peer found with id: {}", instance_id);
        }
    }
}

fn handle_connection(instance_id: u64, mut stream: TcpStream, sender: Sender<Message>, map: Arc<Mutex<HashMap<u64, TcpStream>>>) {
    let mut buffer = [0; 512];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Connection closed by peer {}", instance_id);
                map.lock().unwrap().remove(&instance_id);
                break;
            }
            Ok(bytes_read) => {
                if let Ok(message) = serde_json::from_slice::<Message>(&buffer[..bytes_read]) {
                    println!("Received message from {}", message.from);
                    sender.send(message).expect("Failed to send message to mpsc channel");
                }
            }
            Err(e) => {
                println!("Failed to read from stream: {} in instance {}", e, instance_id);
                map.lock().unwrap().remove(&instance_id);
                break;
            }
        }
    }
}