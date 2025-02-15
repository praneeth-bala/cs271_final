use cs271_final::utils::network::Network;
use cs271_final::utils::network::Message;

use std::sync::mpsc::{self, Receiver, Sender};
use std::{thread,io};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs;

fn main() {
    // Proxy is instance 0
    let (sender, receiver): (Sender<Message>, Receiver<Message>) = mpsc::channel();
    let mut network = Network::new(0, sender.clone(), true);
    let config_map = Arc::new(Mutex::new(load_config("config.txt")));
    let config_map_clone = config_map.clone();
    network.listen_for_instances(3000);
    thread::spawn(move || {
        handle_incoming_events(
            network,
            receiver,
            config_map_clone
        );
    });
    println!("Proxy started!");

    // Just update config for any input
    let mut input = String::new();
    loop {
        io::stdin().read_line(&mut input).unwrap();
        update_config(&config_map, "config.txt");
    }
}

fn load_config(file_path: &str) -> HashMap<u64, usize> {
    let content = fs::read_to_string(file_path).expect("Failed to read config file");
    let mut config_map = HashMap::new();
    for (partition_id, line) in content.lines().enumerate() {
        for server in line.split(',').filter_map(|s| s.trim().parse::<u64>().ok()) {
            config_map.insert(server, partition_id);
        }
    }
    config_map
}

fn update_config(config_map: &Arc<Mutex<HashMap<u64, usize>>>, file_path: &str) {
    let new_config = load_config(file_path);
    let mut guard = config_map.lock().unwrap();
    *guard = new_config;
    println!("Configuration updated.");
}

fn handle_incoming_events(mut network: Network, receiver: Receiver<Message>, config_map: Arc<Mutex<HashMap<u64, usize>>>){
    loop {
        match receiver.recv() {
            Ok(message) => {
                let config = config_map.lock().unwrap();
                if config.get(&message.from) == config.get(&message.to) {
                    println!("Relaying message from {} to {}", message.from, message.to);
                    network.send_message(message);
                } else {
                    println!("Dropping message from {} to {} due to partitioning", message.from, message.to);
                }
            }
            Err(_) => {
                println!("mpsc channel closed");
                break;
            }
        }
    }
}
