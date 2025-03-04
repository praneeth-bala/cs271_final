use cs271_final::utils::constants::{PROXY_INSTANCE_ID, PROXY_PORT};
use cs271_final::utils::event::Event;
use cs271_final::utils::network::Network;

use std::sync::mpsc::{self, Receiver, Sender};
use std::{thread, time::Duration};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs;

fn main() {
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(PROXY_INSTANCE_ID, sender.clone(), true);
    let config_map = Arc::new(Mutex::new(load_config("config.txt")));
    let config_map_clone = config_map.clone();
    network.listen_for_instances(PROXY_PORT);
    thread::spawn(move || {
        handle_events(
            network,
            receiver,
            config_map_clone
        );
    });
    println!("Proxy started!");

    loop {
        update_config(&config_map, "config.txt");
        thread::sleep(Duration::from_millis(5000));
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

fn handle_events(mut network: Network, receiver: Receiver<Event>, config_map: Arc<Mutex<HashMap<u64, usize>>>){
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(_) => {
                        println!("Handling local event");
                        // Handle local events if any
                    }
                    Event::Network(message) => {
                        println!("Handling network event from {} to {}", message.from, message.to);
                        let config = config_map.lock().unwrap();
                        if config.get(&message.from) == config.get(&message.to) {
                            println!("Relaying message from {} to {}", message.from, message.to);
                            network.send_message(message);
                        } else {
                            println!("Dropping message from {} to {} due to partitioning", message.from, message.to);
                        }
                    }
                }
            }
            Err(_) => {
                println!("mpsc channel closed");
                break;
            }
        }
    }
}
