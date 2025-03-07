use cs271_final::utils::constants::{PROXY_INSTANCE_ID, PROXY_PORT};
use cs271_final::utils::event::{Event, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

fn main() {
    env_logger::init();
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(PROXY_INSTANCE_ID, sender.clone(), true);
    let config_map = Arc::new(Mutex::new(load_config("config.txt")));
    let config_map_clone = config_map.clone();
    network.listen_for_instances(PROXY_PORT);
    thread::spawn(move || {
        handle_events(network, receiver, config_map_clone);
    });
    println!("Proxy started!");

    loop {
        update_config(&config_map, "config.txt");
        thread::sleep(Duration::from_millis(5000));
    }
}

fn load_config(file_path: &str) -> HashMap<u64, Vec<usize>> {
    let content = fs::read_to_string(file_path).expect("Failed to read config file");
    let mut config_map: HashMap<u64, Vec<usize>> = HashMap::new();
    for (partition_id, line) in content.lines().enumerate() {
        for server in line.split(',').filter_map(|s| s.trim().parse::<u64>().ok()) {
            if config_map.contains_key(&server) {
                config_map.get_mut(&server).unwrap().push(partition_id);
            } else {
                config_map.insert(server, vec![partition_id]);
            }
        }
    }
    config_map
}

fn update_config(config_map: &Arc<Mutex<HashMap<u64, Vec<usize>>>>, file_path: &str) {
    let new_config = load_config(file_path);
    let mut guard = config_map.lock().unwrap();
    *guard = new_config;
    println!("Configuration updated.");
}

fn has_intersection(vec1: &Vec<usize>, vec2: &Vec<usize>) -> bool {
    let set1: HashSet<_> = vec1.iter().collect();
    vec2.iter().any(|x| set1.contains(x))
}

fn handle_events(
    mut network: Network,
    receiver: Receiver<Event>,
    config_map: Arc<Mutex<HashMap<u64, Vec<usize>>>>,
) {
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(_) => {
                        println!("Handling local event");
                        // Handle local events if any
                    }
                    Event::Network(message) => {
                        println!(
                            "Handling network event from {} to {}",
                            message.from, message.to
                        );
                        let config = config_map.lock().unwrap();
                        match (config.get(&message.from), config.get(&message.to)) {
                            (Some(from_partitions), Some(to_partitions)) => {
                                if has_intersection(from_partitions, to_partitions) {
                                    println!(
                                        "Relaying message from {} to {}",
                                        message.from, message.to
                                    );
                                    network.send_message(message);
                                } else {
                                    println!(
                                        "Dropping message from {} to {} due to partitioning",
                                        message.from, message.to
                                    );
                                }
                            }
                            (Some(_), None) => {
                                println!(
                                    "Server {} not found, notifying client {}",
                                    message.to, message.from
                                );
                                network.send_message(NetworkEvent {
                                    from: PROXY_INSTANCE_ID,
                                    to: message.from,
                                    payload: NetworkPayload::ServerNotFound {
                                        instance: message.to,
                                    }
                                    .serialize(),
                                });
                            }
                            _ => {
                                println!("Dropping message from {} to {}: invalid sender or both instances not found", message.from, message.to);
                            }
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
