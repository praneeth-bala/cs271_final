use cs271_final::utils::constants::{CLIENT_INSTANCE_ID, PROXY_PORT};
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::{Event, LocalEvent, LocalPayload, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;
use rand::Rng;

use std::sync::mpsc::{self, Receiver, Sender};
use std::{io, process, thread, time::Duration};

fn main() {
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(CLIENT_INSTANCE_ID, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) {
        eprintln!("Failed to connect to proxy. Exiting...");
        process::exit(1);
    }
    thread::spawn(move || {
        handle_events(network, receiver);
    });

    println!("Client started!");

    loop {
        println!("\nMenu Options:");
        println!("1. Print Balance");
        println!("2. Print Datastore");
        println!("3. Transfer");
        println!("4. Exit");

        let mut choice = String::new();
        io::stdin().read_line(&mut choice).unwrap_or_default();
        let choice = choice.trim();

        match choice {
            "1" => {
                println!("Enter client ID:");
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap_or_default();

                if let Ok(id) = id_input.trim().parse::<u64>() {
                    if id > 0 && id <= 3000 { // Validate client ID range
                        sender
                            .send(Event::Local(LocalEvent {
                                payload: LocalPayload::PrintBalance { id },
                            }))
                            .unwrap_or_else(|e| eprintln!("Failed to send print-balance event: {}", e));
                    } else {
                        println!("Invalid client ID. Please enter a number between 1 and 3000.");
                    }
                } else {
                    println!("Invalid client ID. Please enter a valid number.");
                }
            }
            "2" => {
                println!("Enter server ID (1-9):");
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap_or_default();

                if let Ok(instance) = id_input.trim().parse::<u64>() {
                    if instance >= 1 && instance <= 9 { // Validate server ID range
                        sender
                            .send(Event::Local(LocalEvent {
                                payload: LocalPayload::PrintDatastore { instance },
                            }))
                            .unwrap_or_else(|e| eprintln!("Failed to send print-datastore event: {}", e));
                    } else {
                        println!("Invalid server ID. Please enter a number between 1 and 9.");
                    }
                } else {
                    println!("Invalid server ID. Please enter a valid number.");
                }
            }
            "3" => {
                println!("Enter sender ID (1-3000):");
                let mut from_input = String::new();
                io::stdin().read_line(&mut from_input).unwrap_or_default();
                let from = from_input.trim().parse::<u64>();

                println!("Enter receiver ID (1-3000):");
                let mut to_input = String::new();
                io::stdin().read_line(&mut to_input).unwrap_or_default();
                let to = to_input.trim().parse::<u64>();

                println!("Enter amount (positive integer):");
                let mut amount_input = String::new();
                io::stdin().read_line(&mut amount_input).unwrap_or_default();
                let amount = amount_input.trim().parse::<i64>();

                match (from, to, amount) {
                    (Ok(from), Ok(to), Ok(amount)) if from > 0 && to > 0 && from <= 3000 && to <= 3000 && amount > 0 => {
                        let from_instances = DataStore::get_all_instances_from_id(from);
                        let to_instances = DataStore::get_all_instances_from_id(to);
                        if from_instances == to_instances {
                            sender
                                .send(Event::Local(LocalEvent {
                                    payload: LocalPayload::Transfer { from, to, amount },
                                }))
                                .unwrap_or_else(|e| eprintln!("Failed to send transfer event: {}", e));
                        } else {
                            println!("Cross-shard transaction detected. Please ensure sender and receiver are in the same cluster (1-1000, 1001-2000, or 2001-3000).");
                        }
                    }
                    _ => println!("Invalid input. Please enter valid positive numbers for all fields."),
                }
            }
            "4" => {
                println!("Exiting...");
                break;
            }
            _ => println!("Invalid choice. Please select a number between 1 and 4."),
        }
        thread::sleep(Duration::from_millis(100)); // Add small delay to prevent overwhelming the network
    }
}

fn handle_events(mut network: Network, receiver: Receiver<Event>) {
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(message) => {
                        // println!("Handling local event: {:?}", message.payload);
                        match message.payload {
                            LocalPayload::PrintBalance { id } => {
                                let instances = DataStore::get_all_instances_from_id(id);
                                for instance in instances {
                                    network.send_message(NetworkEvent {
                                        from: CLIENT_INSTANCE_ID,
                                        to: instance,
                                        payload: NetworkPayload::PrintBalance { id }.serialize(),
                                    });
                                }
                            }
                            LocalPayload::PrintDatastore { instance } => {
                                network.send_message(NetworkEvent {
                                    from: CLIENT_INSTANCE_ID,
                                    to: instance,
                                    payload: NetworkPayload::PrintDatastore.serialize(),
                                });
                            }
                            LocalPayload::Transfer { from, to, amount } => {
                                let from_instances = DataStore::get_all_instances_from_id(from);
                                let to_instances = DataStore::get_all_instances_from_id(to);
                                if from_instances == to_instances {
                                    // Intra-shard: send to a random server in the cluster
                                    let instance = from_instances[rand::thread_rng().gen_range(0..from_instances.len())];
                                    network.send_message(NetworkEvent {
                                        from: CLIENT_INSTANCE_ID,
                                        to: instance,
                                        payload: NetworkPayload::Transfer { from, to, amount }.serialize(),
                                    });
                                } else {
                                    println!("Cross-shard transaction detected, implement 2PC instead");
                                }
                            }
                        }
                    }
                    Event::Network(message) => {
                        println!("Handling network event from {} to {}", message.from, message.to);
                        // Optionally, parse and display server responses (e.g., balances or transaction results)
                        if let Ok(payload) = NetworkPayload::deserialize(message.payload) {
                            match payload {
                                NetworkPayload::PrintBalance { id } => {
                                    println!("Balance request for client {} received from server {}", id, message.from);
                                }
                                NetworkPayload::PrintDatastore => {
                                    println!("Datastore request received from server {}", message.from);
                                }
                                _ => {}
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