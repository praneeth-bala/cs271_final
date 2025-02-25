use cs271_final::utils::constants::CLIENT_INSTANCE_ID;
use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::{Event, LocalEvent, LocalPayload, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;

use std::sync::mpsc::{self, Receiver, Sender};
use std::{io, process, thread};
use std::collections::HashMap;
use std::time::{Duration, Instant};

fn main() {
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(CLIENT_INSTANCE_ID, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) {
        process::exit(1);
    }
    thread::spawn(move || {
        handle_events(network, receiver);
    });

    println!("Client started!");

    let mut transaction_id_counter = 0;

    loop {
        println!("\nMenu Options:");
        println!("1. Print Balance");
        println!("2. Print Datastore");
        println!("3. Transfer");
        println!("4. Exit");

        let mut choice = String::new();
        io::stdin().read_line(&mut choice).unwrap();
        let choice = choice.trim();

        match choice {
            "1" => {
                println!("Enter client ID:");
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap();

                if let Ok(id) = id_input.trim().parse::<u64>() {
                    sender
                        .send(Event::Local(LocalEvent {
                            payload: LocalPayload::PrintBalance { id },
                        }))
                        .expect("Failed to send print-balance event");
                } else {
                    println!("Invalid client ID. Please enter a valid number.");
                }
            }
            "2" => {
                println!("Enter server ID:");
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap();

                if let Ok(instance) = id_input.trim().parse::<u64>() {
                    sender
                        .send(Event::Local(LocalEvent {
                            payload: LocalPayload::PrintDatastore { instance },
                        }))
                        .expect("Failed to send print-datastore event");
                } else {
                    println!("Invalid server ID. Please enter a valid number.");
                }
            }
            "3" => {
                println!("Enter sender ID:");
                let mut from_input = String::new();
                io::stdin().read_line(&mut from_input).unwrap();
                let from = from_input.trim().parse::<u64>();

                println!("Enter receiver ID:");
                let mut to_input = String::new();
                io::stdin().read_line(&mut to_input).unwrap();
                let to = to_input.trim().parse::<u64>();

                println!("Enter amount:");
                let mut amount_input = String::new();
                io::stdin().read_line(&mut amount_input).unwrap();
                let amount = amount_input.trim().parse::<i64>();

                match (from, to, amount) {
                    (Ok(from), Ok(to), Ok(amount)) => {
                        transaction_id_counter += 1;
                        let transaction_id = transaction_id_counter;
                        sender
                            .send(Event::Local(LocalEvent {
                                payload: LocalPayload::Transfer { from, to, amount },
                            }))
                            .expect("Failed to send transfer event");

                        if from / 1000 != to / 1000 {
                            println!("Initiating 2PC for cross-shard transaction {}", transaction_id);
                            coordinate_2pc(transaction_id, from, to, amount, &sender);
                        }
                    }
                    _ => println!("Invalid input. Please enter valid numbers for all fields."),
                }
            }
            "4" => {
                println!("Exiting...");
                break;
            }
            _ => println!("Invalid choice. Please select a number between 1 and 4."),
        }
    }
}

fn coordinate_2pc(transaction_id: u64, from: u64, to: u64, amount: i64, sender: &Sender<Event>) {
    let from_cluster = DataStore::get_random_instance_from_id(from);
    let to_cluster = DataStore::get_random_instance_from_id(to);

    // Prepare all instances for 2PC (for commit/abort later)
    let from_instances = DataStore::get_all_instances_from_id(from);
    let to_instances = DataStore::get_all_instances_from_id(to);
    let mut all_instances = from_instances;
    all_instances.extend(to_instances.iter());

    // Send event to initialize pending_2pc in handle_events
    sender.send(Event::Local(LocalEvent {
        payload: LocalPayload::Transfer { from: transaction_id, to: 0, amount: 0 }, // Repurpose Transfer to signal 2PC start
    })).expect("Failed to signal 2PC start");

    // Send Prepare messages
    sender.send(Event::Network(NetworkEvent {
        from: CLIENT_INSTANCE_ID,
        to: from_cluster,
        payload: NetworkPayload::Prepare { transaction_id, from, to, amount }.serialize(),
    })).expect("Failed to send Prepare to from cluster");
    sender.send(Event::Network(NetworkEvent {
        from: CLIENT_INSTANCE_ID,
        to: to_cluster,
        payload: NetworkPayload::Prepare { transaction_id, from, to, amount }.serialize(),
    })).expect("Failed to send Prepare to to cluster");

    println!("Sent Prepare messages for transaction {}", transaction_id);
}

fn handle_events(mut network: Network, receiver: Receiver<Event>) {
    let mut pending_2pc: HashMap<u64, (Vec<u64>, usize, Instant, bool)> = HashMap::new(); // Added 'committed' flag
    let timeout_duration = Duration::from_secs(5); // 5-second timeout

    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(message) => {
                        println!("Handling local event");
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
                                if to == 0 && amount == 0 { // Special case to initialize 2PC
                                    let transaction_id = from; // Repurpose 'from' as transaction_id
                                    let from_id = DataStore::get_random_instance_from_id(transaction_id); // Temporary IDs for cluster lookup
                                    let to_id = transaction_id + 1000; // Arbitrary offset for testing
                                    let from_cluster = DataStore::get_all_instances_from_id(from_id);
                                    let to_cluster = DataStore::get_all_instances_from_id(to_id);
                                    let mut all_instances = from_cluster;
                                    all_instances.extend(to_cluster.iter());
                                    pending_2pc.insert(transaction_id, (all_instances, 0, Instant::now(), false));
                                    println!("Initialized 2PC state for transaction {}", transaction_id);
                                } else if from / 1000 == to / 1000 {
                                    network.send_message(NetworkEvent {
                                        from: CLIENT_INSTANCE_ID,
                                        to: DataStore::get_random_instance_from_id(from),
                                        payload: NetworkPayload::Transfer { from, to, amount }.serialize(),
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                    Event::Network(message) => {
                        println!("Handling network event from {} to {}", message.from, message.to);
                        let payload = NetworkPayload::deserialize(message.payload)
                            .expect("Failed to deserialize payload");
                        match payload {
                            NetworkPayload::PrepareResponse { transaction_id, success } => {
                                if let Some((instances, acks, start_time, committed)) = pending_2pc.get_mut(&transaction_id) {
                                    if *committed {
                                        continue; // Already processed
                                    }
                                    if success {
                                        *acks += 1;
                                        println!("Received PrepareResponse for transaction {}: {}/2 acks", transaction_id, *acks);
                                        if *acks == 2 {
                                            println!("All clusters prepared for transaction {}, committing", transaction_id);
                                            for instance in instances {
                                                network.send_message(NetworkEvent {
                                                    from: CLIENT_INSTANCE_ID,
                                                    to: *instance,
                                                    payload: NetworkPayload::Commit { transaction_id }.serialize(),
                                                });
                                            }
                                            *committed = true; // Mark as committed
                                            *acks = 0; // Reset for Ack counting
                                        }
                                    } else {
                                        println!("Cluster failed to prepare for transaction {}, aborting", transaction_id);
                                        for instance in instances {
                                            network.send_message(NetworkEvent {
                                                from: CLIENT_INSTANCE_ID,
                                                to: *instance,
                                                payload: NetworkPayload::Abort { transaction_id }.serialize(),
                                            });
                                        }
                                        pending_2pc.remove(&transaction_id);
                                    }
                                }
                            }
                            NetworkPayload::Ack { transaction_id, success } => {
                                if let Some((instances, acks, _, committed)) = pending_2pc.get_mut(&transaction_id) {
                                    if !*committed {
                                        continue; // Waiting for prepare phase
                                    }
                                    *acks += 1;
                                    println!("Received Ack for transaction {}: {}/{} acks", transaction_id, *acks, instances.len());
                                    if *acks == instances.len() {
                                        println!("Transaction {} fully completed with success: {}", transaction_id, success);
                                        pending_2pc.remove(&transaction_id);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }

                // Timeout check
                let now = Instant::now();
                let mut to_abort = Vec::new();
                for (&transaction_id, (instances, acks, start_time, committed)) in pending_2pc.iter() {
                    if now.duration_since(*start_time) > timeout_duration {
                        if !*committed && *acks < 2 {
                            println!("Timeout waiting for PrepareResponse for transaction {}, aborting", transaction_id);
                        } else if *committed && *acks < instances.len() {
                            println!("Timeout waiting for Acks for transaction {}, aborting remaining", transaction_id);
                        }
                        for instance in instances {
                            network.send_message(NetworkEvent {
                                from: CLIENT_INSTANCE_ID,
                                to: *instance,
                                payload: NetworkPayload::Abort { transaction_id }.serialize(),
                            });
                        }
                        to_abort.push(transaction_id);
                    }
                }
                for transaction_id in to_abort {
                    pending_2pc.remove(&transaction_id);
                }
            }
            Err(_) => {
                println!("mpsc channel closed");
                break;
            }
        }
    }
}
