use cs271_final::utils::constants::ABORT_TIMEOUT;
use cs271_final::utils::constants::CLIENT_INSTANCE_ID;
use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::{Event, LocalEvent, LocalPayload, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;

use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use log::{info, trace};
use std::time::{Duration, Instant};
use std::{io, process, thread};

fn main() {
    env_logger::init();
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(CLIENT_INSTANCE_ID, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) {
        process::exit(1);
    }

    // Periodically check to abort pending transactions for cross shard
    let sender_clone = sender.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(ABORT_TIMEOUT));
        sender_clone
            .send(Event::Local(LocalEvent {
                payload: LocalPayload::CheckAbort,
            }))
            .unwrap();
    });

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
        println!("4. Performance");
        println!("5. Exit");

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
                                payload: LocalPayload::Transfer { from, to, amount, transaction_id },
                            }))
                            .expect("Failed to send transfer event");

                        if from / 1000 != to / 1000 {
                            println!(
                                "Initiating 2PC for cross-shard transaction {}",
                                transaction_id
                            );
                            coordinate_2pc(transaction_id, from, to, amount, &sender);
                        }
                    }
                    _ => println!("Invalid input. Please enter valid numbers for all fields."),
                }
            }
            "4" => {
                sender
                    .send(Event::Local(LocalEvent {
                        payload: LocalPayload::Start2PC {
                            transaction_id: 0,
                            from: 0,
                            to: 0,
                            amount: 0,
                        },
                    }))
                    .expect("Failed to send performance event");
            }
            "5" => {
                println!("Exiting...");
                break;
            }
            _ => println!("Invalid choice. Please select a number between 1 and 5."),
        }
    }
}

fn coordinate_2pc(transaction_id: u64, from: u64, to: u64, amount: i64, sender: &Sender<Event>) {
    let from_cluster = DataStore::get_random_instance_from_id(from);
    let to_cluster = DataStore::get_random_instance_from_id(to);

    let from_instances = DataStore::get_all_instances_from_id(from);
    let to_instances = DataStore::get_all_instances_from_id(to);
    let mut all_instances = from_instances;
    all_instances.extend(to_instances.iter());

    sender
        .send(Event::Local(LocalEvent {
            payload: LocalPayload::Start2PC {
                transaction_id,
                from,
                to,
                amount,
            },
        }))
        .expect("Failed to initialize 2PC state");

    // Send Prepare as local events, to be converted to network events in handle_events
    sender
        .send(Event::Local(LocalEvent {
            payload: LocalPayload::PrepareCluster {
                transaction_id,
                target: from_cluster,
                from,
                to,
                amount,
            },
        }))
        .expect("Failed to send Prepare to from cluster");
    sender
        .send(Event::Local(LocalEvent {
            payload: LocalPayload::PrepareCluster {
                transaction_id,
                target: to_cluster,
                from,
                to,
                amount,
            },
        }))
        .expect("Failed to send Prepare to to cluster");

    info!(
        "Sent Prepare messages for transaction {} to servers {} and {}",
        transaction_id, from_cluster, to_cluster
    );
}

fn handle_events(mut network: Network, receiver: Receiver<Event>) {
    let mut pending_2pc: HashMap<u64, (Vec<u64>, HashSet<u64>, Instant, bool)> = HashMap::new();
    let mut pending_raft: HashMap<u64, Instant> = HashMap::new();
    let mut completed_transactions: Vec<(u64, Duration)> = Vec::new();
    // let timeout_duration = Duration::from_secs(5);

    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(message) => {
                        trace!("Handling local event: {:?}", message.payload);
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
                            LocalPayload::Transfer { from, to, amount , transaction_id} => {
                                if from / 1000 == to / 1000 {
                                    pending_raft.insert(
                                        transaction_id,
                                        Instant::now(),
                                    );
                                    network.send_message(NetworkEvent {
                                        from: CLIENT_INSTANCE_ID,
                                        to: DataStore::get_random_instance_from_id(from),
                                        payload: NetworkPayload::Transfer { from, to, amount, transaction_id }
                                            .serialize(),
                                    });
                                }
                            }
                            LocalPayload::Start2PC {
                                transaction_id,
                                from,
                                to,
                                amount,
                            } => {
                                if transaction_id == 0 && from == 0 && to == 0 && amount == 0 {
                                    let transaction_count = completed_transactions.len() as f64;
                                    if transaction_count == 0.0 {
                                        println!("No transactions completed yet.");
                                        continue;
                                    }
                                    let total_time: Duration =
                                        completed_transactions.iter().map(|&(_, t)| t).sum();
                                    let avg_latency = total_time.as_secs_f64() / transaction_count;
                                    let throughput = transaction_count / total_time.as_secs_f64();
                                    println!("Performance Metrics:");
                                    println!(
                                        "Completed Transactions: {}",
                                        transaction_count as u64
                                    );
                                    println!("Average Latency: {:.3} seconds", avg_latency);
                                    println!("Throughput: {:.3} transactions/second", throughput);
                                } else {
                                    let from_instance =
                                        DataStore::get_random_instance_from_id(from);
                                    let to_instance = DataStore::get_random_instance_from_id(to);
                                    let all_instances = vec![from_instance, to_instance];
                                    pending_2pc.insert(
                                        transaction_id,
                                        (all_instances, HashSet::new(), Instant::now(), false),
                                    );
                                    info!(
                                        "Initialized 2PC state for transaction {}",
                                        transaction_id
                                    );
                                }
                            }
                            LocalPayload::PrepareCluster {
                                transaction_id,
                                target,
                                from,
                                to,
                                amount,
                            } => {
                                info!(
                                    "Sending Prepare to server {} for transaction {}",
                                    target, transaction_id
                                );
                                network.send_message(NetworkEvent {
                                    from: CLIENT_INSTANCE_ID,
                                    to: target,
                                    payload: NetworkPayload::Prepare {
                                        transaction_id,
                                        from,
                                        to,
                                        amount,
                                    }
                                    .serialize(),
                                });
                            }
                            LocalPayload::CheckAbort {} => {
                                for (transaction_id, (instances, _, instant, committed)) in pending_2pc.iter() {
                                    if instant.elapsed() > Duration::from_millis(ABORT_TIMEOUT) && !committed {
                                        for instance in instances {
                                            network.send_message(NetworkEvent {
                                                from: CLIENT_INSTANCE_ID,
                                                to: *instance,
                                                payload: NetworkPayload::Abort {
                                                    transaction_id: *transaction_id,
                                                }
                                                .serialize(),
                                            });
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Event::Network(message) => {
                        trace!(
                            "Handling network event from {} to {}",
                            message.from, message.to
                        );
                        match NetworkPayload::deserialize(message.payload) {
                            Ok(payload) => {
                                trace!("Deserialized payload: {:?}", payload);
                                match payload {
                                    NetworkPayload::PrepareResponse {
                                        transaction_id,
                                        success,
                                    } => {
                                        info!("Received PrepareResponse for transaction {}: success={}", transaction_id, success);
                                        if let Some((instances, acks, _, committed)) =
                                            pending_2pc.get_mut(&transaction_id)
                                        {
                                            if *committed {
                                                continue;
                                            }
                                            if success {
                                                if acks.contains(&message.from) {
                                                    continue;
                                                }
                                                acks.insert(message.from);
                                                info!(
                                                    "Transaction {} prepare acks: {}/2",
                                                    transaction_id,
                                                    acks.len()
                                                );
                                                if acks.len() == 2 {
                                                    info!("All clusters prepared for transaction {}, committing", transaction_id);
                                                    for instance in instances {
                                                        network.send_message(NetworkEvent {
                                                            from: CLIENT_INSTANCE_ID,
                                                            to: *instance,
                                                            payload: NetworkPayload::Commit {
                                                                transaction_id,
                                                            }
                                                            .serialize(),
                                                        });
                                                    }
                                                    *committed = true;
                                                    acks.clear();
                                                }
                                            } else {
                                                info!("Cluster failed to prepare for transaction {}, aborting", transaction_id);
                                                *committed = true;
                                                acks.clear();
                                                for instance in instances {
                                                    network.send_message(NetworkEvent {
                                                        from: CLIENT_INSTANCE_ID,
                                                        to: *instance,
                                                        payload: NetworkPayload::Abort {
                                                            transaction_id,
                                                        }
                                                        .serialize(),
                                                    });
                                                }
                                            }
                                        } else {
                                            info!(
                                                "No pending 2PC state for transaction {}",
                                                transaction_id
                                            );
                                        }
                                    }
                                    NetworkPayload::Ack {
                                        transaction_id,
                                        success,
                                    } => {
                                        info!(
                                            "Received Ack for transaction {}: success={}",
                                            transaction_id, success
                                        );
                                        if let Some((instances, acks, start_time, _)) =
                                            pending_2pc.get_mut(&transaction_id)
                                        {
                                            acks.insert(message.from);
                                            info!(
                                                "Transaction {} acks: {}/{}",
                                                transaction_id,
                                                acks.len(),
                                                instances.len()
                                            );
                                            if acks.len() == instances.len() {
                                                let latency = start_time.elapsed();
                                                println!("Transaction {} fully completed with success: {} in {:?}", transaction_id, success, latency);
                                                completed_transactions
                                                    .push((transaction_id, latency));
                                                pending_2pc.remove(&transaction_id);
                                            }
                                        } else if let Some(start_time) = pending_raft.get_mut(&transaction_id) {
                                            let latency = start_time.elapsed();
                                            println!("Transaction {} fully completed in {:?}", transaction_id, latency);
                                            completed_transactions
                                                .push((transaction_id, latency));
                                            pending_raft.remove(&transaction_id);
                                        }
                                    }
                                    other => trace!("Unexpected payload: {:?}", other),
                                }
                            }
                            Err(e) => trace!("Failed to deserialize payload: {}", e),
                        }
                    }
                }
            }
            Err(_) => {
                trace!("mpsc channel closed");
                break;
            }
        }
    }
}
