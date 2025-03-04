use cs271_final::utils::constants::{HEARTBEAT_TIMEOUT, PROXY_PORT};
use cs271_final::utils::event::{Event, LocalEvent, LocalPayload, NetworkPayload};
use cs271_final::utils::network::Network;

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::{env, io, process, thread, time::Duration};

mod server;
use server::{RaftServer, ServerRole};

fn main() {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check if the instance ID is provided
    if args.len() < 2 {
        eprintln!("Usage: {} <instance_id>", args[0]);
        process::exit(1);
    }

    // Parse the instance ID from the command-line argument
    let instance_id = match args[1].parse() {
        Ok(id) => id,
        Err(_) => {
            eprintln!("Error: Invalid instance ID. Please provide a valid number.");
            process::exit(1);
        }
    };

    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(instance_id, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) {
        process::exit(1);
    }
    let raft_server = RaftServer::new(instance_id);

    let last_ping = Arc::new(Mutex::new(std::time::Instant::now()));
    
    // Spawn a separate thread for timeout handling
    let last_ping_clone = Arc::clone(&last_ping);
    let role_clone = Arc::clone(&raft_server.role);
    let sender_clone = sender.clone();
    thread::spawn(move || {
        let election_timeout = Duration::from_millis(
            3 * HEARTBEAT_TIMEOUT + rand::random::<u64>() % HEARTBEAT_TIMEOUT,
        ); // Random timeout between 3T & 4Ts
        loop {
            thread::sleep(Duration::from_millis(HEARTBEAT_TIMEOUT/10));
            let mut locked_ping = last_ping_clone.lock().unwrap();

            if *role_clone.lock().unwrap() != ServerRole::Leader
                && locked_ping.elapsed() >= election_timeout
            {
                sender_clone
                    .send(Event::Local(LocalEvent {
                        payload: LocalPayload::StartElection,
                    }))
                    .unwrap();
                *locked_ping = std::time::Instant::now();
            }

            if *role_clone.lock().unwrap() == ServerRole::Leader {
                sender_clone
                    .send(Event::Local(LocalEvent {
                        payload: LocalPayload::SendHeartbeat,
                    }))
                    .unwrap();
            }
        }
    });

    thread::spawn(move || {
        handle_events(network, receiver, raft_server, last_ping);
    });

    println!("Server {} started!", instance_id);

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    loop {
        // io::stdin().read_line(&mut input).unwrap();
        // update_config(&config_map, "config.txt");
        thread::sleep(Duration::from_millis(100000));
    }
}

fn handle_events(
    mut network: Network,
    receiver: Receiver<Event>,
    mut raft_server: RaftServer,
    last_ping: Arc<Mutex<std::time::Instant>>,
) {
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(message) => {
                        println!("Handling local event");
                        // Handle local events if any
                        match message {
                            LocalEvent {
                                payload: LocalPayload::SendHeartbeat,
                            } => {
                                raft_server.replicate_log(&mut network, None);
                            }
                            LocalEvent {
                                payload: LocalPayload::StartElection,
                            } => {
                                raft_server.start_election(&mut network);
                            }
                            _ => {}
                        }
                    }
                    Event::Network(message) => {
                        let payload = NetworkPayload::deserialize(message.payload)
                            .expect("Failed to deserialize payload");
                        match payload {
                            NetworkPayload::PrintBalance { id } => {
                                println!(
                                    "Server {} processing PrintBalance for ID {}",
                                    raft_server.instance_id, id
                                );
                                raft_server.datastore.print_value(id);
                            }
                            NetworkPayload::PrintDatastore => {
                                println!(
                                    "Server {} processing PrintDatastore",
                                    raft_server.instance_id
                                );
                                raft_server.datastore.print_datastore();
                            }
                            NetworkPayload::Transfer { from, to, amount } => {
                                println!(
                                    "Server {} received Transfer request: {} -> {} ({} units)",
                                    raft_server.instance_id, from, to, amount
                                );
                                raft_server.handle_transfer(payload, message.from, &mut network);
                            }
                            NetworkPayload::RequestVote { .. } => {
                                println!(
                                    "Server {} received RequestVote from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_request_vote(
                                    payload,
                                    message.from,
                                    &mut network,
                                );
                            }
                            NetworkPayload::VoteResponse { term, vote_granted } => {
                                println!("Server {} received VoteResponse from {} in term {}, vote_granted: {}", raft_server.instance_id, message.from, term, vote_granted);
                                raft_server.handle_vote_response(payload);
                            }
                            NetworkPayload::AppendEntries { .. } => {
                                println!(
                                    "Server {} received AppendEntries from {}",
                                    raft_server.instance_id, message.from
                                );
                                *last_ping.lock().unwrap() = std::time::Instant::now();

                                raft_server.handle_append_entries(
                                    payload,
                                    message.from,
                                    &mut network,
                                );
                            }
                            NetworkPayload::AppendEntriesResponse { term, success, .. } => {
                                println!("Server {} received AppendEntriesResponse from {} in term {}, success: {}", raft_server.instance_id, message.from, term, success);
                                raft_server.handle_append_entries_response(
                                    payload,
                                    message.from,
                                    &mut network,
                                );
                            }
                            NetworkPayload::Prepare { .. } => {
                                println!(
                                    "Server {} received Prepare from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_prepare(payload, message.from, &mut network);
                            }
                            NetworkPayload::PrepareResponse { .. } => todo!(),
                            NetworkPayload::Commit { .. } => {
                                println!(
                                    "Server {} received Commit from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_commit(payload, message.from, &mut network);
                            }
                            NetworkPayload::Abort { .. } => {
                                println!(
                                    "Server {} received Abort from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_abort(payload, message.from, &mut network);
                            }
                            NetworkPayload::Ack {
                                transaction_id,
                                success,
                            } => {
                                println!(
                                    "Server {} received Ack for transaction {} with success: {}",
                                    raft_server.instance_id, transaction_id, success
                                );
                                // Followers don’t need to act on Ack; it’s for the client
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
