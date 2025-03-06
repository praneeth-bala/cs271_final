use cs271_final::utils::constants::{
    CLIENT_INSTANCE_ID, HEARTBEAT_TIMEOUT, PACKET_PROCESS_DELAY, PROXY_PORT,
};
use cs271_final::utils::event::{Event, LocalEvent, LocalPayload, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;

use log::{debug, info, trace};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::{env, io, process, thread, time::Duration};

mod server;
use server::{RaftServer, ServerRole};

fn main() {
    env_logger::init();
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
            HEARTBEAT_TIMEOUT + 2 * PACKET_PROCESS_DELAY + ((instance_id - 1) % 3) * 2000,
        );
        let mut last_heartbeat = Instant::now();
        loop {
            thread::sleep(Duration::from_millis(HEARTBEAT_TIMEOUT / 10));
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

            if last_heartbeat.elapsed() < Duration::from_millis(HEARTBEAT_TIMEOUT) {
                continue;
            }

            if *role_clone.lock().unwrap() == ServerRole::Leader {
                sender_clone
                    .send(Event::Local(LocalEvent {
                        payload: LocalPayload::SendHeartbeat,
                    }))
                    .unwrap();
                last_heartbeat = Instant::now();
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
                        trace!("Handling local event");
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
                                info!(
                                    "Server {} processing PrintBalance for ID {}",
                                    raft_server.instance_id, id
                                );
                                raft_server.datastore.print_value(id);
                                let balance = raft_server.datastore.kv_store.get(&id).copied();
                                network.send_message(NetworkEvent {
                                    from: raft_server.instance_id,
                                    to: CLIENT_INSTANCE_ID,
                                    payload: NetworkPayload::BalanceResponse { id, balance }
                                        .serialize(),
                                });
                                info!(
                                    "Server {} sent balance {:?} for ID {} to client",
                                    raft_server.instance_id, balance, id
                                );
                            }
                            NetworkPayload::PrintDatastore => {
                                info!(
                                    "Server {} processing PrintDatastore",
                                    raft_server.instance_id
                                );
                                raft_server.datastore.print_datastore();
                                let transactions =
                                    raft_server.datastore.committed_transactions.clone();
                                let transaction_count = transactions.len();
                                network.send_message(NetworkEvent {
                                    from: raft_server.instance_id,
                                    to: CLIENT_INSTANCE_ID,
                                    payload: NetworkPayload::DatastoreResponse {
                                        instance: raft_server.instance_id,
                                        transactions,
                                    }
                                    .serialize(),
                                });
                                info!(
                                    "Server {} sent datastore with {} transactions to client",
                                    raft_server.instance_id, transaction_count
                                );
                            }
                            NetworkPayload::Transfer {
                                from,
                                to,
                                amount,
                                transaction_id,
                            } => {
                                info!(
                                                                                    "Server {} received Transfer request: {} -> {} ({} units), ID: {}",
                                                                                    raft_server.instance_id, from, to, amount, transaction_id
                                                                                );
                                raft_server.handle_transfer(payload, message.from, &mut network);
                            }
                            NetworkPayload::RequestVote { .. } => {
                                debug!(
                                    "Server {} received RequestVote from {}",
                                    raft_server.instance_id, message.from
                                );
                                *last_ping.lock().unwrap() = std::time::Instant::now();
                                raft_server.handle_request_vote(
                                    payload,
                                    message.from,
                                    &mut network,
                                );
                            }
                            NetworkPayload::VoteResponse { term, vote_granted } => {
                                debug!("Server {} received VoteResponse from {} in term {}, vote_granted: {}", raft_server.instance_id, message.from, term, vote_granted);
                                raft_server.handle_vote_response(payload);
                            }
                            NetworkPayload::AppendEntries { .. } => {
                                debug!(
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
                                debug!("Server {} received AppendEntriesResponse from {} in term {}, success: {}", raft_server.instance_id, message.from, term, success);
                                raft_server.handle_append_entries_response(
                                    payload,
                                    message.from,
                                    &mut network,
                                );
                            }
                            NetworkPayload::Prepare { .. } => {
                                info!(
                                    "Server {} received Prepare from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_prepare(payload, message.from, &mut network);
                            }
                            NetworkPayload::PrepareResponse { .. } => todo!(),
                            NetworkPayload::Commit { .. } => {
                                info!(
                                    "Server {} received Commit from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_commit(payload, message.from, &mut network);
                            }
                            NetworkPayload::Abort { .. } => {
                                info!(
                                    "Server {} received Abort from {}",
                                    raft_server.instance_id, message.from
                                );
                                raft_server.handle_abort(payload, message.from, &mut network);
                            }
                            NetworkPayload::Ack {
                                transaction_id,
                                success,
                            } => {
                                info!(
                                    "Server {} received Ack for transaction {} with success: {}",
                                    raft_server.instance_id, transaction_id, success
                                );
                                // Followers don’t need to act on Ack; it’s for the client
                            }
                            NetworkPayload::BalanceResponse { id, balance } => {
                                trace!(
                                    "Server {} ignoring BalanceResponse for ID {} from {} ",
                                    raft_server.instance_id,
                                    id,
                                    message.from,
                                );
                            }
                            NetworkPayload::DatastoreResponse {
                                instance,
                                transactions,
                            } => {
                                trace!(
                                    "Server {} ignoring DatastoreResponse for instance {} from {}",
                                    raft_server.instance_id,
                                    instance,
                                    message.from
                                );
                            }
                        }
                    }
                }
            }
            Err(_) => {
                debug!("mpsc channel closed");
                break;
            }
        }
    }
}
