use cs271_final::utils::constants::{PROXY_PORT};
use cs271_final::utils::datastore::{DataStore, Transaction};
use cs271_final::utils::event::{Event, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;
use cs271_final::utils::raft::{RaftRole, RaftState};

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::{env, io};
use std::{process, thread};
use std::time::Duration;

fn main() {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check if the instance ID is provided
    if args.len() < 2 {
        eprintln!("Usage: {} <instance_id>", args[0]);
        process::exit(1);
    }

    // Parse the instance ID from the command-line argument
    let instance_id: u64 = match args[1].parse() {
        Ok(id) => id,
        Err(_) => {
            eprintln!("Error: Invalid instance ID. Please provide a valid number.");
            process::exit(1);
        }
    };

    // Create communication channel and network
    let (sender, receiver) = mpsc::channel();
    let mut network = Network::new(instance_id, sender.clone(), false);
    
    // Connect to the proxy
    if !network.connect_to_proxy(PROXY_PORT) {
        process::exit(1);
    }

    // Spawn the event handling thread
    thread::spawn(move || {
        handle_events(network, receiver, instance_id);
    });

    println!("Server {} started!", instance_id);

    // Keep the main thread alive (simple stdin loop for now)
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
}

fn handle_events(mut network: Network, receiver: Receiver<Event>, instance_id: u64) {
    // Load or initialize DataStore
    let datastore = DataStore::load(instance_id);

    // Create RaftState with thread-safe Arc<Mutex> using the public sender
    let raft_sender = network.sender.clone(); // Access public sender field
    let raft = RaftState::new(instance_id, network, raft_sender);

    // Start as a follower and log initialization
    println!("Initializing RaftState for server {} as Follower", instance_id);
    RaftState::handle_follower(Arc::clone(&raft));

    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(_) => {
                        println!("Handling local event on server {}", instance_id);
                        // Handle local events if any (not implemented here)
                    }
                    Event::Network(message) => {
                        println!("Server {} received network event from {} to {}", instance_id, message.from, message.to);
                        let payload = NetworkPayload::deserialize(message.payload)
                            .unwrap_or_else(|e| {
                                println!("Server {} failed to deserialize payload: {}", instance_id, e);
                                panic!("Deserialization failed");
                            });
                        match payload {
                            NetworkPayload::PrintBalance { id } => {
                                println!("Server {} handling PrintBalance for client {}", instance_id, id);
                                datastore.print_value(id);
                            }
                            NetworkPayload::PrintDatastore => {
                                println!("Server {} handling PrintDatastore", instance_id);
                                datastore.print_datastore();
                            }
                            NetworkPayload::Transfer { from, to, amount } => {
                                println!("Server {} received Transfer request: {} -> {} ({} units)", instance_id, from, to, amount);
                                // Intra-shard check
                                let shard_start = ((instance_id - 1) / 3) * 1000 + 1;
                                let shard_end = shard_start + 999;
                                if shard_start <= from && from <= shard_end && shard_start <= to && to <= shard_end {
                                    let transaction = Transaction { from, to, value: amount };
                                    let raft_clone = Arc::clone(&raft); // Clone Arc outside the lock
                                    {
                                        let mut state = raft_clone.lock().unwrap();
                                        println!("Server {} Raft state: role={:?}, term={}", instance_id, state.role, state.current_term);
                                        if state.role == RaftRole::Leader {
                                            println!("Server {} processing transfer as leader for term {}", instance_id, state.current_term);
                                            state.append_and_replicate_transaction(transaction, Arc::clone(&raft_clone), instance_id);
                                        } else {
                                            println!("Server {} not leader (role={:?}), redirecting or ignoring", instance_id, state.role);
                                        }
                                    }
                                } else {
                                    println!("Server {}: Cross-shard transaction detected, implement 2PC instead", instance_id);
                                }
                            }
                            NetworkPayload::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                                let mut state = raft.lock().unwrap();
                                println!("Server {} handling RequestVote for term {} from candidate {}", instance_id, term, candidate_id);
                                let current_term = state.current_term; // Extract current_term before using state
                                let vote_granted = state.handle_request_vote(term, candidate_id, last_log_index, last_log_term);
                                state.network.send_message(NetworkEvent {
                                    from: instance_id,
                                    to: candidate_id,
                                    payload: NetworkPayload::VoteResponse { term: current_term, vote_granted }.serialize(),
                                });
                                println!("Server {} responded to RequestVote: term={}, vote_granted={}", instance_id, current_term, vote_granted);
                            }
                            NetworkPayload::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                                let mut state = raft.lock().unwrap();
                                println!("Server {} handling AppendEntries for term {} from leader {}", instance_id, term, leader_id);
                                let current_term = state.current_term; // Extract current_term before using state
                                let success = state.handle_append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit);
                                state.network.send_message(NetworkEvent {
                                    from: instance_id,
                                    to: leader_id,
                                    payload: NetworkPayload::AppendResponse { term: current_term, success }.serialize(),
                                });
                                println!("Server {} responded to AppendEntries: term={}, success={}", instance_id, current_term, success);
                            }
                            NetworkPayload::VoteResponse { term, vote_granted } => {
                                let mut state = raft.lock().unwrap();
                                println!("Server {} received VoteResponse for term {}: vote_granted={}", instance_id, term, vote_granted);
                                if state.role == RaftRole::Candidate && term == state.current_term && vote_granted {
                                    let votes = 1; // Vote for self (simplified, track votes properly in a real implementation)
                                    println!("Server {} counting votes: current votes={}", instance_id, votes);
                                    if votes > state.get_cluster_peers(instance_id).len() as u64 / 2 {
                                        println!("Server {} elected as leader for term {}", instance_id, state.current_term);
                                        state.become_leader(Arc::clone(&raft), instance_id);
                                    }
                                }
                            }
                            NetworkPayload::AppendResponse { term, success } => {
                                let mut state = raft.lock().unwrap();
                                println!("Server {} received AppendResponse for term {}: success={}", instance_id, term, success);
                                if state.role == RaftRole::Leader && success {
                                    state.commit_index = state.log.len() as u64;
                                    state.apply_committed_entries();
                                    println!("Server {} updated commit_index to {} and applied entries", instance_id, state.commit_index);
                                }
                            }
                        }
                    }
                }
            }
            Err(_) => {
                println!("Server {}: mpsc channel closed", instance_id);
                break;
            }
        }
    }
}