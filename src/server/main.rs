use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::{DataStore, LogEntry, Transaction};
use cs271_final::utils::event::{Event, NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;

use std::sync::mpsc::{self, Receiver, Sender};
use std::{env, io, process, thread, time::Duration};

#[derive(Clone, Copy, PartialEq)]
enum ServerRole {
    Follower,
    Candidate,
    Leader,
}

struct RaftServer {
    role: ServerRole,
    datastore: DataStore,
    election_timeout: Option<Duration>,
    heartbeat_timeout: Option<Duration>,
    votes_received: usize, // Track votes for candidates
    cluster_servers: Vec<u64>, // Servers in the same cluster (shard)
}

impl RaftServer {
    fn new(instance_id: u64) -> Self {
        let cluster = if instance_id <= 3 {
            vec![1, 2, 3] // Cluster C1
        } else if instance_id <= 6 {
            vec![4, 5, 6] // Cluster C2
        } else {
            vec![7, 8, 9] // Cluster C3
        };
        println!("Server {} initialized as Follower in cluster {:?}", instance_id, cluster);
        RaftServer {
            role: ServerRole::Follower,
            datastore: DataStore::load(instance_id),
            election_timeout: Some(Duration::from_millis(150 + rand::random::<u64>() % 150)), // Random timeout between 150-300ms
            heartbeat_timeout: Some(Duration::from_millis(50)), // Heartbeat every 50ms
            votes_received: 0,
            cluster_servers: cluster,
        }
    }

    fn start_election(&mut self, network: &mut Network) {
        println!("Server {} starting election in term {}", self.datastore.instance_id, self.datastore.current_term);
        self.role = ServerRole::Candidate;
        self.datastore.current_term += 1;
        self.datastore.voted_for = Some(self.datastore.instance_id);
        self.votes_received = 1; // Vote for self

        // Send RequestVote RPCs to all other servers in the cluster
        let request = NetworkPayload::RequestVote {
            term: self.datastore.current_term,
            candidate_id: self.datastore.instance_id,
            last_log_index: self.datastore.log.len() as u64,
            last_log_term: self.datastore.last_log_entry()
                .map_or(0, |entry| entry.term),
        };
        for server in &self.cluster_servers {
            if *server != self.datastore.instance_id {
                println!("Server {} sending RequestVote to server {} in term {}", self.datastore.instance_id, server, self.datastore.current_term);
                network.send_message(NetworkEvent {
                    from: self.datastore.instance_id,
                    to: *server,
                    payload: request.serialize(),
                });
            }
        }
    }

    fn step_down(&mut self) {
        println!("Server {} stepping down to Follower in term {}", self.datastore.instance_id, self.datastore.current_term);
        self.role = ServerRole::Follower;
        self.datastore.voted_for = None;
        self.votes_received = 0;
    }

    fn handle_request_vote(&mut self, request: NetworkPayload, from: u64) -> NetworkPayload {
        println!("Server {} received RequestVote from {} in term {:?}", self.datastore.instance_id, from, match request {
            NetworkPayload::RequestVote { term, .. } => term,
            _ => 0,
        });
        match request {
            NetworkPayload::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                if term < self.datastore.current_term {
                    println!("Server {} rejecting RequestVote from {}: term {} < current term {}", self.datastore.instance_id, candidate_id, term, self.datastore.current_term);
                    return NetworkPayload::VoteResponse {
                        term: self.datastore.current_term,
                        vote_granted: false,
                    };
                }

                if term > self.datastore.current_term {
                    println!("Server {} updating term to {} and stepping down", self.datastore.instance_id, term);
                    self.datastore.current_term = term;
                    self.step_down();
                }

                let last_log = self.datastore.last_log_entry();
                let up_to_date = match last_log {
                    Some(entry) => last_log_term >= entry.term && last_log_index >= self.datastore.log.len() as u64,
                    None => last_log_index == 0,
                };

                let already_voted = self.datastore.voted_for.is_some();
                let grant_vote = !already_voted && up_to_date;

                if grant_vote {
                    println!("Server {} granting vote to candidate {} in term {}", self.datastore.instance_id, candidate_id, term);
                    self.datastore.voted_for = Some(candidate_id);
                } else {
                    println!("Server {} denying vote to candidate {} in term {}: already voted or log not up-to-date", self.datastore.instance_id, candidate_id, term);
                }

                NetworkPayload::VoteResponse {
                    term: self.datastore.current_term,
                    vote_granted: grant_vote,
                }
            }
            _ => unreachable!("Unexpected payload type for RequestVote"),
        }
    }

    fn handle_append_entries(&mut self, request: NetworkPayload, from: u64) -> NetworkPayload {
        println!("Server {} received AppendEntries from {} in term {:?}", self.datastore.instance_id, from, match request {
            NetworkPayload::AppendEntries { term, .. } => term,
            _ => 0,
        });
        match request {
            NetworkPayload::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term < self.datastore.current_term {
                    println!("Server {} rejecting AppendEntries from {}: term {} < current term {}", self.datastore.instance_id, leader_id, term, self.datastore.current_term);
                    return NetworkPayload::AppendEntriesResponse {
                        term: self.datastore.current_term,
                        success: false,
                    };
                }

                if term > self.datastore.current_term {
                    println!("Server {} updating term to {} and stepping down", self.datastore.instance_id, term);
                    self.datastore.current_term = term;
                    self.step_down();
                }

                self.role = ServerRole::Follower; // Reset to follower on receiving valid AppendEntries

                if prev_log_index > 0 && !self.datastore.log_is_consistent(prev_log_index, prev_log_term) {
                    println!("Server {} rejecting AppendEntries from {}: log inconsistency at index {} term {}", self.datastore.instance_id, leader_id, prev_log_index, prev_log_term);
                    return NetworkPayload::AppendEntriesResponse {
                        term: self.datastore.current_term,
                        success: false,
                    };
                }

                // Append new entries
                let start_index = prev_log_index as usize + 1;
                for entry in &entries {
                    println!("Server {} appending log entry at index {} in term {}", self.datastore.instance_id, entry.index, entry.term);
                    if start_index <= self.datastore.log.len() {
                        self.datastore.log[start_index - 1] = entry.clone();
                    } else {
                        self.datastore.append_log(entry.clone());
                    }
                }

                // Update commit index
                if leader_commit > self.datastore.log.len() as u64 {
                    println!("Server {} applying committed entries up to index {}", self.datastore.instance_id, leader_commit);
                    self.datastore.apply_committed_entries(leader_commit);
                }

                println!("Server {} accepted AppendEntries from {} successfully", self.datastore.instance_id, leader_id);
                NetworkPayload::AppendEntriesResponse {
                    term: self.datastore.current_term,
                    success: true,
                }
            }
            _ => unreachable!("Unexpected payload type for AppendEntries"),
        }
    }

    fn become_leader(&mut self) {
        println!("Server {} becoming leader in term {}", self.datastore.instance_id, self.datastore.current_term);
        self.role = ServerRole::Leader;
        // Initialize nextIndex and matchIndex for each follower (simplified for now)
        // For this implementation, we'll assume nextIndex starts at the end of the log
    }

    fn replicate_log(&mut self, network: &mut Network) {
        println!("Server {} (Leader) sending AppendEntries to cluster in term {}", self.datastore.instance_id, self.datastore.current_term);
        let last_log = self.datastore.last_log_entry().unwrap_or(&LogEntry {
            term: 0,
            index: 0,
            command: Transaction { from: 0, to: 0, value: 0 },
        });
        let request = NetworkPayload::AppendEntries {
            term: self.datastore.current_term,
            leader_id: self.datastore.instance_id,
            prev_log_index: if last_log.index > 1 { last_log.index - 1 } else { 0 },
            prev_log_term: if last_log.index > 1 {
                self.datastore.log[(last_log.index - 2) as usize].term
            } else { 0 },
            entries: if last_log.index > 0 { vec![last_log.clone()] } else { vec![] }, // Send the latest entry or empty for heartbeat
            leader_commit: self.datastore.log.len() as u64, // Simplified commit index
        };
        for server in &self.cluster_servers {
            if *server != self.datastore.instance_id {
                println!("Server {} sending AppendEntries to server {} in term {}", self.datastore.instance_id, server, self.datastore.current_term);
                network.send_message(NetworkEvent {
                    from: self.datastore.instance_id,
                    to: *server,
                    payload: request.serialize(),
                });
            }
        }
    }
}

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

    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(instance_id, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) {
        process::exit(1);
    }
    let mut raft_server = RaftServer::new(instance_id);

    thread::spawn(move || {
        handle_events(network, receiver, raft_server);
    });

    println!("Server {} started!", instance_id);

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
}

fn handle_events(mut network: Network, receiver: Receiver<Event>, mut raft_server: RaftServer) {
    let mut last_election_time = std::time::Instant::now();
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(_) => {
                        println!("Handling local event");
                        // Handle local events if any
                    }
                    Event::Network(message) => {
                        let payload = NetworkPayload::deserialize(message.payload)
                            .expect("Failed to deserialize payload");
                        match payload {
                            NetworkPayload::PrintBalance { id } => {
                                println!("Server {} processing PrintBalance for ID {}", raft_server.datastore.instance_id, id);
                                raft_server.datastore.print_value(id);
                            }
                            NetworkPayload::PrintDatastore => {
                                println!("Server {} processing PrintDatastore", raft_server.datastore.instance_id);
                                raft_server.datastore.print_datastore();
                            }
                            NetworkPayload::Transfer { from, to, amount } => {
                                println!("Server {} received Transfer request: {} -> {} ({} units)", raft_server.datastore.instance_id, from, to, amount);
                                // For intra-shard, initiate Raft consensus
                                if from / 1000 == to / 1000 { // Same cluster (shard)
                                    let transaction = Transaction { from, to, value: amount };
                                    let log_entry = LogEntry {
                                        term: raft_server.datastore.current_term,
                                        index: raft_server.datastore.log.len() as u64 + 1,
                                        command: transaction,
                                    };
                                    println!("Server {} appending log entry for transfer in term {}", raft_server.datastore.instance_id, raft_server.datastore.current_term);
                                    raft_server.datastore.append_log(log_entry);
                                    if raft_server.role == ServerRole::Leader {
                                        raft_server.replicate_log(&mut network);
                                    } else {
                                        println!("Server {} not leader, waiting for election or leader response", raft_server.datastore.instance_id);
                                        // Not leader, need to forward to leader or start election
                                        // For now, we'll handle election timeout
                                    }
                                } else {
                                    // Cross-shard, handle with 2PC (to be implemented later)
                                    println!("Server {} detected cross-shard transaction, to be handled with 2PC", raft_server.datastore.instance_id);
                                    todo!("Implement 2PC for cross-shard transactions");
                                }
                            }
                            NetworkPayload::RequestVote { .. } => {
                                let response = raft_server.handle_request_vote(payload, message.from);
                                println!("Server {} responding to RequestVote from {} with vote_granted: {}", raft_server.datastore.instance_id, message.from, match response {
                                    NetworkPayload::VoteResponse { vote_granted, .. } => vote_granted,
                                    _ => false,
                                });
                                network.send_message(NetworkEvent {
                                    from: raft_server.datastore.instance_id,
                                    to: message.from,
                                    payload: response.serialize(),
                                });
                            }
                            NetworkPayload::VoteResponse { term, vote_granted } => {
                                println!("Server {} received VoteResponse from {} in term {}, vote_granted: {}", raft_server.datastore.instance_id, message.from, term, vote_granted);
                                if raft_server.role == ServerRole::Candidate && term == raft_server.datastore.current_term {
                                    if vote_granted {
                                        raft_server.votes_received += 1;
                                        let majority = (raft_server.cluster_servers.len() + 1) / 2; // +1 for self
                                        println!("Server {} has {} votes, needs {} for majority", raft_server.datastore.instance_id, raft_server.votes_received, majority);
                                        if raft_server.votes_received >= majority {
                                            raft_server.become_leader();
                                            raft_server.replicate_log(&mut network); // Start heartbeats
                                        }
                                    }
                                } else if term > raft_server.datastore.current_term {
                                    println!("Server {} updating term to {} and stepping down due to higher term in VoteResponse", raft_server.datastore.instance_id, term);
                                    raft_server.datastore.current_term = term;
                                    raft_server.step_down();
                                }
                            }
                            NetworkPayload::AppendEntries { .. } => {
                                let response = raft_server.handle_append_entries(payload, message.from);
                                println!("Server {} responding to AppendEntries from {} with success: {}", raft_server.datastore.instance_id, message.from, match response {
                                    NetworkPayload::AppendEntriesResponse { success, .. } => success,
                                    _ => false,
                                });
                                network.send_message(NetworkEvent {
                                    from: raft_server.datastore.instance_id,
                                    to: message.from,
                                    payload: response.serialize(),
                                });
                            }
                            NetworkPayload::AppendEntriesResponse { term, success } => {
                                println!("Server {} received AppendEntriesResponse from {} in term {}, success: {}", raft_server.datastore.instance_id, message.from, term, success);
                                if raft_server.role == ServerRole::Leader && term == raft_server.datastore.current_term {
                                    if success {
                                        println!("Server {} confirmed log replication success from {}", raft_server.datastore.instance_id, message.from);
                                        // Update matchIndex and nextIndex (simplified for now)
                                        // For this implementation, we assume replication succeeded
                                    } else if term > raft_server.datastore.current_term {
                                        println!("Server {} updating term to {} and stepping down due to higher term in AppendEntriesResponse", raft_server.datastore.instance_id, term);
                                        raft_server.datastore.current_term = term;
                                        raft_server.step_down();
                                    }
                                }
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

        // Check election timeout (Follower behavior)
        if raft_server.role == ServerRole::Follower {
            if last_election_time.elapsed() >= raft_server.election_timeout.unwrap() {
                println!("Server {} election timeout triggered, starting election in term {}", raft_server.datastore.instance_id, raft_server.datastore.current_term);
                raft_server.start_election(&mut network);
                last_election_time = std::time::Instant::now();
            }
        }

        // Check heartbeat timeout (Leader behavior)
        if raft_server.role == ServerRole::Leader {
            if last_election_time.elapsed() >= raft_server.heartbeat_timeout.unwrap() {
                println!("Server {} (Leader) sending heartbeat in term {}", raft_server.datastore.instance_id, raft_server.datastore.current_term);
                raft_server.replicate_log(&mut network);
                last_election_time = std::time::Instant::now();
            }
        }
    }
}