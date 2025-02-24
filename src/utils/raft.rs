use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc::{Sender, Receiver}};
use std::thread;
use std::time::Duration;
use rand::random;
use crate::utils::{datastore::{DataStore, Transaction}, event::{NetworkEvent, NetworkPayload, Event}, network::Network};

#[derive(Debug, PartialEq, Clone)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone)]
pub struct LogEntry {
    pub term: u64,
    pub transaction: Transaction,
}

pub struct RaftState {
    instance_id: u64,
    pub role: RaftRole,
    pub current_term: u64,
    voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    last_applied: u64,
    election_timeout: u64, // In milliseconds
    pub network: Network, // Made pub
    pub sender: Sender<Event>, // Made pub
    datastore: DataStore,
    votes_received: usize, // Add to track votes for leader election
}

impl RaftState {
    pub fn new(instance_id: u64, network: Network, sender: Sender<Event>) -> Arc<Mutex<Self>> {
        let datastore = DataStore::load(instance_id); // Load or initialize DataStore
        println!("Initializing RaftState for server {} as Follower with timeout {}ms", instance_id, 150 + random::<u64>() % 150);
        Arc::new(Mutex::new(RaftState {
            instance_id,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            election_timeout: 150 + random::<u64>() % 150, // Random between 150–300ms
            network,
            sender,
            datastore,
            votes_received: 0, // Initialize votes_received
        }))
    }

    // Follower behavior (from Raft Summary)
    pub fn handle_follower(raft: Arc<Mutex<Self>>) {
        let timeout = {
            let state = raft.lock().unwrap();
            state.election_timeout
        };
        println!("Starting election timeout for server {} with timeout {}ms", {
            let state = raft.lock().unwrap();
            state.instance_id
        }, timeout);
        let raft_clone = Arc::clone(&raft); // Clone for thread safety
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(timeout));
            let mut state = raft_clone.lock().unwrap();
            println!("Server {} election timeout triggered, current role={:?}, term={}", state.instance_id, state.role, state.current_term);
            if state.role == RaftRole::Follower {
                state.become_candidate();
                state.request_vote(&raft_clone, state.instance_id);
            }
        });
    }

    // Candidate behavior (from Raft Summary)
    pub fn become_candidate(&mut self) {
        self.role = RaftRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.instance_id);
        self.votes_received = 1; // Vote for self
    }

    pub fn request_vote(&self, raft: &Arc<Mutex<Self>>, instance_id: u64) {
        println!("Server {} requesting votes for term {}", instance_id, self.current_term);
        let (last_log_index, last_log_term, current_term) = {
            let state = raft.lock().unwrap();
            (
                state.log.len() as u64,
                state.log.last().map_or(0, |entry| entry.term),
                state.current_term,
            )
        };
        for peer in self.get_cluster_peers(instance_id) {
            if peer != instance_id {
                let mut network = {
                    let state = raft.lock().unwrap();
                    state.network.clone()
                };
                println!("Sending RequestVote from {} to {} for term {}", instance_id, peer, current_term);
                network.send_message(NetworkEvent {
                    from: instance_id,
                    to: peer,
                    payload: NetworkPayload::RequestVote {
                        term: current_term,
                        candidate_id: instance_id,
                        last_log_index,
                        last_log_term,
                    }.serialize(),
                });
            }
        }
    }

    // Leader behavior (from Raft Summary)
    pub fn become_leader(&mut self, raft: Arc<Mutex<Self>>, instance_id: u64) {
        println!("Server {} became leader for term {}", instance_id, self.current_term);
        self.role = RaftRole::Leader;
        self.votes_received = 0; // Reset votes for next election
        // Send periodic heartbeats
        let raft_clone = Arc::clone(&raft);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(Duration::from_millis(100)); // Heartbeat every 100ms
                let mut state = raft_clone.lock().unwrap();
                for peer in state.get_cluster_peers(instance_id) {
                    if peer != instance_id {
                        state.send_heartbeat(peer, Arc::clone(&raft_clone));
                    }
                }
            }
        });
    }

    pub fn send_heartbeat(&self, to: u64, raft: Arc<Mutex<Self>>) {
        let (current_term, log_len, last_log_term, commit_index, instance_id) = {
            let state = raft.lock().unwrap();
            (
                state.current_term,
                state.log.len() as u64,
                state.log.last().map_or(0, |e| e.term),
                state.commit_index,
                state.instance_id,
            )
        };
        println!("Sending heartbeat from {} to {} for term {}", instance_id, to, current_term);
        let network_event = NetworkEvent {
            from: instance_id,
            to,
            payload: NetworkPayload::AppendEntries {
                term: current_term,
                leader_id: instance_id,
                prev_log_index: log_len,
                prev_log_term: last_log_term,
                entries: Vec::new(), // Empty for heartbeat
                leader_commit: commit_index,
            }.serialize(),
        };
        let _ = self.sender.send(Event::Network(network_event)); // Use sender to send Event::Network
    }

    // Handle RPCs (RequestVote and AppendEntries)
    pub fn handle_request_vote(&mut self, term: u64, candidate_id: u64, last_log_index: u64, last_log_term: u64) -> bool {
        println!("Server {} handling RequestVote for term {} from candidate {}", self.instance_id, term, candidate_id);
        if term > self.current_term {
            println!("Server {} stepping down to Follower for term {}", self.instance_id, term);
            self.become_follower(term);
        }
        let vote_granted = term == self.current_term &&
            (self.voted_for.is_none() || self.voted_for == Some(candidate_id)) &&
            (last_log_term > self.log.last().map_or(0, |e| e.term) ||
             (last_log_term == self.log.last().map_or(0, |e| e.term) && last_log_index >= self.log.len() as u64));
        if vote_granted {
            self.voted_for = Some(candidate_id);
            self.votes_received = 0; // Reset votes if granting a vote to another candidate
            println!("Server {} granting vote to candidate {} for term {}", self.instance_id, candidate_id, self.current_term);
        } else {
            println!("Server {} denying vote to candidate {} for term {}", self.instance_id, candidate_id, self.current_term);
        }
        vote_granted
    }

    pub fn handle_append_entries(&mut self, term: u64, leader_id: u64, prev_log_index: u64, prev_log_term: u64, entries: Vec<(u64, Transaction)>, leader_commit: u64) -> bool {
        println!("Server {} handling AppendEntries for term {} from leader {}", self.instance_id, term, leader_id);
        if term > self.current_term {
            println!("Server {} stepping down to Follower for term {}", self.instance_id, term);
            self.become_follower(term);
        }
        if prev_log_index == 0 || (prev_log_index <= self.log.len() as u64 && self.log[(prev_log_index - 1) as usize].term == prev_log_term) {
            println!("Server {} appending {} entries for term {}", self.instance_id, entries.len(), term);
            // Append new entries
            for (entry_term, transaction) in entries {
                self.log.push(LogEntry { term: entry_term, transaction });
            }
            // Update commit index
            if leader_commit > self.commit_index {
                self.commit_index = leader_commit.min(self.log.len() as u64);
                self.apply_committed_entries();
                println!("Server {} updated commit_index to {} and applied entries", self.instance_id, self.commit_index);
            }
            true
        } else {
            println!("Server {} rejecting AppendEntries due to log inconsistency", self.instance_id);
            false
        }
    }

    pub fn become_follower(&mut self, term: u64) {
        println!("Server {} becoming follower for term {}", self.instance_id, term);
        self.role = RaftRole::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.votes_received = 0; // Reset votes when becoming follower
    }

    pub fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get((self.last_applied - 1) as usize) {
                println!("Server {} applying transaction: {} -> {} ({})", self.instance_id, entry.transaction.from, entry.transaction.to, entry.transaction.value);
                // Apply transaction to DataStore
                self.datastore.process_transfer(entry.transaction.from, entry.transaction.to, entry.transaction.value);
                self.datastore.record_transaction(entry.transaction.from, entry.transaction.to, entry.transaction.value);
                self.datastore.save_to_file(); // Persist changes
            }
        }
    }

    pub fn get_cluster_peers(&self, instance_id: u64) -> Vec<u64> {
        // Return peers in the same cluster (based on instance_id)
        if instance_id <= 3 {
            vec![1, 2, 3]
        } else if instance_id <= 6 {
            vec![4, 5, 6]
        } else {
            vec![7, 8, 9]
        }
    }

    // Helper to append and replicate a transaction (for leaders)
    pub fn append_and_replicate_transaction(&mut self, transaction: Transaction, raft: Arc<Mutex<Self>>, instance_id: u64) {
        if self.role != RaftRole::Leader {
            println!("Server {} not leader (role={:?}), cannot append transaction", instance_id, self.role);
            return;
        }
        println!("Server {} appending transaction: {} -> {} ({}) for term {}", instance_id, transaction.from, transaction.to, transaction.value, self.current_term);
        // Extract necessary data before locking raft
        let current_term = self.current_term;
        let commit_index = self.commit_index;
        let log_len = self.log.len() as u64;
        let last_log_term = self.log.last().map_or(0, |e| e.term);

        self.log.push(LogEntry { term: current_term, transaction: transaction.clone() });
        let peers = self.get_cluster_peers(instance_id); // Use instance_id directly, no new lock needed
        for peer in peers {
            if peer != instance_id {
                let network_event = NetworkEvent {
                    from: instance_id,
                    to: peer,
                    payload: NetworkPayload::AppendEntries {
                        term: current_term,
                        leader_id: instance_id,
                        prev_log_index: log_len - 1, // Previous log index
                        prev_log_term: last_log_term,
                        entries: vec![(current_term, transaction.clone())],
                        leader_commit: commit_index,
                    }.serialize(),
                };
                println!("Server {} sending AppendEntries to {} for term {}", instance_id, peer, current_term);
                let _ = self.sender.send(Event::Network(network_event)); // Use sender to send Event::Network
            }
        }
        // Update commit index after replication (simplified for this example)
        let mut raft_locked = raft.lock().unwrap();
        raft_locked.commit_index = log_len; // Update commit index after appending
        raft_locked.apply_committed_entries();
    }
}