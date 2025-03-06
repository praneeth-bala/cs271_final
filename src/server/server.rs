use cs271_final::utils::constants::CLIENT_INSTANCE_ID;
use cs271_final::utils::datastore::{DataStore, LogEntry, Transaction};
use cs271_final::utils::event::{NetworkEvent, NetworkPayload};
use cs271_final::utils::network::Network;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::{info, debug};

#[derive(Clone, Copy, PartialEq)]
pub enum ServerRole {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftServer {
    pub role: Arc<Mutex<ServerRole>>,
    pub datastore: DataStore,
    pub cluster_servers: Vec<u64>,
    pub next_index_map: HashMap<u64, usize>,
    pub commit_index_map: HashMap<u64, usize>,
    pub instance_id: u64,
    pub leader_id: Option<u64>,

    pub votes_received: u64,
    pub current_term: u64,
    pub voted_for: Option<u64>,

}

impl RaftServer {
    pub fn new(instance_id: u64) -> Self {
        let cluster = if instance_id <= 3 {
            vec![1, 2, 3] // Cluster C1
        } else if instance_id <= 6 {
            vec![4, 5, 6] // Cluster C2
        } else {
            vec![7, 8, 9] // Cluster C3
        };
        let next_index_map = cluster.iter().map(|&server| (server, 0)).collect();
        let commit_index_map = cluster.iter().map(|&server| (server, 0)).collect();
        debug!(
            "Server {} initialized as Follower in cluster {:?}",
            instance_id, cluster
        );
        RaftServer {
            role: Arc::new(Mutex::new(ServerRole::Follower)),
            datastore: DataStore::load(instance_id),
            votes_received: 0,
            cluster_servers: cluster,
            next_index_map,
            commit_index_map,
            instance_id,
            leader_id: None,
            current_term: 0,
            voted_for: None,
        }
    }

    pub fn start_election(&mut self, network: &mut Network) {
        debug!(
            "Server {} starting election in term {}",
            self.instance_id, self.current_term
        );
        *self.role.lock().unwrap() = ServerRole::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.instance_id);
        self.votes_received = 1; // Vote for self

        // Send RequestVote RPCs to all other servers in the cluster
        let request = NetworkPayload::RequestVote {
            term: self.current_term,
            candidate_id: self.instance_id,
            last_log_index: self.datastore.log.len() as u64,
            last_log_term: self
                .datastore
                .last_log_entry()
                .map_or(0, |entry| entry.term),
        };
        for server in &self.cluster_servers {
            if *server != self.instance_id {
                debug!(
                    "Server {} sending RequestVote to server {} in term {}",
                    self.instance_id, server, self.current_term
                );
                network.send_message(NetworkEvent {
                    from: self.instance_id,
                    to: *server,
                    payload: request.serialize(),
                });
            }
        }
    }

    pub fn step_down(&mut self) {
        debug!(
            "Server {} stepping down to Follower in term {}",
            self.instance_id, self.current_term
        );
        *self.role.lock().unwrap() = ServerRole::Follower;
        self.voted_for = None;
        self.votes_received = 0;
    }

    pub fn handle_request_vote(
        &mut self,
        request: NetworkPayload,
        from: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                if term < self.current_term {
                    debug!(
                        "Server {} rejecting RequestVote from {}: term {} < current term {}",
                        self.instance_id, candidate_id, term, self.current_term
                    );
                    network.send_message(NetworkEvent {
                        from: self.instance_id,
                        to: from,
                        payload: NetworkPayload::VoteResponse {
                            term: self.current_term,
                            vote_granted: false,
                        }
                        .serialize(),
                    });
                    return;
                }

                if term > self.current_term {
                    debug!(
                        "Server {} updating term to {} and stepping down",
                        self.instance_id, term
                    );
                    self.current_term = term;
                    self.voted_for = None;
                    self.step_down();
                }

                let last_log = self.datastore.last_log_entry();
                let up_to_date = match last_log {
                    Some(entry) => {
                        last_log_term >= entry.term
                            && last_log_index >= self.datastore.log.len() as u64
                    }
                    _ => last_log_index == 0,
                };

                let already_voted = self.voted_for.is_some();
                let grant_vote = !already_voted && up_to_date;

                if grant_vote {
                    debug!(
                        "Server {} granting vote to candidate {} in term {}",
                        self.instance_id, candidate_id, term
                    );
                    self.voted_for = Some(candidate_id);
                    self.leader_id = Some(candidate_id);
                } else {
                    debug!("Server {} denying vote to candidate {} in term {}: already voted or log not up-to-date", self.instance_id, candidate_id, term);
                    return;
                }

                network.send_message(NetworkEvent {
                    from: self.instance_id,
                    to: from,
                    payload: NetworkPayload::VoteResponse {
                        term: self.current_term,
                        vote_granted: grant_vote,
                    }
                    .serialize(),
                });
            }
            _ => unreachable!("Unexpected payload type for RequestVote"),
        }
    }

    pub fn handle_vote_response(&mut self, request: NetworkPayload) {
        match request {
            NetworkPayload::VoteResponse { term, vote_granted } => {
                if *self.role.lock().unwrap() == ServerRole::Candidate && term == self.current_term
                {
                    if vote_granted {
                        self.votes_received += 1;
                        let majority = ((self.cluster_servers.len() + 1) / 2) as u64; // +1 for self
                        debug!(
                            "Server {} has {} votes, needs {} for majority",
                            self.instance_id, self.votes_received, majority
                        );
                        if self.votes_received >= majority {
                            self.become_leader();
                            self.leader_id = Some(self.instance_id);
                            // Set next index of all followers to the end of the log
                            for server in &self.cluster_servers {
                                self.next_index_map
                                    .insert(*server, self.datastore.log.len());
                                self.commit_index_map.insert(*server, 0);
                            }
                            self.commit_index_map
                                .insert(self.instance_id, self.datastore.log.len());
                        }
                    }
                } else if term > self.current_term {
                    debug!("Server {} updating term to {} and stepping down due to higher term in VoteResponse", self.instance_id, term);
                    self.current_term = term;
                    self.step_down();
                }
            }
            _ => unreachable!("Unexpected payload type for VoteResponse"),
        }
    }

    pub fn handle_append_entries(
        &mut self,
        request: NetworkPayload,
        from: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term < self.current_term {
                    debug!(
                        "Server {} rejecting AppendEntries from {}: term {} < current term {}",
                        self.instance_id, leader_id, term, self.current_term
                    );
                    network.send_message(NetworkEvent {
                        from: self.instance_id,
                        to: from,
                        payload: NetworkPayload::AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            next_index: self.datastore.log.len(),
                        }
                        .serialize(),
                    });
                    return;
                }

                if term > self.current_term {
                    debug!(
                        "Server {} updating term to {} and stepping down",
                        self.instance_id, term
                    );
                    self.current_term = term;
                    self.step_down();
                }

                *self.role.lock().unwrap() = ServerRole::Follower; // Reset to follower on receiving valid AppendEntries
                self.leader_id = Some(leader_id);

                if !self
                    .datastore
                    .log_is_consistent(&prev_log_index, &prev_log_term)
                {
                    debug!("Server {} rejecting AppendEntries from {}: log inconsistency prev_index:{}, term:{}", self.instance_id, leader_id, prev_log_index.unwrap_or(1337), prev_log_term.unwrap_or(1337));
                    network.send_message(NetworkEvent {
                        from: self.instance_id,
                        to: from,
                        payload: NetworkPayload::AppendEntriesResponse {
                            term: self.current_term,
                            success: false,
                            next_index: self.datastore.log.len(),
                        }
                        .serialize(),
                    });
                    return;
                }

                // Append new entries
                let mut start_index = if prev_log_index.is_some() {
                    prev_log_index.unwrap() as usize + 1
                } else {
                    0
                };
                for entry in &entries {
                    debug!(
                        "Server {} appending log entry at index {} in term {}",
                        self.instance_id, entry.index, entry.term
                    );
                    if start_index < self.datastore.log.len() {
                        self.datastore.log[start_index] = entry.clone();
                    } else {
                        self.datastore.append_log(entry.clone());
                    }
                    start_index += 1;
                }
                self.datastore.log.truncate(start_index);
                self.datastore.save_to_file();

                // Update commit index
                self.datastore.update_commit_from_index(&leader_commit);

                debug!(
                    "Server {} accepted AppendEntries from {} successfully",
                    self.instance_id, leader_id
                );
                network.send_message(NetworkEvent {
                    from: self.instance_id,
                    to: from,
                    payload: NetworkPayload::AppendEntriesResponse {
                        term: self.current_term,
                        success: true,
                        next_index: self.datastore.log.len(),
                    }
                    .serialize(),
                });
                return;
            }
            _ => unreachable!("Unexpected payload type for AppendEntries"),
        }
    }

    pub fn handle_append_entries_response(
        &mut self,
        request: NetworkPayload,
        from: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::AppendEntriesResponse {
                term,
                success,
                next_index,
            } => {
                if *self.role.lock().unwrap() == ServerRole::Leader && term == self.current_term {
                    if success {
                        debug!(
                            "Server {} confirmed log replication success from {} with next index {}",
                            self.instance_id, from, next_index
                        );
                        self.next_index_map.insert(from, next_index);
                        self.commit_index_map.insert(from, next_index);
                        self.datastore
                            .calculate_latest_commit(&self.commit_index_map, self.current_term);

                        // Cross shard stuff
                        // Need to check if latest commit index exceeds any pending transaction index in the log and send prepare success message for those.
                        let mut to_delete: Vec<u64> = Vec::new();
                        for (&key, index) in self.datastore.pending_transactions.iter() {
                            if self.datastore.committed_transactions.len() > *index {
                                let tran = self.datastore.log_entry(*index).unwrap().command;
                                if tran.twopc_prepare {
                                    // If prepare was replicated
                                    network.send_message(NetworkEvent {
                                        from: self.instance_id,
                                        to: CLIENT_INSTANCE_ID,
                                        payload: NetworkPayload::PrepareResponse {
                                            transaction_id: key,
                                            success: true,
                                        }
                                        .serialize(),
                                    });
                                } else {
                                    // If commit/abort was replicated
                                    network.send_message(NetworkEvent {
                                        from: self.instance_id,
                                        to: CLIENT_INSTANCE_ID,
                                        payload: NetworkPayload::Ack {
                                            transaction_id: key,
                                            success: if tran.value == 0 {false} else {true},
                                        }
                                        .serialize(),
                                    });
                                    to_delete.push(key);
                                }
                            }
                        }
                        for key in to_delete.iter() {
                            let index = self.datastore.pending_transactions.get(key).unwrap();
                            let tran = self.datastore.log_entry(*index).unwrap().command;
                            self.datastore.release_locks(vec![tran.from, tran.to]);
                            self.datastore.pending_transactions.remove(key);
                        }
                        self.datastore.save_to_file();
                    } else if term > self.current_term {
                        debug!(
                            "Server {} updating term to {} and stepping down due to higher term",
                            self.instance_id, term
                        );
                        self.current_term = term;
                        self.step_down();
                        self.leader_id = Some(from);
                    } else {
                        debug!(
                            "Server {} detected log inconsistency with follower {}: success=false",
                            self.instance_id, from
                        );
                        if self.next_index_map[&from] != 0{
                            self.next_index_map
                                .insert(from, self.next_index_map[&from] - 1);
                        }
                        self.replicate_log(network, Some(from));
                    }
                } else {
                    debug!("Server {} ignoring AppendEntriesResponse: not leader or term mismatch (current: {}, received: {})", self.instance_id, self.current_term, term);
                }
            }
            _ => unreachable!("Unexpected payload type for AppendEntriesResponse"),
        }
    }

    pub fn handle_transfer(
        &mut self,
        request: NetworkPayload,
        from_instance: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::Transfer { from, to, amount, transaction_id } => {
                // For intra-shard, initiate Raft consensus
                if self.datastore.kv_store.contains_key(&from) && self.datastore.kv_store.contains_key(&to) {
                    if *self.role.lock().unwrap() != ServerRole::Leader {
                        info!(
                            "Server {} not leader, redirecting to {}",
                            self.instance_id,
                            self.leader_id.unwrap()
                        );
                        network.send_message(NetworkEvent {
                            from: from_instance,
                            to: self.leader_id.unwrap(),
                            payload: request.serialize(),
                        });
                        return;
                    }

                    let sufficient_funds ;
                    if let Some(balance) = self.datastore.kv_store.get(&from) {
                        sufficient_funds = *balance >= amount;
                        info!(
                            "Server {} checked balance for {}: {} (needed {})",
                            self.instance_id, from, balance, amount
                        );
                    } else {
                        sufficient_funds = false;
                    }

                    if !sufficient_funds || !self.datastore.acquire_locks(vec![from, to]) {
                        network.send_message(NetworkEvent {
                            from: self.instance_id,
                            to: CLIENT_INSTANCE_ID,
                            payload: NetworkPayload::Ack {
                                transaction_id,
                                success: false,
                            }
                            .serialize(),
                        });
                        return;
                    } 

                    // Same cluster (shard)
                    let transaction = Transaction {
                        from,
                        to,
                        value: amount,
                        twopc_transaction_id: 0,
                        twopc_prepare: false,
                    };
                    let log_entry = LogEntry {
                        term: self.current_term,
                        index: self.datastore.log.len(),
                        command: transaction,
                    };
                    debug!(
                        "Server {} appending log entry for transfer in term {}",
                        self.instance_id, self.current_term
                    );
                    self.datastore.append_log(log_entry);
                    self.next_index_map
                        .insert(self.instance_id, self.datastore.log.len());
                    self.commit_index_map
                        .insert(self.instance_id, self.datastore.log.len());
                    self.datastore.add_pending_transaction(
                        transaction_id,
                        self.datastore.log.len()-1
                    );
                    self.replicate_log(network, None);
                }
            }
            _ => unreachable!("Unexpected payload type for Transfer"),
        }
    }

    pub fn become_leader(&mut self) {
        debug!(
            "Server {} becoming leader in term {}",
            self.instance_id, self.current_term
        );
        *self.role.lock().unwrap() = ServerRole::Leader;
    }

    pub fn replicate_log(&mut self, network: &mut Network, on: Option<u64>) {
        debug!(
            "Server {} (Leader) sending AppendEntries to cluster in term {}",
            self.instance_id, self.current_term
        );
        for server in &self.cluster_servers {
            if *server != self.instance_id && (on.is_none() || on.unwrap() == *server) {
                debug!(
                    "Server {} sending AppendEntries to server {} in term {}",
                    self.instance_id, server, self.current_term
                );
                let last_log = self.datastore.log_entry(self.next_index_map[&server]);
                let request = NetworkPayload::AppendEntries {
                    term: self.current_term,
                    leader_id: self.instance_id,
                    prev_log_index: if last_log.is_some() {
                        if last_log.unwrap().index == 0 {
                            None
                        } else {
                            Some(last_log.unwrap().index - 1)
                        }
                    } else {
                        if self.next_index_map[&server] == 0 {
                            None
                        } else {
                            Some(self.next_index_map[&server] - 1)
                        }
                    },
                    prev_log_term: if last_log.is_some() {
                        if last_log.unwrap().index == 0 {
                            None
                        } else {
                            Some(
                                self.datastore
                                    .log_entry((last_log.unwrap().index - 1) as usize)
                                    .unwrap()
                                    .term,
                            )
                        }
                    } else {
                        if self.next_index_map[&server] == 0 {
                            None
                        } else {
                            Some(
                                self.datastore
                                    .log_entry(self.next_index_map[&server] - 1)
                                    .unwrap()
                                    .term,
                            )
                        }
                    },
                    entries: if last_log.is_some() {
                        self.datastore.log_slice(self.next_index_map[&server])
                    } else {
                        vec![]
                    },
                    leader_commit: if self.datastore.committed_transactions.len() > 0 {
                        Some(self.datastore.committed_transactions.len() - 1)
                    } else {
                        None
                    },
                };
                network.send_message(NetworkEvent {
                    from: self.instance_id,
                    to: *server,
                    payload: request.serialize(),
                });
            }
        }
    }

    pub fn handle_prepare(
        &mut self,
        request: NetworkPayload,
        from_instance: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::Prepare {
                transaction_id,
                from,
                to,
                amount,
            } => {
                info!(
                    "Server {} handling Prepare for transaction {} from client {}",
                    self.instance_id, transaction_id, from_instance
                );
                if *self.role.lock().unwrap() != ServerRole::Leader {
                    info!(
                        "Server {} not leader, redirecting Prepare to {}",
                        self.instance_id,
                        self.leader_id.unwrap()
                    );
                    network.send_message(NetworkEvent {
                        from: from_instance,
                        to: self.leader_id.unwrap(),
                        payload: request.serialize(),
                    });
                    return;
                }

                let shard_start = ((self.instance_id - 1) / 3) * 1000 + 1;
                let shard_end = shard_start + 999;
                let mut locked_items = Vec::new();
                let mut sufficient_funds = true;

                if from >= shard_start && from <= shard_end {
                    locked_items.push(from);
                    if let Some(balance) = self.datastore.kv_store.get(&from) {
                        sufficient_funds = *balance >= amount;
                        info!(
                            "Server {} checked balance for {}: {} (needed {})",
                            self.instance_id, from, balance, amount
                        );
                    } else {
                        sufficient_funds = false;
                    }
                }
                if to >= shard_start && to <= shard_end {
                    locked_items.push(to);
                }

                if !sufficient_funds
                    || !self
                        .datastore
                        .acquire_locks(locked_items.clone())
                {
                    info!("Server {} aborting Prepare for transaction {}: insufficient funds or locks unavailable", self.instance_id, transaction_id);
                    network.send_message(NetworkEvent {
                        from: self.instance_id,
                        to: from_instance,
                        payload: NetworkPayload::PrepareResponse {
                            transaction_id,
                            success: false,
                        }
                        .serialize(),
                    });
                    self.datastore.release_locks(locked_items);
                    return;
                }

                let transaction = Transaction {
                    from,
                    to,
                    value: amount,
                    twopc_transaction_id: transaction_id,
                    twopc_prepare: true,
                };
                let log_entry = LogEntry {
                    term: self.current_term,
                    index: self.datastore.log.len(),
                    command: transaction,
                };
                debug!(
                    "Server {} appending log entry for transaction {} at index {}",
                    self.instance_id, transaction_id, log_entry.index
                );
                self.datastore.append_log(log_entry);
                self.datastore.add_pending_transaction(
                    transaction_id,
                    self.datastore.log.len()-1
                );
                self.next_index_map
                    .insert(self.instance_id, self.datastore.log.len());
                self.commit_index_map
                    .insert(self.instance_id, self.datastore.log.len());
                self.replicate_log(network, None);

                info!(
                    "Server {} initiated prepare for transaction {}, awaiting consensus",
                    self.instance_id, transaction_id
                );
            }
            _ => unreachable!("Unexpected payload type for Prepare"),
        }
    }

    pub fn handle_commit(
        &mut self,
        request: NetworkPayload,
        from_instance: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::Commit { transaction_id } => {
                if *self.role.lock().unwrap() != ServerRole::Leader {
                    info!(
                        "Server {} not leader, redirecting Commit to {}",
                        self.instance_id,
                        self.leader_id.unwrap()
                    );
                    network.send_message(NetworkEvent {
                        from: from_instance,
                        to: self.leader_id.unwrap(),
                        payload: request.serialize(),
                    });
                    return;
                }
                if let Some(&pending_index) = self.datastore.pending_transactions.get(&transaction_id) {
                    let mut transaction = self.datastore.log_entry(pending_index).unwrap().command;
                    transaction.twopc_prepare = false;
                    let log_entry = LogEntry {
                        term: self.current_term,
                        index: self.datastore.log.len(),
                        command: transaction,
                    };

                    debug!(
                        "Server {} appending log entry for transaction {} at index {}",
                        self.instance_id, transaction_id, log_entry.index
                    );
                    self.datastore.append_log(log_entry);
                    self.next_index_map
                        .insert(self.instance_id, self.datastore.log.len());
                    self.commit_index_map
                        .insert(self.instance_id, self.datastore.log.len());
                    *self.datastore.pending_transactions.get_mut(&transaction_id).unwrap()=self.datastore.log.len()-1;
                    self.replicate_log(network, None);
                }
            }
            _ => unreachable!("Unexpected payload type for Commit"),
        }
    }

    pub fn handle_abort(
        &mut self,
        request: NetworkPayload,
        from_instance: u64,
        network: &mut Network,
    ) {
        match request {
            NetworkPayload::Abort { transaction_id } => {
                if *self.role.lock().unwrap() != ServerRole::Leader {
                    info!(
                        "Server {} not leader, redirecting Abort to {}",
                        self.instance_id,
                        self.leader_id.unwrap_or(0)
                    );
                    network.send_message(NetworkEvent {
                        from: from_instance,
                        to: self.leader_id.unwrap_or(0),
                        payload: request.serialize(),
                    });
                    return;
                }

                if let Some(&pending_index) = self.datastore.pending_transactions.get(&transaction_id) {
                    if self.datastore.log_entry(pending_index).unwrap().command.twopc_prepare == false {
                        return;
                    }
                    let mut transaction = self.datastore.log_entry(pending_index).unwrap().command;
                    transaction.value = 0;
                    transaction.twopc_prepare = false;
                    let log_entry = LogEntry {
                        term: self.current_term,
                        index: self.datastore.log.len(),
                        command: transaction,
                    };

                    debug!(
                        "Server {} appending log entry for transaction {} at index {}",
                        self.instance_id, transaction_id, log_entry.index
                    );
                    self.datastore.append_log(log_entry);
                    self.next_index_map
                        .insert(self.instance_id, self.datastore.log.len());
                    self.commit_index_map
                        .insert(self.instance_id, self.datastore.log.len());
                    *self.datastore.pending_transactions.get_mut(&transaction_id).unwrap()=self.datastore.log.len()-1;
                    self.replicate_log(network, None);
                } else {
                    network.send_message(NetworkEvent {
                        from: self.instance_id,
                        to: CLIENT_INSTANCE_ID,
                        payload: NetworkPayload::Ack {
                            transaction_id,
                            success: false,
                        }
                        .serialize(),
                    });
                }
            }
            _ => unreachable!("Unexpected payload type for Abort"),
        }
    }
}
