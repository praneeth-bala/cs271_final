use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::{Event, NetworkPayload};
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
}

impl RaftServer {
    fn new(instance_id: u64) -> Self {
        RaftServer {
            role: ServerRole::Follower,
            datastore: DataStore::load(instance_id),
            election_timeout: Some(Duration::from_millis(150 + rand::random::<u64>() % 150)), // Random timeout between 150-300ms
            heartbeat_timeout: Some(Duration::from_millis(50)), // Heartbeat every 50ms
        }
    }

    fn start_election(&mut self) {
        self.role = ServerRole::Candidate;
        self.datastore.current_term += 1;
        self.datastore.voted_for = Some(self.datastore.instance_id);
        // TODO: Send RequestVote RPCs to other servers in cluster
    }

    fn step_down(&mut self) {
        self.role = ServerRole::Follower;
        self.datastore.voted_for = None;
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

// TODO
fn handle_events(mut network: Network, receiver: Receiver<Event>, mut raft_server: RaftServer) {
    let mut last_election_time = std::time::Instant::now();
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(_) => {
                        println!("Handling local event");
                        // Handle local events if any (currently not used in server)
                    }
                    Event::Network(message) => {
                        let payload = NetworkPayload::deserialize(message.payload)
                            .expect("Failed to deserialize payload");
                        match payload {
                            NetworkPayload::PrintBalance { id } => {
                                let instances = DataStore::get_all_instances_from_id(id);
                                for instance in instances {
                                    if instance == raft_server.datastore.instance_id {
                                        raft_server.datastore.print_value(id); // can we not jsut do this? instead of whole for loop? TODO: test
                                    }
                                }
                            }
                            NetworkPayload::PrintDatastore => {
                                raft_server.datastore.print_datastore();
                            }
                            NetworkPayload::Transfer { from, to, amount } => {
                                // For intra-shard, initiate Raft consensus
                                if from / 1000 == to / 1000 { // Same cluster (shard)
                                    let transaction = cs271_final::utils::datastore::Transaction { from, to, value: amount };
                                    let log_entry = cs271_final::utils::datastore::LogEntry {
                                        term: raft_server.datastore.current_term,
                                        index: raft_server.datastore.log.len() as u64 + 1,
                                        command: transaction,
                                    };
                                    raft_server.datastore.append_log(log_entry);
                                    if raft_server.role == ServerRole::Leader {
                                        // As leader, replicate log entry to followers
                                        // TODO: Implement AppendEntries RPC (next step)
                                    } else {
                                        // Not leader, need to forward to leader or start election
                                        // For now, we'll handle election timeout
                                    }
                                } else {
                                    // Cross-shard, handle with 2PC (to be implemented later)
                                    todo!("Implement 2PC for cross-shard transactions");
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
                raft_server.start_election();
                last_election_time = std::time::Instant::now();
                // TODO: Send RequestVote RPCs to other servers in cluster (next step)
            }
        }

        // Check heartbeat timeout (Leader behavior)
        if raft_server.role == ServerRole::Leader {
            if last_election_time.elapsed() >= raft_server.heartbeat_timeout.unwrap() {
                // TODO: Send AppendEntries RPCs as heartbeats (next step)
                last_election_time = std::time::Instant::now();
            }
        }
    }
}
