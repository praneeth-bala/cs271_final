use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::{Event, NetworkPayload};
use cs271_final::utils::network::Network;

use std::{env, io};
use std::{process, thread};
use std::sync::mpsc::{self, Receiver, Sender};

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
    if !network.connect_to_proxy(PROXY_PORT) { process::exit(1); }
    thread::spawn(move || {
        handle_events(
            network,
            receiver,
            instance_id
        );
    });

    println!("Server {} started!", instance_id);

    // TODO
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
}

// TODO
fn handle_events(_: Network, receiver: Receiver<Event>, instance_id: u64){
    let datastore: DataStore = DataStore::load(instance_id);
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(_) => {
                        println!("Handling local event");
                        // Handle local events if any
                    }
                    Event::Network(message) => {
                        let payload = NetworkPayload::deserialize(message.payload).expect("Failed to deserialize payload");
                        match payload {
                            NetworkPayload::PrintBalance { id } => {
                                datastore.print_value(id);
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
