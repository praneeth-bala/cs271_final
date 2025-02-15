use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::network::Network;
use cs271_final::utils::network::Message;

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

    let (sender, receiver): (Sender<Message>, Receiver<Message>) = mpsc::channel();
    let mut network = Network::new(instance_id, sender.clone(), false);
    if !network.connect_to_proxy(3000) { process::exit(1); }
    thread::spawn(move || {
        handle_incoming_events(
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
fn handle_incoming_events(mut network: Network, receiver: Receiver<Message>, instance_id: u64){
    let mut datastore: DataStore = DataStore::load(instance_id);

    loop {
        match receiver.recv() {
            Ok(message) => {
                network.send_message(message);
            }
            Err(_) => {
                println!("mpsc channel closed");
                break;
            }
        }
    }
}
