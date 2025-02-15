use cs271_final::utils::network::Network;
use cs271_final::utils::network::Message;

use std::{thread, io, process};
use std::sync::mpsc::{self, Receiver, Sender};

fn main() {
    // Client is instance 10
    let instance_id = 10;

    let (sender, receiver): (Sender<Message>, Receiver<Message>) = mpsc::channel();
    let mut network = Network::new(instance_id, sender.clone(), false);
    if !network.connect_to_proxy(3000) { process::exit(1); }
    thread::spawn(move || {
        handle_incoming_events(
            network,
            receiver,
        );
    });

    println!("Client started!");

    // TODO client stuff
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
}

// TODO
fn handle_incoming_events(mut network: Network, receiver: Receiver<Message>){
    loop {
        match receiver.recv() {
            Ok(message) => {

            }
            Err(_) => {
                println!("mpsc channel closed");
                break;
            }
        }
    }
}
