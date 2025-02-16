use cs271_final::utils::constants::CLIENT_INSTANCE_ID;
use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::Event;
use cs271_final::utils::event::LocalEvent;
use cs271_final::utils::event::LocalPayload;
use cs271_final::utils::event::NetworkEvent;
use cs271_final::utils::event::NetworkPayload;
use cs271_final::utils::network::Network;

use std::{thread, io, process};
use std::sync::mpsc::{self, Receiver, Sender};

fn main() {
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(CLIENT_INSTANCE_ID, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) { process::exit(1); }
    thread::spawn(move || {
        handle_events(
            network,
            receiver,
        );
    });

    println!("Client started!");

    // TODO client stuff
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        // Place print balance request for all instances holding given id
        let id: u64 = input.trim().parse().unwrap();
        sender.send(Event::Local(LocalEvent {
            payload: LocalPayload::PrintBalance { id: id }
        })).expect("Failed to send message to mpsc channel");
    }
}

// TODO
fn handle_events(mut network: Network, receiver: Receiver<Event>){
    loop {
        match receiver.recv() {
            Ok(event) => {
                match event {
                    Event::Local(message) => {
                        println!("Handling local event");
                        match message.payload {
                            LocalPayload::PrintBalance { id } => {
                                let instances = DataStore::get_all_instances_from_id(id);
                                for instance in instances {
                                    network.send_message(NetworkEvent{
                                        from: CLIENT_INSTANCE_ID,
                                        to: instance,
                                        payload: NetworkPayload::PrintBalance { id: id }.serialize()
                                    });
                                }
                            }
                        }
                    }
                    Event::Network(message) => {
                        println!("Handling network event from {} to {}", message.from, message.to);
                        // Handle network events if any
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
