use cs271_final::utils::constants::CLIENT_INSTANCE_ID;
use cs271_final::utils::constants::PROXY_PORT;
use cs271_final::utils::datastore::DataStore;
use cs271_final::utils::event::Event;
use cs271_final::utils::event::LocalEvent;
use cs271_final::utils::event::LocalPayload;
use cs271_final::utils::event::NetworkEvent;
use cs271_final::utils::event::NetworkPayload;
use cs271_final::utils::network::Network;

use std::sync::mpsc::{self, Receiver, Sender};
use std::{io, process, thread};

fn main() {
    let (sender, receiver): (Sender<Event>, Receiver<Event>) = mpsc::channel();
    let mut network = Network::new(CLIENT_INSTANCE_ID, sender.clone(), false);
    if !network.connect_to_proxy(PROXY_PORT) {
        process::exit(1);
    }
    thread::spawn(move || {
        handle_events(network, receiver);
    });

    println!("Client started!");

    loop {
        println!("\nMenu Options:");
        println!("1. Print Balance");
        println!("2. Print Datastore");
        println!("3. Transfer");
        println!("4. Exit");

        let mut choice = String::new();
        io::stdin().read_line(&mut choice).unwrap();
        let choice = choice.trim();

        match choice {
            "1" => {
                println!("Enter client ID:");
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap();

                if let Ok(id) = id_input.trim().parse::<u64>() {
                    sender
                        .send(Event::Local(LocalEvent {
                            payload: LocalPayload::PrintBalance { id },
                        }))
                        .expect("Failed to send print-balance event");
                } else {
                    println!("Invalid client ID. Please enter a valid number.");
                }
            }
            "2" => {
                println!("Enter server ID:");
                let mut id_input = String::new();
                io::stdin().read_line(&mut id_input).unwrap();

                if let Ok(instance) = id_input.trim().parse::<u64>() {
                    sender
                        .send(Event::Local(LocalEvent {
                            payload: LocalPayload::PrintDatastore { instance },
                        }))
                        .expect("Failed to send print-datastore event");
                } else {
                    println!("Invalid server ID. Please enter a valid number.");
                }
            }
            "3" => {
                println!("Enter sender ID:");
                let mut from_input = String::new();
                io::stdin().read_line(&mut from_input).unwrap();
                let from = from_input.trim().parse::<u64>();

                println!("Enter receiver ID:");
                let mut to_input = String::new();
                io::stdin().read_line(&mut to_input).unwrap();
                let to = to_input.trim().parse::<u64>();

                println!("Enter amount:");
                let mut amount_input = String::new();
                io::stdin().read_line(&mut amount_input).unwrap();
                let amount = amount_input.trim().parse::<i64>();

                match (from, to, amount) {
                    (Ok(from), Ok(to), Ok(amount)) => {
                        sender
                            .send(Event::Local(LocalEvent {
                                payload: LocalPayload::Transfer { from, to, amount },
                            }))
                            .expect("Failed to send transfer event");
                    }
                    _ => println!("Invalid input. Please enter valid numbers for all fields."),
                }
            }
            "4" => {
                println!("Exiting...");
                break;
            }
            _ => println!("Invalid choice. Please select a number between 1 and 4."),
        }
    }
}

// TODO
fn handle_events(mut network: Network, receiver: Receiver<Event>) {
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
                                    network.send_message(NetworkEvent {
                                        from: CLIENT_INSTANCE_ID,
                                        to: instance,
                                        payload: NetworkPayload::PrintBalance { id: id }
                                            .serialize(),
                                    });
                                }
                            }
                            LocalPayload::PrintDatastore { instance } => {
                                network.send_message(NetworkEvent {
                                    from: CLIENT_INSTANCE_ID,
                                    to: instance,
                                    payload: NetworkPayload::PrintDatastore.serialize(),
                                });
                            }
                            LocalPayload::Transfer { from, to, amount } => {
                                if from / 1000 == to / 1000 {
                                    network.send_message(NetworkEvent {
                                        from: CLIENT_INSTANCE_ID,
                                        to: DataStore::get_random_instance_from_id(from),
                                        payload: NetworkPayload::Transfer { from, to, amount }
                                            .serialize(),
                                    });
                                }
                            },
                            _ => {}
                        }
                    }
                    Event::Network(message) => {
                        println!(
                            "Handling network event from {} to {}",
                            message.from, message.to
                        );
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
