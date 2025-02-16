use serde::{Serialize, Deserialize};
use bincode;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkEvent {
    pub from: u64,
    pub to: u64,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum NetworkPayload {
    PrintBalance { id: u64 }
}

impl NetworkPayload {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize RaftPayload")
    }

    pub fn deserialize(data: Vec<u8>) -> Result<Self, bincode::Error> {
        bincode::deserialize(&data)
    }
}

pub struct LocalEvent {
    pub payload: LocalPayload
}

pub enum LocalPayload {
    PrintBalance { id: u64 }
}

pub enum Event {
    Local(LocalEvent),
    Network(NetworkEvent),
}
