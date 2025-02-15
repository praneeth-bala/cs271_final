use serde::{Serialize, Deserialize};
use bincode;

// Example types (these are just placeholders)
#[derive(Serialize, Deserialize, Debug)]
pub enum Payload {
    RequestVote { term: u64, candidate_id: u64 },
    VoteResponse { term: u64, vote_granted: bool },
    AppendEntries { term: u64, leader_id: u64, entries: Vec<String> },
    AppendResponse { term: u64, success: bool },
}

impl Payload {
    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize RaftPayload")
    }

    pub fn deserialize(data: Vec<u8>) -> Result<Self, bincode::Error> {
        bincode::deserialize(&data)
    }
}
