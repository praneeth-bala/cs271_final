use bincode;
use serde::{Deserialize, Serialize};

use super::datastore::LogEntry;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkEvent {
    pub from: u64,
    pub to: u64,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum NetworkPayload {
    PrintBalance {
        id: u64,
    },
    PrintDatastore,
    Transfer {
        from: u64,
        to: u64,
        amount: i64,
    },

    RequestVote {
        term: u64,           // Candidate's term
        candidate_id: u64,   // Candidate requesting vote
        last_log_index: u64, // Index of candidate's last log entry
        last_log_term: u64,  // Term of candidate's last log entry
    },
    VoteResponse {
        term: u64,          // Current term for the responder
        vote_granted: bool, // True if vote is granted
    },
    AppendEntries {
        term: u64,                     // Leader's term
        leader_id: u64,                // So follower can redirect clients
        prev_log_index: Option<usize>, // Index of log entry immediately preceding new ones
        prev_log_term: Option<u64>,    // Term of prevLogIndex entry
        entries: Vec<LogEntry>,        // Log entries to store (empty for heartbeat)
        leader_commit: Option<usize>,  // Leader's commitIndex
    },
    AppendEntriesResponse {
        term: u64,         // Current term for the responder
        success: bool,     // True if follower contained matching entries
        next_index: usize, // Latest index of the follower
    },

    Prepare {
        transaction_id: u64,
        from: u64,
        to: u64,
        amount: i64,
    },
    PrepareResponse {
        transaction_id: u64,
        success: bool,
    },
    Commit {
        transaction_id: u64,
    },
    Abort {
        transaction_id: u64,
    },
    Ack {
        transaction_id: u64,
        success: bool,
    },
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
    pub payload: LocalPayload,
}

#[derive(Debug)]

pub enum LocalPayload {
    PrintBalance {
        id: u64,
    },
    PrintDatastore {
        instance: u64,
    },
    Transfer {
        from: u64,
        to: u64,
        amount: i64,
    },

    SendHeartbeat,
    StartElection,
    CheckAbort,

    HandlePrepare {
        transaction_id: u64,
        from: u64,
        to: u64,
        amount: i64,
    },
    HandleCommit {
        transaction_id: u64,
    },
    HandleAbort {
        transaction_id: u64,
    },

    Start2PC {
        transaction_id: u64,
        from: u64,
        to: u64,
        amount: i64,
    },

    PrepareCluster { transaction_id: u64, target: u64, from: u64, to: u64, amount: i64 },
}

pub enum Event {
    Local(LocalEvent),
    Network(NetworkEvent),
}
