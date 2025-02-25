use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::Write;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub from: u64,
    pub to: u64,
    pub value: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PendingTransaction {
    pub transaction_id: u64,
    pub from: u64,
    pub to: u64,
    pub amount: i64,
    pub locked_items: Vec<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataStore {
    pub instance_id: u64,
    pub kv_store: BTreeMap<u64, i64>,
    pub committed_transactions: Vec<Transaction>,

    pub log: Vec<LogEntry>,

    pub locks: HashMap<u64, u64>, // Key: item_id, Value: transaction_id holding the lock
    pub pending_transactions: HashMap<u64, PendingTransaction>, // Key: transaction_id, Value: transaction details
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: usize,
    pub command: Transaction,
}

impl DataStore {
    pub fn new(instance_id: u64) -> Self {
        Self {
            instance_id,
            kv_store: BTreeMap::new(),
            committed_transactions: Vec::new(),
            log: Vec::new(),
            locks: todo!(),
            pending_transactions: todo!(),
        }
    }

    pub fn load(instance_id: u64) -> Self {
        let ds: DataStore;
        if let Some(new_ds) = DataStore::load_from_file(instance_id) {
            println!("Loaded existing datastore from file.");
            ds = new_ds;
        } else {
            println!("No existing datastore found. Creating new and initializing.");
            let mut new_ds = DataStore::new(instance_id);
            new_ds.init();
            new_ds.save_to_file();
            ds = new_ds;
        };
        ds
    }

    pub fn init(&mut self) {
        let start = (self.instance_id - 1) / 3 * 1000 + 1;
        let end = start + 999;
        for id in start..=end {
            self.kv_store.insert(id, 10);
        }
    }

    pub fn save_to_file(&self) {
        let filename = format!("server_{}_store.json", self.instance_id);
        let json = serde_json::to_string_pretty(self).unwrap();
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(filename)
            .unwrap();
        file.write_all(json.as_bytes()).unwrap();
    }

    pub fn load_from_file(instance_id: u64) -> Option<Self> {
        let filename = format!("server_{}_store.json", instance_id);
        if let Ok(content) = fs::read_to_string(&filename) {
            serde_json::from_str(&content).ok()
        } else {
            None
        }
    }

    pub fn update_value(&mut self, id: u64, value: i64) {
        if let Some(v) = self.kv_store.get_mut(&id) {
            *v = value;
        }
    }

    pub fn record_transaction(&mut self, from: u64, to: u64, value: i64) {
        self.committed_transactions
            .push(Transaction { from, to, value });
    }

    pub fn print_value(&self, id: u64) {
        if let Some(value) = self.kv_store.get(&id) {
            println!("Key {}: Value {}", id, value);
        } else {
            println!("Key {} not found on Server {}", id, self.instance_id);
        }
    }

    pub fn get_random_instance_from_id(id: u64) -> u64 {
        let mut rng = rand::rng();
        if id <= 1000 {
            rng.random_range(1..=3) as u64
        } else if id <= 2000 {
            rng.random_range(4..=6) as u64
        } else {
            rng.random_range(7..=9) as u64
        }
    }

    pub fn get_all_instances_from_id(id: u64) -> Vec<u64> {
        if id <= 1000 {
            vec![1, 2, 3]
        } else if id <= 2000 {
            vec![4, 5, 6]
        } else {
            vec![7, 8, 9]
        }
    }

    pub fn print_datastore(&self) {
        println!("Committed Transactions on Server {}:", self.instance_id);
        for tx in &self.committed_transactions {
            println!("From: {}, To: {}, Amount: {}", tx.from, tx.to, tx.value);
        }
    }

    pub fn process_transfer(&mut self, from: u64, to: u64, amount: i64) -> bool {
        if let Some(balance) = self.kv_store.get_mut(&from) {
            if *balance >= amount {
                *balance -= amount;
                *self.kv_store.entry(to).or_insert(0) += amount;
                self.record_transaction(from, to, amount);
                println!(
                    "Transaction successful: {} -> {} ({} units)",
                    from, to, amount
                );
                self.save_to_file();
                return true;
            }
        }
        println!("Transaction failed: insufficient funds or invalid account.");
        false
    }

    // Helper to append an entry to the log
    pub fn append_log(&mut self, entry: LogEntry) {
        self.log.push(entry);
        self.save_to_file();
    }

    // Helper to get log entry
    pub fn log_entry(&self, index: usize) -> Option<&LogEntry> {
        self.log.get(index)
    }

    pub fn log_slice(&self, start: usize) -> Vec<LogEntry> {
        self.log[start..].iter().cloned().collect()
    }

    // Helper to get the last log entry
    pub fn last_log_entry(&self) -> Option<&LogEntry> {
        self.log.last()
    }

    // Helper to check if log is consistent with leader's log
    pub fn log_is_consistent(
        &self,
        prev_log_index: &Option<usize>,
        prev_log_term: &Option<u64>,
    ) -> bool {
        if prev_log_index.is_none() {
            if self.log.len() == 0 {
                return true;
            } else {
                return false;
            }
        }
        if prev_log_index.unwrap() >= self.log.len() {
            return false;
        }
        let entry = &self.log[prev_log_index.unwrap()];
        entry.term == prev_log_term.unwrap()
    }

    // Check next index for each follower and commit transactions when atleast one log from current term is replicated on majority of servers
    pub fn calculate_latest_commit(
        &mut self,
        commit_index_map: &HashMap<u64, usize>,
        current_term: u64,
    ) {
        let mut indexes: Vec<usize> = commit_index_map.values().cloned().collect();
        indexes.sort();
        let commit_index = indexes[indexes.len() / 2];
        if commit_index > self.log.len() {
            return;
        }
        if commit_index != 0 && self.log_entry(commit_index - 1).unwrap().term != current_term {
            return;
        }
        println!(
            "Server {} applying committed entries up to index {}",
            self.instance_id, commit_index
        );
        for i in self.committed_transactions.len()..commit_index {
            if let Some(entry) = self.log.get(i) {
                if self.kv_store.contains_key(&entry.command.from)
                    && self.kv_store.contains_key(&entry.command.to)
                {
                    self.process_transfer(
                        entry.command.from,
                        entry.command.to,
                        entry.command.value,
                    );
                }
            }
        }
    }

    // Apply commit based on leader commit
    pub fn update_commit_from_index(&mut self, commit_index: &Option<usize>) {
        if commit_index.is_none() {
            return;
        }
        if commit_index.unwrap() >= self.log.len() {
            return;
        }
        println!(
            "Server {} applying committed entries up to index {}",
            self.instance_id,
            commit_index.unwrap()
        );
        for i in self.committed_transactions.len()..commit_index.unwrap() + 1 {
            if let Some(entry) = self.log.get(i) {
                if self.kv_store.contains_key(&entry.command.from)
                    && self.kv_store.contains_key(&entry.command.to)
                {
                    self.process_transfer(
                        entry.command.from,
                        entry.command.to,
                        entry.command.value,
                    );
                }
            }
        }
    }

    // New method to acquire locks for a transaction
    pub fn acquire_locks(&mut self, transaction_id: u64, items: Vec<u64>) -> bool {
        for &item in &items {
            if self.locks.contains_key(&item) {
                println!(
                    "Server {} failed to lock item {}: already locked",
                    self.instance_id, item
                );
                return false; // Lock unavailable
            }
        }
        // All locks available, acquire them
        for &item in &items {
            self.locks.insert(item, transaction_id);
        }
        println!(
            "Server {} acquired locks for items {:?} for transaction {}",
            self.instance_id, items, transaction_id
        );
        true
    }

    // New method to release locks for a transaction
    pub fn release_locks(&mut self, transaction_id: u64) {
        let mut locked_items = Vec::new();
        self.locks.retain(|item, tid| {
            if *tid == transaction_id {
                locked_items.push(*item);
                false // Remove the lock
            } else {
                true // Keep the lock
            }
        });
        println!(
            "Server {} released locks for items {:?} for transaction {}",
            self.instance_id, locked_items, transaction_id
        );
    }

    // New method to add a pending transaction
    pub fn add_pending_transaction(
        &mut self,
        transaction_id: u64,
        from: u64,
        to: u64,
        amount: i64,
        locked_items: Vec<u64>,
    ) {
        let pending = PendingTransaction {
            transaction_id,
            from,
            to,
            amount,
            locked_items,
        };
        self.pending_transactions.insert(transaction_id, pending);
        println!(
            "Server {} added pending transaction {}",
            self.instance_id, transaction_id
        );
        self.save_to_file();
    }

    // New method to commit a pending transaction
    pub fn commit_pending_transaction(&mut self, transaction_id: u64) -> bool {
        if let Some(pending) = self.pending_transactions.remove(&transaction_id) {
            if let Some(balance) = self.kv_store.get_mut(&pending.from) {
                if *balance >= pending.amount {
                    *balance -= pending.amount;
                    *self.kv_store.entry(pending.to).or_insert(0) += pending.amount;
                    self.record_transaction(pending.from, pending.to, pending.amount);
                    self.release_locks(transaction_id);
                    println!(
                        "Server {} committed transaction {}",
                        self.instance_id, transaction_id
                    );
                    self.save_to_file();
                    return true;
                }
            }
            println!(
                "Server {} failed to commit transaction {}: insufficient funds",
                self.instance_id, transaction_id
            );
            self.release_locks(transaction_id);
            self.save_to_file();
            return false;
        }
        println!(
            "Server {} found no pending transaction {} to commit",
            self.instance_id, transaction_id
        );
        false
    }

    // New method to abort a pending transaction
    pub fn abort_pending_transaction(&mut self, transaction_id: u64) {
        if self.pending_transactions.remove(&transaction_id).is_some() {
            self.release_locks(transaction_id);
            println!(
                "Server {} aborted transaction {}",
                self.instance_id, transaction_id
            );
            self.save_to_file();
        }
    }
}
