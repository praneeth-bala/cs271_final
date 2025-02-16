use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Transaction {
    pub id: u64,
    pub value: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataStore {
    pub instance_id: u64,
    pub kv_store: HashMap<u64, i64>,
    pub committed_transactions: Vec<Transaction>,
}

impl DataStore {
    pub fn new(instance_id: u64) -> Self {
        Self {
            instance_id,
            kv_store: HashMap::new(),
            committed_transactions: Vec::new(),
        }
    }

    pub fn load(instance_id: u64) -> Self {
        let ds: DataStore;
        if let Some(new_ds) = DataStore::load_from_file(instance_id) {
            println!("Loaded existing datastore from file.");
            ds=new_ds;
        } else {
            println!("No existing datastore found. Creating new and initializing.");
            let mut new_ds = DataStore::new(instance_id);
            new_ds.init();
            new_ds.save_to_file();
            ds=new_ds;
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
            self.committed_transactions.push(Transaction { id, value });
        }
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
        if id<=1000 {
            rng.random_range(1..=3) as u64
        } else if id<=2000 {
            rng.random_range(4..=6) as u64
        } else {
            rng.random_range(7..=9) as u64
        }
    }

    pub fn get_all_instances_from_id(id: u64) -> Vec<u64> {
        if id<=1000 {
            vec![1,2,3]
        } else if id<=2000 {
            vec![4,5,6]
        } else {
            vec![7,8,9]
        }
    }
}
