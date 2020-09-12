use std::collections::HashMap;
use std::sync::{Arc, Weak};
use tokio::sync::Notify;

pub struct News {
    id_alloc: u64,
    subscribers: HashMap<u64, Weak<Notify>>,
}
impl News {
    pub fn new() -> Self {
        Self {
            id_alloc: 0,
            subscribers: HashMap::new(),
        }
    }
    pub fn subscribe(&mut self) -> Subscriber {
        let receiver = Arc::new(Notify::new());
        let id = self.id_alloc;
        self.id_alloc += 1;
        let sender = Arc::downgrade(&receiver);
        self.subscribers.insert(id, sender);
        Subscriber { receiver }
    }
    pub fn publish(&mut self) {
        let mut obsolete_list = vec![];
        for (k, v) in &self.subscribers {
            match Weak::upgrade(v) {
                Some(noti) => {
                    noti.notify();
                }
                None => {
                    obsolete_list.push(*k);
                }
            }
        }
        for id in obsolete_list {
            self.subscribers.remove(&id);
        }
    }
}
pub struct Subscriber {
    receiver: Arc<Notify>,
}
impl Subscriber {
    pub async fn wait(&self) {
        self.receiver.notified().await
    }
}
