use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Table<A> {
    x: Arc<RwLock<HashMap<u64, A>>>,
}
impl<A> Table<A> {
    pub fn new() -> Self {
        Self {
            x: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn insert(&self, i: u64, x: A) {
        let mut y = self.x.write().await;
        y.insert(i, x);
    }
    pub async fn get<B>(&self, i: u64, f: impl Fn(&A) -> B) -> Option<B> {
        let y = self.x.read().await;
        y.get(&i).map(f)
    }
    pub async fn remove(&self, i: u64) {
        let mut y = self.x.write().await;
        y.remove(&i);
    }
}

// experimental
// pub struct Table<A> {
//     x: dashmap::DashMap<u64, A>,
// }
// impl <A: 'static> Table<A> {
//     pub fn new() -> Self {
//         Self {
//             x: dashmap::DashMap::new(),
//         }
//     }
//     pub async fn insert(&self, i: u64, x: A) {
//         self.x.insert(i, x);
//     }
//     pub async fn get<B>(&self, i: u64, f: impl Fn(&A) -> B) -> Option<B> {
//         self.x.get(&i).map(|x| f(x.value()))
//     }
//     pub async fn remove(&self, i: u64) {
//         self.x.remove(&i);
//     }
// }

#[cfg(test)]
mod tests {
    use dashmap::DashMap;
    #[test]
    fn test_dashmap_0() {
        let m = DashMap::new();
        for i in 0..1000000 {
            let x = m.get(&0);
            m.insert(0, i);
            let v = m.get(&0).unwrap().value().clone();
            assert_eq!(v, i);
        }
    }
    #[test]
    fn test_dashmap_1() {
        let m = DashMap::new();
        m.insert(0, 1);
        if let Some(x) = m.get(&0) {
            m.insert(0, 2);
        }
        let v = m.get(&0).unwrap().value().clone();
        assert_eq!(v, 2);
    }
    #[test]
    fn test_dashmap_2() {
        let m = DashMap::new();
        m.insert(0, 1);
        for _ in 0..1000000 {
            let x = rand::random();
            let ok = m.insert(0, x);
            assert!(ok);
        }
    }
    #[test]
    fn test_dashmap_3() {
        let m = DashMap::new();
        let w = 79;
        for k in 0..w {
            m.insert(k, 1);
        }
        for i in 1..10000 {
            for k in w * i..w * (i + 1) {
                m.insert(k, 1);
            }
            let mut sum = 0;
            for k in w * (i - 1)..w * i {
                sum += *m.get(&k).unwrap().value();
            }
            assert!(m.insert(w * i, sum));
            for k in w * (i - 1)..w * i {
                let ok = m.remove(&k);
                assert!(ok);
            }
        }
    }
}
