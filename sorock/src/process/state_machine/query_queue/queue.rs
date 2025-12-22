use super::*;

pub type QueryQueue = Arc<parking_lot::Mutex<QueryQueueRaw>>;

pub struct QueryQueueRaw {
    inner: Vec<Query>,
}
impl QueryQueueRaw {
    pub fn new() -> Self {
        Self { inner: vec![] }
    }

    pub fn queue(&mut self, q: Query) -> Result<()> {
        self.inner.push(q);
        Ok(())
    }

    pub fn drain(&mut self) -> Vec<Query> {
        let out = self.inner.drain(..).collect();
        out
    }

    pub fn requeue(&mut self, qs: Vec<Query>) {
        for q in qs {
            self.inner.push(q);
        }
    }
}
