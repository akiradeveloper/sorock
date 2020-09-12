use futures::future::{AbortHandle, Abortable};
use std::future::Future;

/// mechanism to drop spawned thread when the owner drops.
#[derive(Debug)]
pub struct ThreadDrop {
    handles: Vec<AbortHandle>,
}
impl ThreadDrop {
    pub fn new() -> Self {
        Self { handles: vec![] }
    }
    pub fn register<F: Future>(&mut self, f: F) -> Abortable<F> {
        let (g, hdl) = abortable(f);
        self.handles.push(hdl);
        g
    }
}
impl Drop for ThreadDrop {
    fn drop(&mut self) {
        for x in &mut self.handles {
            x.abort();
        }
    }
}
fn abortable<F: Future>(fut: F) -> (Abortable<F>, AbortHandle) {
    let (hdl, reg) = AbortHandle::new_pair();
    let f = Abortable::new(fut, reg);
    (f, hdl)
}
