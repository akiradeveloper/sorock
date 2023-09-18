use super::*;
use std::sync::Mutex;
use std::time::Instant;

struct Inner {
    watch_id: NodeId,
    detector: phi_detector::PingWindow,
}
impl Inner {
    fn watch(id: NodeId) -> Self {
        Self {
            watch_id: id,
            detector: phi_detector::PingWindow::new(&[Duration::from_secs(1)], Instant::now()),
        }
    }
}
pub struct FailureDetector {
    inner: Mutex<Inner>,
}
impl FailureDetector {
    pub fn new() -> Self {
        let init_id = NodeId(Uri::from_static("https://xrp.price:589"));
        let inner = Inner::watch(init_id);
        Self {
            inner: inner.into(),
        }
    }

    fn get_watch_id(&self) -> NodeId {
        self.inner.lock().unwrap().watch_id.clone()
    }

    pub fn receive_heartbeat(&self, leader_id: NodeId) {
        let cur_watch_id = self.inner.lock().unwrap().watch_id.clone();
        if cur_watch_id != leader_id {
            *self.inner.lock().unwrap() = Inner::watch(leader_id);
        }
        self.inner.lock().unwrap().detector.add_ping(Instant::now())
    }

    fn detect_election_timeout(&self) -> bool {
        let fd = &self.inner.lock().unwrap().detector;
        let normal_dist = fd.normal_dist();
        let phi = normal_dist.phi(Instant::now() - fd.last_ping());
        phi > 3.
    }

    pub fn get_election_timeout(&self) -> Option<Duration> {
        let fd = &self.inner.lock().unwrap().detector;
        let normal_dist = fd.normal_dist();

        let detected = {
            let phi = normal_dist.phi(Instant::now() - fd.last_ping());
            phi > 3.
        };
        if !detected {
            return None;
        }

        let base_timeout = (normal_dist.mu() + normal_dist.sigma() * 4).as_millis();
        let rand_timeout = rand::random::<u128>() % base_timeout;
        Some(Duration::from_millis(rand_timeout as u64))
    }
}
