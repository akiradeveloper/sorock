use super::*;

use rand::Rng;
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
        let null_node_id = NodeId(Uri::from_static("http://null-node"));
        let inner = Inner::watch(null_node_id);
        Self {
            inner: inner.into(),
        }
    }

    pub fn receive_heartbeat(&self, leader_id: NodeId) {
        let cur_watch_id = self.inner.lock().unwrap().watch_id.clone();
        if cur_watch_id != leader_id {
            *self.inner.lock().unwrap() = Inner::watch(leader_id);
        }
        self.inner.lock().unwrap().detector.add_ping(Instant::now())
    }

    /// Get the wait time before becoming a candidate.
    /// Returns None if the current leader is still considered alive.
    pub fn get_election_timeout(&self) -> Option<Duration> {
        let fd = &self.inner.lock().unwrap().detector;
        let normal_dist = fd.normal_dist();

        let detected = {
            let phi = normal_dist.phi(Instant::now() - fd.last_ping());
            // Akka suggests threshold is set to 12 in cloud environment
            // so we take it as a sane default here.
            // https://doc.akka.io/docs/akka/current/typed/failure-detector.html
            let threshold = 12.;
            phi > threshold
        };
        if !detected {
            return None;
        }

        // Timeout is randomized to avoid multiple followers try to promote simultaneously.
        // Here, the number is chosen in range [0, 4*mu]. The reason is as follows:
        // In this case, the average difference of two random numbers is mu,
        // which is the average interval of the heartbeat.
        // This means two random timeouts are sufficiently distant and it mitigates the risk
        // that two promotions conflict.
        let mut rng = rand::thread_rng();
        let mu = normal_dist.mu().as_millis() as u64;
        let random_timeout = Duration::from_millis(rng.gen_range(0..=4 * mu));

        Some(random_timeout)
    }
}
