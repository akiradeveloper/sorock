use super::*;

use rand::Rng;
use std::time::Instant;

struct Inner {
    watch_id: NodeId,
    detector: phi_detector::PingWindow,
    last_ping: Instant,
}
impl Inner {
    fn watch(id: NodeId) -> Self {
        Self {
            watch_id: id,
            detector: phi_detector::PingWindow::new(Duration::from_secs(1)),
            last_ping: Instant::now(),
        }
    }
}
pub struct FailureDetector {
    inner: spin::RwLock<Inner>,
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
        let mut inner = self.inner.write();
        let cur_watch_id = inner.watch_id.clone();
        if cur_watch_id != leader_id {
            *inner = Inner::watch(leader_id);
        }

        let now = Instant::now();
        let du = now - inner.last_ping;
        inner.detector.add_ping(du);
        inner.last_ping = now;
    }

    /// Get the wait time before becoming a candidate.
    /// Returns None if the current leader is still considered alive.
    pub fn get_election_timeout(&self) -> Option<Duration> {
        let inner = self.inner.read();

        let fd = &inner.detector;
        let normal_dist = fd.normal_dist();

        let detected = {
            let du = Instant::now() - inner.last_ping;
            let phi = normal_dist.phi(du);
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
        // Here, the number is chosen in range [0, 3*mu]. The reason is as follows:
        // In this case, the average difference of two random numbers is mu,
        // which is the average interval of the heartbeat.
        // This means two random timeouts are sufficiently distant and it mitigates the risk
        // that two promotions conflict.
        let mut rng = rand::thread_rng();
        let mu = normal_dist.mu().as_millis() as u64;
        let random_timeout = Duration::from_millis(rng.gen_range(0..=3 * mu));

        Some(random_timeout)
    }
}
