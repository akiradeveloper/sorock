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

    pub fn receive_heartbeat(&self, leader_id: NodeId) {
        let cur_watch_id = self.inner.lock().unwrap().watch_id.clone();
        if cur_watch_id != leader_id {
            *self.inner.lock().unwrap() = Inner::watch(leader_id);
        }
        self.inner.lock().unwrap().detector.add_ping(Instant::now())
    }

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
        let rand_timeout = {
            let max_width = normal_dist.sigma() * 4;
            let width = rand::random::<u128>() % max_width.as_millis();
            Duration::from_millis(width as u64)
        };
        // timeout is chosen in [mu, mu + 4 * sigma)
        let timeout = normal_dist.mu() + rand_timeout;
        Some(timeout)
    }
}
