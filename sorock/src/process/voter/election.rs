use super::*;

impl Voter {
    pub fn get_election_timeout(&self) -> Option<Duration> {
        // This is an optimization to avoid unnecessary election.
        // If the node doesn't contain itself in its membership,
        // it can't become a new leader anyway.
        if !self
            .peers
            .read_membership()
            .contains(&self.driver.self_node_id())
        {
            return None;
        }
        self.leader_failure_detector.get_election_timeout()
    }
}
