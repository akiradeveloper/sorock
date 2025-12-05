use super::*;

pub struct Effect {
    pub voter: Voter,
    pub peers: Peers,
    pub state_mechine: StateMachine,
}
impl Effect {
    /// Try to become a leader.
    pub async fn exec(self, force_vote: bool) -> Result<()> {
        info!("try to promote to leader (force={force_vote})");

        let _lk = self.voter.vote_lock.lock().await;

        let pre_vote_term = {
            let ballot = self.voter.read_ballot().await?;
            ballot.cur_term + 1
        };

        info!("start pre-vote. try promote at term {pre_vote_term}");
        let ok = self
            .request_votes(pre_vote_term, force_vote, true)
            .await
            .unwrap_or(false);
        if !ok {
            info!("pre-vote failed for term {pre_vote_term}");
            return Ok(());
        }
        // --- END pre-vote ---

        let vote_term = {
            let mut new_ballot = self.voter.read_ballot().await?;
            let vote_term = new_ballot.cur_term + 1;
            ensure!(vote_term == pre_vote_term);

            // Vote to itself
            new_ballot.cur_term = vote_term;
            new_ballot.voted_for = Some(self.voter.driver.self_node_id());
            self.voter.write_ballot(new_ballot).await?;

            // Becoming Candidate avoids this node starts another election during this election.
            self.voter.write_election_state(ElectionState::Candidate);
            vote_term
        };

        info!("start election. try promote at term {vote_term}");

        // Try to promote at the term.
        // Failing some I/O operations during election will be considered as election failure.
        let ok = self
            .request_votes(vote_term, force_vote, false)
            .await
            .unwrap_or(false);

        self.post_election(vote_term, ok).await?;

        Ok(())
    }

    /// Request votes to the peers in the cluster and returns whether it got enough votes.
    async fn request_votes(
        &self,
        vote_term: Term,
        force_vote: bool,
        pre_vote: bool,
    ) -> Result<bool> {
        let (others, remaining) = {
            let membership = self.peers.read_membership();
            ensure!(membership.contains(&self.voter.driver.self_node_id()));

            let n = membership.len();
            let mut others = vec![];
            for id in membership {
                if id != self.voter.driver.self_node_id() {
                    others.push(id);
                }
            }

            let majority = n / 2 + 1;
            (others, majority - 1)
        };

        let log_last_clock = {
            let last_log_index = self.state_mechine.get_log_last_index().await?;
            self.state_mechine
                .get_entry(last_log_index)
                .await?
                .this_clock
        };

        // Get remaining votes from others.
        let mut vote_requests = vec![];
        for endpoint in others {
            let selfid = self.voter.driver.self_node_id();
            let conn = self.voter.driver.connect(endpoint);
            vote_requests.push(async move {
                let req = request::RequestVote {
                    candidate_id: selfid,
                    candidate_clock: log_last_clock,
                    vote_term,
                    // $3.2.3
                    // If force_vote is set, the receiver server accepts the vote request
                    // regardless of the heartbeat timeout otherwise the vote request is
                    // dropped when it's receiving heartbeat.
                    force_vote,
                    // $8.6 Preventing disruptions when a server rejoins the cluster
                    // We recommend the Pre-Vote extension in deployments that would benefit from additional robustness.
                    pre_vote,
                };
                let resp = conn.request_vote(req).await;
                match resp {
                    Ok(granted) => granted,
                    Err(_) => false,
                }
            });
        }

        let ok = quorum::join(remaining, vote_requests).await;
        Ok(ok)
    }

    async fn post_election(&self, vote_term: Term, ok: bool) -> Result<()> {
        if ok {
            info!("got enough votes from the cluster. promoted to leader");

            // As soon as the node becomes the leader, replicate noop entries with term.
            let index = state_machine::effect::append_new_entry::Effect {
                state_mechine: self.state_mechine.clone(),
            }
            .exec(
                Command::serialize(Command::Barrier(vote_term)),
                Some(vote_term),
            )
            .await?;

            info!("noop barrier is queued at index({index}) (term={vote_term})");

            // Initialize replication progress
            peers::effect::reset_progress::Effect {
                peers: self.peers.clone(),
            }
            .exec(index);

            self.voter.write_election_state(ElectionState::Leader);
        } else {
            info!("failed to become leader. now back to follower");
            self.voter.write_election_state(ElectionState::Follower);
        }
        Ok(())
    }
}
