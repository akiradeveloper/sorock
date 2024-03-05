use super::*;

impl Voter {
    /// Returns grated or not on vote.
    pub async fn receive_vote_request(
        &self,
        candidate_term: Term,
        candidate_id: NodeId,
        candidate_last_log_clock: Clock,
        force_vote: bool,
        pre_vote: bool,
    ) -> Result<bool> {
        let _lk = self.vote_lock.lock().await;

        let allow_update_ballot = !pre_vote;

        // If it is a force-vote which is set by TimeoutNow,
        // or it believes the leader has failed, it should vote.
        let leader_failed = self.get_election_timeout().is_some();
        let should_vote = force_vote || leader_failed;
        if !should_vote {
            return Ok(false);
        }

        let mut ballot = self.read_ballot().await?;
        if candidate_term < ballot.cur_term {
            warn!("candidate term is older. reject vote");
            return Ok(false);
        }

        if candidate_term > ballot.cur_term {
            warn!("received newer term. reset vote");
            ballot.cur_term = candidate_term;
            ballot.voted_for = None;
            if allow_update_ballot {
                self.write_election_state(ElectionState::Follower);
            }
        }

        let last_log_clock = {
            let cur_last_index = self.command_log.get_log_last_index().await?;
            if cur_last_index == 0 {
                Clock { term: 0, index: 0 }
            } else {
                self.command_log.get_entry(cur_last_index).await?.this_clock
            }
        };

        let candidate_win = match candidate_last_log_clock.term.cmp(&last_log_clock.term) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => candidate_last_log_clock.index >= last_log_clock.index,
            std::cmp::Ordering::Less => false,
        };

        if !candidate_win {
            warn!("candidate clock is older. reject vote");
            if allow_update_ballot {
                self.write_ballot(ballot).await?;
            }
            return Ok(false);
        }

        let grant = match &ballot.voted_for {
            None => {
                info!("learn node@{candidate_id} as the new leader");
                ballot.voted_for = Some(candidate_id.clone());
                true
            }
            Some(id) => {
                if id == &candidate_id {
                    true
                } else {
                    // Only one grant vote is allowed for a term.
                    // This is why ballot needs to be persistent.
                    warn!("reject vote for having voted at term@{candidate_term}");
                    false
                }
            }
        };

        if allow_update_ballot {
            self.write_ballot(ballot).await?;
        }

        info!("voted response grant@{grant} to {candidate_id}");
        Ok(grant)
    }

    pub fn get_election_timeout(&self) -> Option<Duration> {
        // This is optimization to avoid unnecessary election.
        // If the node doesn't contain itself in its membership,
        // it won't become a new leader anyway.
        if !self
            .peers
            .read_membership()
            .contains(&self.driver.self_node_id())
        {
            return None;
        }
        self.leader_failure_detector.get_election_timeout()
    }

    pub async fn try_promote(&self, force_vote: bool) -> Result<()> {
        let _lk = self.vote_lock.lock().await;

        let pre_vote_term = {
            let ballot = self.read_ballot().await?;
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
            let mut new_ballot = self.read_ballot().await?;
            let vote_term = new_ballot.cur_term + 1;
            ensure!(vote_term == pre_vote_term);

            // Vote to itself
            new_ballot.cur_term = vote_term;
            new_ballot.voted_for = Some(self.driver.self_node_id());
            self.write_ballot(new_ballot).await?;

            // Becoming Candidate avoids this node starts another election during this election.
            self.write_election_state(ElectionState::Candidate);
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

    async fn request_votes(
        &self,
        vote_term: Term,
        force_vote: bool,
        pre_vote: bool,
    ) -> Result<bool> {
        let (others, remaining) = {
            let membership = self.peers.read_membership();
            ensure!(membership.contains(&self.driver.self_node_id()));

            let n = membership.len();
            let mut others = vec![];
            for id in membership {
                if id != self.driver.self_node_id() {
                    others.push(id);
                }
            }

            let majority = n / 2 + 1;
            (others, majority - 1)
        };

        let log_last_clock = {
            let last_log_index = self.command_log.get_log_last_index().await?;
            self.command_log.get_entry(last_log_index).await?.this_clock
        };

        // Let's get remaining votes out of others.
        let mut vote_requests = vec![];
        for endpoint in others {
            let selfid = self.driver.self_node_id();
            let conn = self.driver.connect(endpoint);
            vote_requests.push(async move {
                let req = request::RequestVote {
                    candidate_id: selfid,
                    candidate_clock: log_last_clock,
                    vote_term,
                    // $4.2.3
                    // If force_vote is set, the receiver server accepts the vote request
                    // regardless of the heartbeat timeout otherwise the vote request is
                    // dropped when it's receiving heartbeat.
                    force_vote,
                    // $9.6 Preventing disruptions when a server rejoins the cluster
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
            let index = self
                .command_log
                .append_new_entry(
                    Command::serialize(Command::Barrier(vote_term)),
                    Some(vote_term),
                )
                .await?;
            info!("noop barrier is queued@{index} (term={vote_term})");

            // Initialize replication progress
            self.peers.reset_progress(index);

            self.write_election_state(ElectionState::Leader);
        } else {
            info!("failed to become leader. now back to follower");
            self.write_election_state(ElectionState::Follower);
        }
        Ok(())
    }
}
