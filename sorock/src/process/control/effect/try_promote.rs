use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
    pub command_log: Actor<CommandLog>,
}

impl Effect<'_> {
    /// Try to become a leader.
    pub async fn exec(mut self, force_vote: bool) -> Result<()> {
        ensure!(matches!(self.ctrl.state, ElectionState::Follower));

        info!("try to promote to leader (force={force_vote})");

        let pre_vote_term = {
            let ballot = self.ctrl.read_ballot().await?;
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
            let mut new_ballot = self.ctrl.read_ballot().await?;
            let vote_term = new_ballot.cur_term + 1;
            ensure!(vote_term == pre_vote_term);

            // Vote to itself
            new_ballot.cur_term = vote_term;
            new_ballot.voted_for = Some(self.ctrl.io.local_server_id.clone());
            self.ctrl.write_ballot(new_ballot).await?;

            // Becoming Candidate avoids this node starts another election during this election.
            self.ctrl.write_election_state(ElectionState::Candidate);
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
        let membership = self.ctrl.read_membership();

        let voters = {
            let mut out = vec![];
            for (id, is_voter) in membership {
                if is_voter {
                    out.push(id);
                }
            }
            out
        };

        ensure!(voters.contains(&self.ctrl.io.local_server_id));
        let (others, remaining) = {
            let n = voters.len();

            let mut others = vec![];
            for voter in voters {
                if voter != self.ctrl.io.local_server_id {
                    others.push(voter);
                }
            }

            (others, n / 2)
        };

        let log_last_clock = {
            let last_log_index = self.command_log.read().await.tail_pointer;
            self.command_log
                .read()
                .await
                .get_entry(last_log_index)
                .await?
                .this_clock
        };

        // Get remaining votes from others.
        let mut vote_requests = vec![];
        for endpoint in others {
            let local_id = self.ctrl.io.local_server_id.clone();
            let conn = self.ctrl.io.connect(&endpoint);
            vote_requests.push(async move {
                let req = request::RequestVote {
                    candidate_id: local_id,
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

    async fn post_election(&mut self, vote_term: Term, ok: bool) -> Result<()> {
        if ok {
            info!("got enough votes from the cluster. promoted to leader");

            // As soon as the node becomes the leader, replicate noop entries with term.
            let index = command_log::effect::append_entry::Effect {
                command_log: &mut *self.command_log.write().await,
            }
            .exec(
                Command::serialize(Command::TermBarrier(vote_term)),
                Some(vote_term),
                None,
            )
            .await?;

            info!("noop barrier is queued at index({index}) (term={vote_term})");

            // Initialize replication progress
            self.reset_replication_state(index).await;

            self.ctrl.write_election_state(ElectionState::Leader);
        } else {
            info!("failed to become leader. now back to follower");
            self.ctrl.write_election_state(ElectionState::Follower);
        }
        Ok(())
    }

    async fn reset_replication_state(&mut self, init_next_index: LogIndex) {
        for (_, cur_progress) in &mut self.ctrl.replication_contexts {
            *cur_progress.write().await = Replication::new(init_next_index);
        }
    }
}
