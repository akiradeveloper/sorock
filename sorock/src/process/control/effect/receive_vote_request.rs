use super::*;

pub struct Effect<'a> {
    pub ctrl: &'a mut Control,
}

impl Effect<'_> {
    fn command_log(&self) -> &Actor<CommandLog> {
        &self.ctrl.command_log_actor
    }

    /// Returns grated or not on vote.
    pub async fn exec(
        self,
        candidate_term: Term,
        candidate_id: ServerAddress,
        candidate_last_log_clock: Clock,
        force_vote: bool,
        pre_vote: bool,
    ) -> Result<bool> {
        let allow_update_ballot = !pre_vote;

        // If it is a force-vote which is set by TimeoutNow,
        // or it believes the leader has failed, it should vote.
        let leader_failed = self.ctrl.get_election_timeout().is_some();
        let should_vote = force_vote || leader_failed;
        if !should_vote {
            return Ok(false);
        }

        let mut ballot = self.ctrl.read_ballot().await?;
        if candidate_term < ballot.cur_term {
            warn!("candidate term is older. reject vote");
            return Ok(false);
        }

        if candidate_term > ballot.cur_term {
            warn!("received newer term. reset vote");
            ballot.cur_term = candidate_term;
            ballot.voted_for = None;
            if allow_update_ballot {
                self.ctrl.write_election_state(ElectionState::Follower);
            }
        }

        let last_log_clock = {
            let cur_last_index = self.command_log().read().await.get_log_last_index().await?;
            if cur_last_index == 0 {
                Clock { term: 0, index: 0 }
            } else {
                self.command_log()
                    .read()
                    .await
                    .get_entry(cur_last_index)
                    .await?
                    .this_clock
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
                self.ctrl.write_ballot(ballot).await?;
            }
            return Ok(false);
        }

        let grant = match &ballot.voted_for {
            None => {
                info!("learn node({candidate_id}) as the new leader");
                ballot.voted_for = Some(candidate_id.clone());
                true
            }
            Some(id) => {
                if id == &candidate_id {
                    true
                } else {
                    // Only one grant vote is allowed for a term.
                    // This is why ballot needs to be persistent.
                    warn!("reject vote for having voted at term({candidate_term})");
                    false
                }
            }
        };

        if allow_update_ballot {
            self.ctrl.write_ballot(ballot).await?;
        }

        info!("voted response grant({grant}) to node({candidate_id})");
        Ok(grant)
    }
}
