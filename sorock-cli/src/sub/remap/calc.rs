use super::*;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Action {
    AddServer = 0,
    PromoteToVoter = 1,
    NominateLeader = 2,
    DethroneLeader = 3,
    DemoterToLeaner = 4,
    RemoveServer = 5,
    None = 6,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct State {
    pub is_voter: bool,
    pub is_leader: bool,
    pub exists: bool,
}

fn calc_action(from: State, to: State) -> Action {
    if from.exists == false && to.exists == true {
        return Action::AddServer;
    }
    if from.is_voter == false && to.is_voter == true {
        return Action::PromoteToVoter;
    }
    if from.is_leader == false && to.is_leader == true {
        return Action::NominateLeader;
    }
    if from.is_leader == true && to.is_leader == false {
        return Action::DethroneLeader;
    }
    if from.is_voter == true && to.is_voter == false {
        return Action::DemoterToLeaner;
    }
    if from.exists == true && to.exists == false {
        return Action::RemoveServer;
    }

    Action::None
}

pub fn calculate_next_action(mut m: Vec<(Uri, State, State)>) -> (Uri, Action) {
    m.sort_by_key(|(_, from, to)| calc_action(*from, *to));

    let (uri, from, to) = m[0].clone();
    (uri, calc_action(from, to))
}

#[cfg(test)]
mod tests {
    use super::*;

    use proptest::prelude::*;
    use std::collections::HashMap;

    fn do_action(s: State, act: Action) -> State {
        match act {
            Action::AddServer => State {
                exists: true,
                is_voter: s.is_voter,
                is_leader: s.is_leader,
            },
            Action::PromoteToVoter => State {
                exists: s.exists,
                is_voter: true,
                is_leader: s.is_leader,
            },
            Action::NominateLeader => State {
                exists: s.exists,
                is_voter: s.is_voter,
                is_leader: true,
            },
            Action::DethroneLeader => State {
                exists: s.exists,
                is_voter: s.is_voter,
                is_leader: false,
            },
            Action::DemoterToLeaner => State {
                exists: s.exists,
                is_voter: false,
                is_leader: s.is_leader,
            },
            Action::RemoveServer => State {
                exists: false,
                is_voter: s.is_voter,
                is_leader: s.is_leader,
            },
            Action::None => s,
        }
    }

    fn arb_state() -> impl Strategy<Value = State> {
        any::<(bool, bool, bool)>().prop_map(|(is_voter, is_leader, exists)| State {
            is_voter,
            is_leader,
            exists,
        })
    }

    fn arb_uri() -> impl Strategy<Value = Uri> {
        any::<u32>().prop_map(|id| format!("http://{id}").parse::<Uri>().unwrap())
    }

    fn arb_records(
        len: std::ops::RangeInclusive<usize>,
    ) -> impl Strategy<Value = HashMap<Uri, (State, State)>> {
        proptest::collection::hash_map(arb_uri(), (arb_state(), arb_state()), len)
    }

    proptest! {
        #[test]
        fn test_termination(init_state in arb_records(3..=300)) {
            let mut cur_table = init_state;
            loop {
                let (uri, action) = {
                    let mut v = vec![];
                    for (uri, (from, to)) in &cur_table {
                        v.push((uri.clone(), *from, *to));
                    }
                    calculate_next_action(v)
                };

                if action == Action::None {
                    proptest::prop_assert!(cur_table.iter().all(|(_, (from, to))| from == to));
                    break;
                }

                let cur_state = cur_table.get(&uri).unwrap().0;
                let next_state = do_action(cur_state, action);
                cur_table.get_mut(&uri).unwrap().0 = next_state;
            }
        }
    }
}
