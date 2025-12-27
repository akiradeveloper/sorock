use super::*;

use regex::Regex;

pub struct ParseResult {
    pub shard_id: u32,
    pub replicas: ShardState,
}

#[derive(Debug, PartialEq)]
struct ReplicaState {
    node_number: usize,
    is_voter: bool,
    is_leader: bool,
}

struct DoParseLine {
    re: Regex,
}

impl DoParseLine {
    pub fn new() -> Self {
        Self {
            re: Regex::new(r"^(\d+)|\[(\d+)\](\*?)$").unwrap(),
        }
    }

    /// Parse n | [n] | [n]*
    fn parse_state(&self, s: &str) -> Option<ReplicaState> {
        let cap = self.re.captures(s)?;
        if let Some(m) = cap.get(1) {
            let node_number = m.as_str().parse::<usize>().ok()?;
            Some(ReplicaState {
                node_number,
                is_voter: false,
                is_leader: false,
            })
        } else if let Some(m) = cap.get(2) {
            let node_number = m.as_str().parse::<usize>().ok()?;
            let is_leader = cap.get(3).is_some() && cap.get(3).unwrap().as_str() == "*";
            Some(ReplicaState {
                node_number,
                is_voter: true,
                is_leader,
            })
        } else {
            None
        }
    }

    fn parse_line(&self, line: &str) -> Option<(u32, Vec<ReplicaState>)> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let shard_id = parts[0].parse::<u32>().ok()?;

        let mut v = vec![];
        for part in &parts[1..] {
            v.push(self.parse_state(part)?);
        }
        Some((shard_id, v))
    }
}

pub struct ParseLine {
    node_list: Vec<Uri>,
    inner: DoParseLine,
}

impl ParseLine {
    pub fn new(node_list: Vec<Uri>) -> Self {
        let inner = DoParseLine::new();
        Self { node_list, inner }
    }

    pub fn parse(&self, line: &str) -> Option<ParseResult> {
        let (shard_id, replica_states) = self.inner.parse_line(line)?;

        let mut h = HashMap::new();
        for rs in replica_states {
            let uri = self
                .node_list
                .get(rs.node_number)
                .cloned()
                .expect("node number out of range");
            h.insert(
                uri,
                calc::State {
                    exists: true,
                    is_voter: rs.is_voter,
                    is_leader: rs.is_leader,
                },
            );
        }

        Some(ParseResult {
            shard_id,
            replicas: ShardState { h },
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_do_parse_state() {
        let parser = super::DoParseLine::new();

        assert_eq!(
            parser.parse_state("1").unwrap(),
            super::ReplicaState {
                node_number: 1,
                is_voter: false,
                is_leader: false,
            }
        );

        assert_eq!(
            parser.parse_state("[2]").unwrap(),
            super::ReplicaState {
                node_number: 2,
                is_voter: true,
                is_leader: false,
            }
        );

        assert_eq!(
            parser.parse_state("[3]*").unwrap(),
            super::ReplicaState {
                node_number: 3,
                is_voter: true,
                is_leader: true,
            }
        );
    }

    #[test]
    fn test_do_parse_line() {
        let line = "42 [0] 1 [3]* [6]";
        let parser = super::DoParseLine::new();
        let (shard_id, replica_states) = parser.parse_line(line).unwrap();
        assert_eq!(shard_id, 42);
        assert_eq!(
            replica_states,
            [
                super::ReplicaState {
                    node_number: 0,
                    is_voter: true,
                    is_leader: false
                },
                super::ReplicaState {
                    node_number: 1,
                    is_voter: false,
                    is_leader: false
                },
                super::ReplicaState {
                    node_number: 3,
                    is_voter: true,
                    is_leader: true
                },
                super::ReplicaState {
                    node_number: 6,
                    is_voter: true,
                    is_leader: false
                },
            ]
        );
    }
}
