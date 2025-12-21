use super::*;

use itertools::Itertools;

pub struct LazyInsert {
    pub index: u64,
    pub data: Vec<u8>,
    pub space: String,
    pub notifier: oneshot::Sender<()>,
}

pub struct Reaper {
    db: Arc<redb::Database>,
    rx: crossbeam::channel::Receiver<LazyInsert>,
}
impl Reaper {
    pub fn new(db: Arc<redb::Database>) -> (Self, crossbeam::channel::Sender<LazyInsert>) {
        let (tx, rx) = crossbeam::channel::unbounded();
        let this = Self { db, rx };
        (this, tx)
    }

    pub fn reap(&self) -> Result<()> {
        let mut elems = vec![];

        // Blocked until the first element is received.
        let head = self.rx.recv_timeout(Duration::from_millis(100))?;
        elems.push(head);

        let n = self.rx.len();
        for _ in 0..n {
            let e = self.rx.try_recv().unwrap();
            elems.push(e);
        }

        elems.sort_unstable_by_key(|e| (e.space.clone(), e.index));
        let elems = elems.into_iter().into_group_map_by(|e| e.space.clone());

        let mut notifiers = vec![];

        let tx = self.db.begin_write()?;
        for (space, elems_in_space) in elems {
            assert!(elems_in_space.is_sorted_by_key(|e| e.index));

            let mut tbl = tx.open_table(table_def(&space))?;
            // Since the insertions may not be consecutive, we split them into consecutive chunks
            // so the insertions after the earliest partition will be purged and no gap remains.
            for es in split_into_consecutive_chunks(elems_in_space, |e| e.index)
                .into_iter()
                .rev()
            {
                let last_index = es.last().unwrap().index;
                for e in es {
                    tbl.insert(e.index, e.data)?;
                    notifiers.push(e.notifier);
                }
                // BUG:
                // Inserting a snapshot entry will remove all the subsequent entries.
                // tbl.retain_in((last_index + 1).., |_, _| false)?;
            }
        }
        tx.commit()?;

        for notifier in notifiers {
            notifier.send(()).ok();
        }
        Ok(())
    }
}

fn split_into_consecutive_chunks<T, F>(xs: Vec<T>, f: F) -> Vec<Vec<T>>
where
    F: Fn(&T) -> u64,
{
    let mut out = vec![];
    for (_, chunk) in xs
        .into_iter()
        .enumerate()
        .chunk_by(|(i, x)| f(x) - *i as u64)
        .into_iter()
    {
        let group: Vec<T> = chunk.map(|(_, x)| x).collect();
        out.push(group);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_into_consecutives() {
        let xs = vec![1, 2, 3, 5, 6, 8];
        let result = split_into_consecutive_chunks(xs, |&x| x);
        assert_eq!(result, vec![vec![1, 2, 3], vec![5, 6], vec![8]]);
    }
}
