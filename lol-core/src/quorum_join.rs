use std::future::Future;
use std::time::Duration;
use tokio::sync::mpsc;

/// execute futures and returns ok only when quorum replied ok within specified duration.
pub async fn quorum_join(
    timeout: Duration,
    quorum: usize,
    futs: Vec<impl Future<Output = bool> + Send + 'static>,
) -> bool {
    let n = futs.len();
    if n < quorum {
        return false;
    }
    assert!(n >= quorum);
    if n == 0 && quorum == 0 {
        return true;
    }
    let main_fut = async {
        let n = futs.len();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut ok: usize = 0;
        let mut ack: usize = 0;
        let mut threads = vec![];
        for fut in futs {
            let txc = tx.clone();
            threads.push(async move {
                let b = fut.await;
                let _ = txc.send(b);
            })
        }
        for th in threads {
            tokio::spawn(th);
        }
        while let Some(x) = rx.recv().await {
            ack += 1;
            if x {
                ok += 1;
            }
            if ok >= quorum {
                return true;
            }
            if ack == n {
                return false;
            }
        }
        unreachable!()
    };
    let res = tokio::time::timeout(timeout, main_fut).await;
    if res.is_err() {
        false
    } else {
        res.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::delay_for;
    async fn f(b: bool, exec_sec: u64) -> bool {
        delay_for(Duration::from_secs(exec_sec)).await;
        b
    }
    #[tokio::test]
    async fn test_quorum_join_ok() {
        let futs = vec![f(true, 0), f(true, 0), f(false, 0)];
        let ok = quorum_join(Duration::from_secs(1), 2, futs).await;
        assert!(ok);
    }
    #[tokio::test]
    async fn test_quorum_join_ng() {
        let futs = vec![f(true, 0), f(false, 0), f(false, 0)];
        let ok = quorum_join(Duration::from_secs(1), 2, futs).await;
        assert!(!ok);
    }
    #[tokio::test]
    async fn test_quorum_join_wait_ok() {
        let futs = vec![f(true, 2), f(true, 2), f(true, 4)];
        let ok = quorum_join(Duration::from_secs(3), 2, futs).await;
        assert!(ok);
    }
    #[tokio::test]
    async fn test_quorum_join_timeout_ng() {
        let futs = vec![f(true, 0), f(true, 2), f(true, 2)];
        let ok = quorum_join(Duration::from_secs(1), 2, futs).await;
        assert!(!ok);
    }
}
