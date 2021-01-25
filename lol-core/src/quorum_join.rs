use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use std::future::Future;

/// Execute futures and returns ok only when quorum replied ok.
pub async fn quorum_join(
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

    let queue = FuturesUnordered::new();
    for fut in futs {
        queue.push(fut);
    }
    futures::pin_mut!(queue);
    let mut ack = 0;
    let mut ok = 0;
    while let Some(x) = queue.next().await {
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
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn f(b: bool) -> bool {
        b
    }
    #[tokio::test]
    async fn test_quorum_join_ok() {
        let futs = vec![f(true), f(false), f(true)];
        let ok = quorum_join(2, futs).await;
        assert!(ok);
    }
    #[tokio::test]
    async fn test_quorum_join_ng() {
        let futs = vec![f(true), f(false), f(false)];
        let ok = quorum_join(2, futs).await;
        assert!(!ok);
    }
}
