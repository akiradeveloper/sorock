use tokio::sync::oneshot;

pub struct CommitOk;
pub struct ApplyOk(pub crate::Message);
pub enum Ack {
    OnCommit(oneshot::Sender<CommitOk>),
    OnApply(oneshot::Sender<ApplyOk>),
}
pub fn channel_for_commit() -> (Ack, oneshot::Receiver<CommitOk>) {
    let (tx, rx) = oneshot::channel::<CommitOk>();
    (Ack::OnCommit(tx), rx)
}
pub fn channel_for_apply() -> (Ack, oneshot::Receiver<ApplyOk>) {
    let (tx, rx) = oneshot::channel::<ApplyOk>();
    (Ack::OnApply(tx), rx)
}
