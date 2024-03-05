use super::*;

use tokio::sync::oneshot;

pub enum Completion {
    User(UserCompletion),
    Kern(KernCompletion),
}

pub struct UserCompletion(oneshot::Sender<Bytes>);
impl UserCompletion {
    pub fn complete_with(self, data: Bytes) -> Result<(), Bytes> {
        self.0.send(data)
    }
}

pub struct KernCompletion(oneshot::Sender<()>);
impl KernCompletion {
    pub fn complete(self) {
        self.0.send(()).ok();
    }
}

pub fn prepare_kern_completion() -> (KernCompletion, oneshot::Receiver<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    (KernCompletion(tx), rx)
}

pub fn prepare_user_completion() -> (UserCompletion, oneshot::Receiver<Bytes>) {
    let (tx, rx) = oneshot::channel::<Bytes>();
    (UserCompletion(tx), rx)
}
