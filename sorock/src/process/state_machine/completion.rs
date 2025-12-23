use super::*;

use tokio::sync::oneshot;

pub enum Completion {
    Application(AppCompletion),
    Kernel(KernelCompletion),
}

pub struct AppCompletion(oneshot::Sender<Bytes>);

impl AppCompletion {
    pub fn complete_with(self, data: Bytes) -> Result<(), Bytes> {
        self.0.send(data)
    }
}

pub struct KernelCompletion(oneshot::Sender<()>);

impl KernelCompletion {
    pub fn complete(self) {
        self.0.send(()).ok();
    }
}

pub fn prepare_kernel_completion() -> (KernelCompletion, oneshot::Receiver<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    (KernelCompletion(tx), rx)
}

pub fn prepare_app_completion() -> (AppCompletion, oneshot::Receiver<Bytes>) {
    let (tx, rx) = oneshot::channel::<Bytes>();
    (AppCompletion(tx), rx)
}
