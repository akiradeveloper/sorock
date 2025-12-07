use super::*;

use tokio::sync::oneshot;

pub enum Completion {
    Application(ApplicationCompletion),
    Kernel(KernelCompletion),
}

pub struct ApplicationCompletion(oneshot::Sender<Bytes>);
impl ApplicationCompletion {
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

pub fn prepare_application_completion() -> (ApplicationCompletion, oneshot::Receiver<Bytes>) {
    let (tx, rx) = oneshot::channel::<Bytes>();
    (ApplicationCompletion(tx), rx)
}
