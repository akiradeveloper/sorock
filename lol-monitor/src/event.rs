use std::sync::mpsc;
use std::thread;
use termion::event::Key;
use termion::input::TermRead;

pub enum Event<T> {
    Input(T),
}
pub struct Events {
    rx: mpsc::Receiver<Event<Key>>,
}
impl Events {
    pub fn new() -> Self {
        let (tx0, rx) = mpsc::channel();

        let tx = tx0.clone();
        thread::spawn(move || {
            let stdin = std::io::stdin();
            for evt in stdin.keys() {
                if let Ok(key) = evt {
                    if let Err(_) = tx.send(Event::Input(key)) {
                        return;
                    }
                }
            }
        });

        Self { rx }
    }
    pub fn next(&self) -> Result<Event<Key>, mpsc::TryRecvError> {
        self.rx.try_recv()
    }
}
