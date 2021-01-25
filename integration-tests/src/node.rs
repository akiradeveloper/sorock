use std::process::Child;

pub struct Node {
    pub child: Child,
}
impl Node {
    fn pid(&self) -> u32 {
        self.child.id()
    }
    fn int(&mut self) {
        std::process::Command::new("kill")
            .arg("-INT")
            .arg(&format!("{}", self.pid()))
            .spawn()
            .unwrap();
    }
    pub fn pause(&mut self) {
        std::process::Command::new("kill")
            .arg("-STOP")
            .arg(&format!("{}", self.pid()))
            .spawn()
            .expect("failed to pause node");
    }
    pub fn unpause(&mut self) {
        std::process::Command::new("kill")
            .arg("-CONT")
            .arg(&format!("{}", self.pid()))
            .spawn()
            .expect("failed to unpause node");
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.int();
    }
}
