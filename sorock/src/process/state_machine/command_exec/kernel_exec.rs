use super::*;

pub struct KernelCommand {
    pub index: LogIndex,
    pub command: Bytes,
    pub kernel_completion: Option<completion::KernelCompletion>,
}

pub struct KernelExec {
    q: CommandWaitQueue<KernelCommand>,
    ctrl_actor: Actor<Control>,
}

impl KernelExec {
    pub fn new(ctrl_actor: Actor<Control>) -> Self {
        Self {
            q: CommandWaitQueue::new(),
            ctrl_actor,
        }
    }

    pub fn insert(&mut self, kernel_command: KernelCommand) {
        self.q.insert(kernel_command.index, kernel_command);
    }

    pub async fn process_once(&mut self) -> bool {
        let Some((_, kernel_command)) = self.q.pop_next() else {
            return false;
        };

        match self.do_process_once(kernel_command).await {
            Ok(()) => true,
            Err(_) => false,
        }
    }

    async fn do_process_once(&mut self, kernel_command: KernelCommand) -> Result<()> {
        let KernelCommand {
            index,
            command,
            kernel_completion,
        } = kernel_command;

        let command = Command::deserialize(&command);

        debug!("process kernel@{index}");

        match command {
            Command::TermBarrier(term) => {
                self.ctrl_actor.write().await.commit_safe_term(term);
            }
            Command::ClusterConfiguration { .. } => {}
            _ => {}
        }

        if let Some(kernel_completion) = kernel_completion {
            kernel_completion.complete();
        }

        Ok(())
    }
}
