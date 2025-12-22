use super::*;

pub struct Effect<'a> {
    pub command_log: &'a mut CommandLog,
}

impl Effect<'_> {
    pub async fn exec(self) -> Result<command_exec::AppCommand> {
        let cur_app_index = self.command_log.app_pointer;
        ensure!(cur_app_index < self.command_log.kernel_pointer);

        let process_index = cur_app_index + 1;
        let e = self.command_log.get_entry(process_index).await?;
        self.command_log.app_pointer = process_index;

        let command = Command::deserialize(&e.command);
        let will_process = match command {
            Command::ExecuteRequest { .. } => true,
            Command::CompleteRequest { .. } => true,
            Command::Snapshot { .. } => true,
            _ => false,
        };

        if will_process {
            Ok(command_exec::AppCommand {
                index: process_index,
                body: Some(command_exec::AppCommandBody {
                    command: e.command,
                    completion: self.command_log.app_completions.remove(&process_index),
                }),
            })
        } else {
            Ok(command_exec::AppCommand {
                index: process_index,
                body: None,
            })
        }
    }
}
