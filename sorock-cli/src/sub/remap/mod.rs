use super::*;

mod calc;

#[derive(Args, Debug)]
pub struct CommandArgs {
    #[clap(long, value_name = "NODE", num_args = 1..)]
    node_list: Vec<Uri>,
}
