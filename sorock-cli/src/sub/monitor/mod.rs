use super::*;

mod mock;
mod model;
mod real;
mod ui;

use ratatui::prelude::*;
use ratatui::widgets::{Block, Gauge};
use ratatui::{
    DefaultTerminal,
    crossterm::event::{self, KeyCode, KeyEventKind},
    widgets::{Axis, Borders, Chart, Dataset, GraphType, StatefulWidget, Widget},
};
use spin::RwLock;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Subcommand, Debug)]
enum Sub {
    #[clap(about = "Monitoring a real shard in a cluster.")]
    Connect { node: Uri, shard_index: u32 },
    #[clap(about = "Test monitoring with test data. (0 -> static, 1 -> mock)")]
    Test { number: u8 },
}

#[derive(Args, Debug)]
pub struct CommandArgs {
    #[clap(subcommand)]
    sub: Sub,
}

pub async fn run(args: CommandArgs) -> Result<()> {
    let model = match args.sub {
        Sub::Connect { node, shard_index } => {
            let node = real::connect_real_node(node, shard_index);
            model::Model::new(node).await
        }
        Sub::Test { number: 0 } => model::Model::test(),
        Sub::Test { number: 1 } => {
            let mock = mock::connect_mock_node();
            model::Model::new(mock).await
        }
        Sub::Test { .. } => unreachable!(),
    };

    let mut terminal = ratatui::init();
    let app_result = App::new(model).run(&mut terminal)?;
    terminal.clear()?;
    ratatui::restore();

    Ok(app_result)
}

struct App {
    model: model::Model,
}
impl App {
    pub fn new(model: model::Model) -> Self {
        Self { model }
    }

    fn run(self, terminal: &mut DefaultTerminal) -> std::io::Result<()> {
        let mut app_state = AppState::default();
        loop {
            terminal.draw(|frame| {
                frame.render_stateful_widget(&self, frame.area(), &mut app_state);
            })?;

            if !event::poll(Duration::from_millis(100))? {
                continue;
            }

            if let event::Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') => return Ok(()),
                        KeyCode::Up | KeyCode::Char('k') => app_state.list_state.previous(),
                        KeyCode::Down | KeyCode::Char('j') => app_state.list_state.next(),
                        _ => {}
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct AppState {
    list_state: tui_widget_list::ListState,
}
impl StatefulWidget for &App {
    type State = AppState;
    fn render(
        self,
        area: ratatui::prelude::Rect,
        buf: &mut ratatui::prelude::Buffer,
        state: &mut Self::State,
    ) where
        Self: Sized,
    {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([Constraint::Length(15), Constraint::Fill(1)].as_ref())
            .split(area);

        let progress_chart = {
            let end = Instant::now();
            let start = end - Duration::from_secs(120);
            let data = self.model.progress_log.read().get_range(start, end);
            ui::progress_chart::ProgressChart::new(data, start, end)
        };
        Widget::render(progress_chart, chunks[0], buf);

        let nodes_list = {
            let mut nodes = vec![];
            let reader = &self.model.nodes.read();

            let min_index = reader
                .nodes
                .values()
                .map(|node_state| node_state.log_state.head_index)
                .min()
                .unwrap_or(0);
            let max_index = reader
                .nodes
                .values()
                .map(|node_state| node_state.log_state.last_index)
                .max()
                .unwrap_or(0);

            for (uri, node_state) in &reader.nodes {
                let log_state = &node_state.log_state;
                nodes.push(ui::node_list::Node {
                    name: uri.to_string(),
                    head_index: log_state.head_index,
                    snapshot_index: log_state.snapshot_index,
                    application_index: log_state.application_index,
                    commit_index: log_state.commit_index,
                    last_index: log_state.last_index,
                    min_max: ui::node_list::IndexRange {
                        min_index,
                        max_index,
                    },
                });
            }
            nodes.sort_by_key(|node| node.name.clone());
            ui::node_list::NodeList::new(nodes)
        };
        StatefulWidget::render(nodes_list, chunks[1], buf, &mut state.list_state);
    }
}
