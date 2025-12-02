use super::*;

#[derive(Clone, Copy)]
pub struct IndexRange {
    pub min_index: u64,
    pub max_index: u64,
}

struct LogStripe {
    min_to_head: u16,
    head_to_snap: u16,
    snap_to_app: u16,
    app_to_commit: u16,
    commit_to_last: u16,
    last_to_max: u16,
}
impl LogStripe {
    pub fn from(node: &Node) -> Self {
        let width = node.min_max.max_index - node.min_max.min_index;

        let min_to_head = (node.head_index - node.min_max.min_index) as f64 / width as f64;
        let min_to_snap = (node.snapshot_index - node.min_max.min_index) as f64 / width as f64;
        let min_to_app = (node.app_index - node.min_max.min_index) as f64 / width as f64;
        let min_to_commit = (node.commit_index - node.min_max.min_index) as f64 / width as f64;
        let min_to_last = (node.last_index - node.min_max.min_index) as f64 / width as f64;

        let k = 10000.0;

        Self {
            min_to_head: (min_to_head * k) as u16,
            head_to_snap: ((min_to_snap - min_to_head) * k) as u16,
            snap_to_app: ((min_to_app - min_to_snap) * k) as u16,
            app_to_commit: ((min_to_commit - min_to_app) * k) as u16,
            commit_to_last: ((min_to_last - min_to_commit) * k) as u16,
            last_to_max: ((1.0 - min_to_last) * k) as u16,
        }
    }
}

#[derive(Clone)]
pub struct Node {
    pub name: String,

    pub head_index: u64,
    pub snapshot_index: u64,
    pub app_index: u64,
    pub commit_index: u64,
    pub last_index: u64,

    pub min_max: IndexRange,
}
impl Widget for Node {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        let stripe = LogStripe::from(&self);

        let outer_block = Block::default().borders(Borders::ALL).title(self.name);

        let inner_area = outer_block.inner(area);

        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(
                [
                    Constraint::Fill(stripe.min_to_head),
                    Constraint::Fill(stripe.head_to_snap),
                    Constraint::Fill(stripe.snap_to_app),
                    Constraint::Fill(stripe.app_to_commit),
                    Constraint::Fill(stripe.commit_to_last),
                    Constraint::Fill(stripe.last_to_max),
                ]
                .as_ref(),
            )
            .split(inner_area);

        Gauge::default()
            .gauge_style(
                Style::default()
                    .fg(Color::Gray)
                    .bg(Color::Black)
                    .add_modifier(Modifier::ITALIC),
            )
            .label("gc")
            .ratio(1.0)
            .render(chunks[1], buf);

        Gauge::default()
            .gauge_style(
                Style::default()
                    .fg(Color::Green)
                    .bg(Color::Black)
                    .add_modifier(Modifier::ITALIC),
            )
            .label("applied")
            .ratio(1.0)
            .render(chunks[2], buf);

        Gauge::default()
            .gauge_style(
                Style::default()
                    .fg(Color::Yellow)
                    .bg(Color::Black)
                    .add_modifier(Modifier::ITALIC),
            )
            .label("commit")
            .ratio(1.0)
            .render(chunks[3], buf);

        Gauge::default()
            .gauge_style(
                Style::default()
                    .fg(Color::Red)
                    .bg(Color::Black)
                    .add_modifier(Modifier::ITALIC),
            )
            .label("uncommit")
            .ratio(1.0)
            .render(chunks[4], buf);

        outer_block.render(area, buf);
    }
}

pub struct NodeList {
    nodes: Vec<Node>,
}
impl NodeList {
    pub fn new(nodes: Vec<Node>) -> Self {
        Self { nodes }
    }
}
impl StatefulWidget for NodeList {
    type State = tui_widget_list::ListState;
    fn render(
        self,
        area: ratatui::prelude::Rect,
        buf: &mut ratatui::prelude::Buffer,
        state: &mut Self::State,
    ) where
        Self: Sized,
    {
        let n = self.nodes.len();
        let builder = tui_widget_list::ListBuilder::new(move |ctx| {
            let selected = ctx.is_selected;
            let idx = ctx.index;
            let mut node = self.nodes[idx].clone();
            let name  = format!("{} [{}/{}/{}/{}/{}]", node.name, node.head_index, node.snapshot_index, node.app_index, node.commit_index, node.last_index);
            node.name = if selected {
                format!("> {}", name)
            } else {
                name
            };
            (node, 3)
        });
        let view = tui_widget_list::ListView::new(builder, n);

        StatefulWidget::render(view, area, buf, state);
    }
}
