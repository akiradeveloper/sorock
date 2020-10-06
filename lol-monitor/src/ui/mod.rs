use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Span, Spans},
    widgets::canvas::{Canvas, Line, Map, MapResolution, Rectangle},
    widgets::{
        Axis, BarChart, Block, Borders, Chart, Dataset, Gauge, List, ListItem, Paragraph, Row,
        Sparkline, Table, Tabs, Wrap,
    },
    Frame,
};
pub struct Member {
    pub id: String,
    pub alive: bool,
    pub snapshot_index: u64,
    pub last_applied: u64,
    pub commit_index: u64,
}
pub struct Model {
    pub members: Vec<Member>,
}
pub fn draw<B>(f: &mut Frame<B>, model: Model)
where
    B: Backend,
{
    let area = f.size();

    let n = model.members.len();
    // let splits = vec![Constraint::Ratio(1,n as u32); n];
    let mut splits = vec![Constraint::Length(7); n];
    splits.push(Constraint::Min(0));
    let chunks = Layout::default()
        .margin(1)
        .constraints(splits.as_ref())
        .split(area);

    let surround = Block::default().borders(Borders::ALL).title("Members");
    f.render_widget(surround, area);

    let mut max_commit_index = 1;
    for i in 0..n {
        let member = &model.members[i];
        max_commit_index = std::cmp::max(max_commit_index, member.commit_index);
    }

    for i in 0..n {
        let member = &model.members[i];
        let name = format!("{}", member.id);
        let style = if member.alive {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::Red)
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(Span::styled(name, style));
        f.render_widget(block, chunks[i]);

        let chunks = Layout::default()
            .margin(1)
            .constraints(
                [
                    Constraint::Length(1),
                    Constraint::Length(2),
                    Constraint::Length(2),
                    Constraint::Min(0),
                ]
                .as_ref(),
            )
            .split(chunks[i]);

        let snapshot_index = Paragraph::new(Span::raw(format!(
            "Snapshot Index: {}",
            member.snapshot_index
        )));
        f.render_widget(snapshot_index, chunks[0]);

        let application_progress = Gauge::default()
            .block(Block::default().title(format!("Application Progress: {}", member.last_applied)))
            .gauge_style(Style::default().fg(Color::Magenta).bg(Color::Black))
            .label("")
            .ratio(member.last_applied as f64 / max_commit_index as f64);
        f.render_widget(application_progress, chunks[1]);

        let replication_progress = Gauge::default()
            .block(Block::default().title(format!("Replication Progress: {}", member.commit_index)))
            .gauge_style(Style::default().fg(Color::Magenta).bg(Color::Black))
            .label("")
            .ratio(member.commit_index as f64 / max_commit_index as f64);
        f.render_widget(replication_progress, chunks[2]);
    }
}
