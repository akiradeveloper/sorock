use super::*;

pub struct ProgressChart {
    data: BTreeMap<Instant, u64>,
    start: Instant,
    end: Instant,
}
impl ProgressChart {
    pub fn new(data: BTreeMap<Instant, u64>, start: Instant, end: Instant) -> Self {
        Self { data, start, end }
    }
    fn to_relative_time(&self, t: Instant) -> f64 {
        let duration = t - self.start;
        duration.as_millis() as f64 / 1_000.
    }
}
impl Widget for ProgressChart {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        let (dataseq, hi_v) = {
            let mut data = vec![];
            for (&t, &v) in &self.data {
                let x = self.to_relative_time(t);
                let y = v as f64;
                data.push((x, y));
            }

            let n = data.len();
            let mut hi_v = 0.;
            let mut out = vec![];
            for j in 1..n {
                let i = j - 1;
                let (ti, xi) = data[i];
                let (tj, xj) = data[j];
                let v = if tj == ti {
                    0.
                } else {
                    (xj - xi) / (tj - ti)
                };
                if v > hi_v {
                    hi_v = v;
                }
                out.push((tj, v));
            }
            (out, hi_v)
        };

        let dataset = Dataset::default()
            // .marker(Marker::Braille)
            .marker(symbols::Marker::HalfBlock)
            .style(Style::default().fg(Color::Yellow))
            .graph_type(GraphType::Bar)
            .data(&dataseq);

        let x_axis = {
            let lo = self.to_relative_time(self.start);
            let hi = self.to_relative_time(self.end);
            Axis::default()
                .style(Style::default().fg(Color::Gray))
                .title("Time")
                .bounds([lo as f64, hi as f64])
                .labels(["-60s", "0s"])
        };
        let y_axis = Axis::default()
            .style(Style::default().fg(Color::Gray))
            .title("Commit/Sec")
            .bounds([0., hi_v])
            .labels(["0".to_string(), format!("{hi_v:.2}")]);
        Chart::new(vec![dataset])
            .block(
                Block::default()
                    .title("Commit Progress")
                    .borders(Borders::ALL),
            )
            .x_axis(x_axis)
            .y_axis(y_axis)
            .render(area, buf);
    }
}
