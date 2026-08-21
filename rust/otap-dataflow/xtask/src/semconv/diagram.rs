// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic interactive SVG rendering for the semantic-convention entity graph.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};

use super::{ENTITY_SPECS, Registry, collect_association_entities};

pub(super) const DEFAULT_OUTPUT: &str = "semconv/entity-signal-graph.svg";

const CANVAS_WIDTH: i32 = 2_400;
const GRAPH_HEIGHT: i32 = 1_020;
const PAGE_MARGIN: i32 = 58;
const DETAIL_Y: i32 = 1_035;
const DETAIL_CONTENT_TOP: i32 = 160;
const DETAIL_FOOTER: i32 = 55;
const DETAIL_DIVIDER_X: i32 = 1_200;
const SIGNAL_LINE_HEIGHT: i32 = 21;
const METRIC_SET_HEADER_HEIGHT: i32 = 27;
const METRIC_SET_GAP: i32 = 13;
const EVENT_COLUMNS: usize = 2;
const METRIC_COLUMNS: usize = 2;

#[derive(Clone, Debug, Default)]
struct EntitySignals {
    events: BTreeSet<String>,
    metric_sets: BTreeMap<String, BTreeSet<String>>,
}

impl EntitySignals {
    fn metric_count(&self) -> usize {
        self.metric_sets.values().map(BTreeSet::len).sum()
    }
}

#[derive(Debug, Default)]
struct MetricSetSignals {
    entities: Vec<String>,
    metrics: BTreeSet<String>,
}

#[derive(Clone, Copy)]
struct NodePosition {
    entity: &'static str,
    class: &'static str,
    x: i32,
    y: i32,
}

const NODE_WIDTH: i32 = 370;
const NODE_HEIGHT: i32 = 88;

const NODE_POSITIONS: &[NodePosition] = &[
    NodePosition {
        entity: "otap.engine",
        class: "entity-core",
        x: 1_015,
        y: 205,
    },
    NodePosition {
        entity: "otap.controller",
        class: "entity-core",
        x: 620,
        y: 340,
    },
    NodePosition {
        entity: "otap.controller.monitor",
        class: "entity-context",
        x: 1_540,
        y: 340,
    },
    NodePosition {
        entity: "otap.pipeline",
        class: "entity-core",
        x: 620,
        y: 475,
    },
    NodePosition {
        entity: "otap.extension.scope",
        class: "entity-extension",
        x: 100,
        y: 610,
    },
    NodePosition {
        entity: "otap.node",
        class: "entity-node-kind",
        x: 620,
        y: 610,
    },
    NodePosition {
        entity: "otap.flow",
        class: "entity-context",
        x: 1_140,
        y: 610,
    },
    NodePosition {
        entity: "otap.extension",
        class: "entity-extension",
        x: 100,
        y: 745,
    },
    NodePosition {
        entity: "otap.node.custom",
        class: "entity-node-kind",
        x: 490,
        y: 745,
    },
    NodePosition {
        entity: "otap.node.topic",
        class: "entity-node-kind",
        x: 880,
        y: 745,
    },
    NodePosition {
        entity: "otap.node.channel",
        class: "entity-node-kind",
        x: 1_270,
        y: 745,
    },
    NodePosition {
        entity: "otap.extension.channel",
        class: "entity-extension",
        x: 100,
        y: 880,
    },
    NodePosition {
        entity: "otap.node.custom.topic",
        class: "entity-node-kind",
        x: 490,
        y: 880,
    },
];

pub(super) fn render(registry: &Registry, output: &Path) -> Result<()> {
    let svg = render_svg(registry)?;
    if let Some(parent) = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create diagram directory {}", parent.display()))?;
    }
    fs::write(output, svg)
        .with_context(|| format!("failed to write entity signal graph {}", output.display()))?;
    println!("Generated {}", output.display());
    Ok(())
}

fn render_svg(registry: &Registry) -> Result<String> {
    validate_positions(registry)?;
    let signals = collect_entity_signals(registry)?;
    let metric_set_count = registry
        .metrics
        .values()
        .filter_map(|metric| metric.annotations.otap_dataflow.metric_set.as_ref())
        .collect::<BTreeSet<_>>()
        .len();

    let mut svg = String::new();
    writeln!(
        svg,
        r#"<svg id="entity-signal-graph" xmlns="http://www.w3.org/2000/svg" width="{CANVAS_WIDTH}" height="{GRAPH_HEIGHT}" viewBox="0 0 {CANVAS_WIDTH} {GRAPH_HEIGHT}" preserveAspectRatio="xMinYMin meet" role="group" aria-labelledby="diagram-title diagram-description" data-base-height="{GRAPH_HEIGHT}">"#
    )?;
    svg.push_str(
        r#"<title id="diagram-title">OTel Arrow Dataflow observable entity architecture</title>
<desc id="diagram-description">An interactive hierarchy of observable entities. Select an entity to display its associated events and metrics, grouped by metric set.</desc>
<style>
  #entity-signal-graph { width: 100%; height: auto; display: block; }
  :root {
    --bg: #f4f7f6;
    --surface: #ffffff;
    --surface-soft: #eef3f1;
    --ink: #14201d;
    --muted: #53645f;
    --line: #c9d4d0;
    --line-strong: #879b94;
    --core: #16745f;
    --node: #256b9c;
    --extension: #7251a6;
    --context: #a65c20;
    --event: #c65128;
    --metric: #2167b2;
    --selected: #e4f2ed;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0e1513;
      --surface: #151f1c;
      --surface-soft: #1b2824;
      --ink: #eef6f3;
      --muted: #a7bbb4;
      --line: #30413b;
      --line-strong: #71877f;
      --core: #69c9ad;
      --node: #7dbbe5;
      --extension: #b99be6;
      --context: #e4a56e;
      --event: #ef936f;
      --metric: #83b9ee;
      --selected: #203b34;
    }
  }
  text { fill: var(--ink); font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  .mono { font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace; }
  .canvas { fill: var(--bg); }
  .eyebrow { fill: var(--core); font-size: 14px; font-weight: 500; letter-spacing: 2.2px; }
  .title { font-size: 42px; font-weight: 500; letter-spacing: -1px; }
  .subtitle, .summary, .hint, .detail-subtitle, .empty { fill: var(--muted); }
  .subtitle { font-size: 18px; }
  .summary { font-size: 15px; font-weight: 500; }
  .hint { font-size: 13px; }
  .topology-label { fill: var(--muted); font-size: 12px; font-weight: 500; letter-spacing: 1.5px; }
  .entity-edge { fill: none; stroke: var(--line-strong); stroke-width: 2; }
  .entity-node { cursor: pointer; outline: none; }
  .entity-card { fill: var(--surface); stroke-width: 2; rx: 16; transition: fill 120ms ease, stroke-width 120ms ease; }
  .entity-core .entity-card { stroke: var(--core); }
  .entity-node-kind .entity-card { stroke: var(--node); }
  .entity-extension .entity-card { stroke: var(--extension); }
  .entity-context .entity-card { stroke: var(--context); }
  .entity-core .entity-accent { fill: var(--core); }
  .entity-node-kind .entity-accent { fill: var(--node); }
  .entity-extension .entity-accent { fill: var(--extension); }
  .entity-context .entity-accent { fill: var(--context); }
  .entity-node:hover .entity-card, .entity-node:focus .entity-card, .entity-node.is-selected .entity-card { fill: var(--selected); stroke-width: 4; }
  .entity-name { font-size: 18px; font-weight: 500; }
  .entity-counts { fill: var(--muted); font-size: 14px; }
  .detail-panel[hidden] { display: none; }
  .detail-surface { fill: var(--surface); stroke: var(--line); stroke-width: 1.5; rx: 20; }
  .detail-divider { stroke: var(--line); stroke-width: 1; }
  .detail-title { font-size: 30px; font-weight: 500; }
  .detail-subtitle { font-size: 15px; }
  .signal-heading { font-size: 19px; font-weight: 500; }
  .signal-count { fill: var(--muted); font-size: 13px; }
  .event-dot { fill: var(--event); }
  .event-name, .metric-name { font-size: 12px; }
  .metric-set-bar { fill: var(--metric); }
  .metric-set-name { fill: var(--metric); font-size: 13px; font-weight: 500; }
  .metric-branch { stroke: var(--line-strong); stroke-width: 1; }
  .empty { font-size: 13px; font-style: italic; }
  .close-control { cursor: pointer; outline: none; }
  .close-hit { fill: var(--surface-soft); stroke: var(--line); stroke-width: 1.5; }
  .close-control:hover .close-hit, .close-control:focus .close-hit { stroke: var(--core); stroke-width: 3; }
  .close-mark { fill: none; stroke: var(--ink); stroke-width: 2.5; stroke-linecap: round; }
</style>
<defs>
  <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--line-strong)"/>
  </marker>
</defs>
<rect class="canvas" width="100%" height="100%"/>
"#,
    );
    render_header(
        &mut svg,
        registry.entities.len(),
        registry.events.len(),
        metric_set_count,
        registry.metrics.len(),
    )?;
    render_entity_topology(&mut svg, registry, &signals)?;
    for position in NODE_POSITIONS {
        let entity_signals = signals
            .get(position.entity)
            .ok_or_else(|| anyhow::anyhow!("missing signals for entity {}", position.entity))?;
        render_detail_panel(&mut svg, position.entity, entity_signals)?;
    }
    render_interaction_script(&mut svg);
    svg.push_str("</svg>\n");
    Ok(svg)
}

fn validate_positions(registry: &Registry) -> Result<()> {
    let positioned = NODE_POSITIONS
        .iter()
        .map(|position| position.entity)
        .collect::<BTreeSet<_>>();
    for entity in registry.entities.keys() {
        if !positioned.contains(entity.as_str()) {
            bail!("entity {entity} has no diagram position");
        }
    }
    for entity in positioned {
        if !registry.entities.contains_key(entity) {
            bail!("diagram position references missing entity {entity}");
        }
    }
    Ok(())
}

fn collect_entity_signals(registry: &Registry) -> Result<BTreeMap<String, EntitySignals>> {
    let mut result = registry
        .entities
        .keys()
        .map(|entity| (entity.clone(), EntitySignals::default()))
        .collect::<BTreeMap<_, _>>();

    for event in registry.events.values() {
        for entity in association_entities(&event.entity_associations, "event", &event.name)? {
            result
                .get_mut(&entity)
                .ok_or_else(|| {
                    anyhow::anyhow!("event {} references unknown entity {entity}", event.name)
                })?
                .events
                .insert(event.name.clone());
        }
    }

    let mut metric_sets = BTreeMap::<String, MetricSetSignals>::new();
    for metric in registry.metrics.values() {
        let set = metric
            .annotations
            .otap_dataflow
            .metric_set
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!("metric {} has no metric_set annotation", metric.name)
            })?;
        let entities = association_entities(&metric.entity_associations, "metric", &metric.name)?;
        let entry = metric_sets.entry(set.clone()).or_default();
        if entry.entities.is_empty() {
            entry.entities = entities;
        } else if entry.entities != entities {
            bail!("metric set {set} has inconsistent entity associations");
        }
        entry.metrics.insert(metric.name.clone());
    }
    for (set, metric_set) in metric_sets {
        for entity in metric_set.entities {
            result
                .get_mut(&entity)
                .ok_or_else(|| {
                    anyhow::anyhow!("metric set {set} references unknown entity {entity}")
                })?
                .metric_sets
                .insert(set.clone(), metric_set.metrics.clone());
        }
    }
    Ok(result)
}

fn association_entities(
    expressions: &[serde_yaml::Value],
    kind: &str,
    name: &str,
) -> Result<Vec<String>> {
    let mut entities = BTreeSet::new();
    let mut errors = Vec::new();
    for expression in expressions {
        collect_association_entities(expression, &mut entities, &mut errors, kind, name);
    }
    if !errors.is_empty() {
        bail!("{}", errors.join("; "));
    }
    if entities.is_empty() {
        bail!("{kind} {name} has no entity association");
    }
    Ok(entities.into_iter().collect())
}

fn render_header(
    svg: &mut String,
    entities: usize,
    events: usize,
    metric_sets: usize,
    metrics: usize,
) -> Result<()> {
    svg.push_str(
        r#"<text class="eyebrow" x="58" y="50">OTEL ARROW DATAFLOW ENGINE &#xB7; SEMANTIC CONVENTIONS</text>
<text class="title" x="58" y="99">Observable entity architecture</text>
<text class="subtitle" x="58" y="133">Select an entity to inspect its associated events and metric sets.</text>
"#,
    );
    writeln!(
        svg,
        r#"<text class="summary" x="2342" y="52" text-anchor="end">{entities} entities &#xB7; {events} events &#xB7; {metric_sets} metric sets &#xB7; {metrics} metrics</text>
<text class="hint" x="2342" y="83" text-anchor="end">Arrow: parent identity scope &#x2192; child identity scope</text>
<text class="topology-label" x="58" y="181">ENTITY GRAPH</text>"#
    )?;
    Ok(())
}

fn render_entity_topology(
    svg: &mut String,
    registry: &Registry,
    signals: &BTreeMap<String, EntitySignals>,
) -> Result<()> {
    let positions = NODE_POSITIONS
        .iter()
        .map(|position| (position.entity, *position))
        .collect::<BTreeMap<_, _>>();
    for spec in ENTITY_SPECS {
        let Some(parent) = spec.parent else {
            continue;
        };
        let child = positions
            .get(spec.r#type)
            .ok_or_else(|| anyhow::anyhow!("missing diagram position for {}", spec.r#type))?;
        let parent = positions
            .get(parent)
            .ok_or_else(|| anyhow::anyhow!("missing diagram position for parent {parent}"))?;
        let start_x = parent.x + NODE_WIDTH / 2;
        let start_y = parent.y + NODE_HEIGHT;
        let end_x = child.x + NODE_WIDTH / 2;
        let end_y = child.y;
        let mid_y = (start_y + end_y) / 2;
        writeln!(
            svg,
            r#"<path class="entity-edge" marker-end="url(#arrow)" d="M {start_x} {start_y} C {start_x} {mid_y}, {end_x} {mid_y}, {end_x} {end_y}"/>"#
        )?;
    }
    for position in NODE_POSITIONS {
        let entity_signals = signals
            .get(position.entity)
            .ok_or_else(|| anyhow::anyhow!("missing signals for entity {}", position.entity))?;
        if !registry.entities.contains_key(position.entity) {
            bail!("registry is missing entity {}", position.entity);
        }
        render_entity_node(svg, position, entity_signals)?;
    }
    Ok(())
}

fn render_entity_node(
    svg: &mut String,
    position: &NodePosition,
    signals: &EntitySignals,
) -> Result<()> {
    let id = svg_id(position.entity);
    let x = position.x;
    let y = position.y;
    writeln!(
        svg,
        r#"<g id="entity-{id}" class="entity-node {}" data-entity="{}" role="button" tabindex="0" aria-label="Open signals for {}" aria-controls="detail-{id}" aria-expanded="false">
  <title>Open signals for {}</title>
  <rect class="entity-card" x="{x}" y="{y}" width="{NODE_WIDTH}" height="{NODE_HEIGHT}"/>
  <rect class="entity-accent" x="{x}" y="{y}" width="7" height="{NODE_HEIGHT}" rx="3.5"/>
  <text class="entity-name mono" x="{}" y="{}">{}</text>
  <text class="entity-counts" x="{}" y="{}">{} events &#xB7; {} metrics</text>
</g>"#,
        position.class,
        xml_escape(position.entity),
        xml_escape(position.entity),
        xml_escape(position.entity),
        x + 22,
        y + 33,
        xml_escape(position.entity),
        x + 22,
        y + 64,
        signals.events.len(),
        signals.metric_count()
    )?;
    Ok(())
}

fn render_detail_panel(svg: &mut String, entity: &str, signals: &EntitySignals) -> Result<()> {
    let event_rows = signals.events.len().max(1).div_ceil(EVENT_COLUMNS);
    let event_height = i32::try_from(event_rows)? * SIGNAL_LINE_HEIGHT;
    let packed_metric_sets = pack_metric_sets(&signals.metric_sets, METRIC_COLUMNS);
    let metric_height = packed_metric_sets
        .iter()
        .map(|column| metric_column_height(column, &signals.metric_sets))
        .max()
        .unwrap_or(SIGNAL_LINE_HEIGHT);
    let content_height = event_height.max(metric_height).max(100);
    let panel_height = DETAIL_CONTENT_TOP + content_height + DETAIL_FOOTER;
    let total_height = DETAIL_Y + panel_height + 28;
    let panel_width = CANVAS_WIDTH - PAGE_MARGIN * 2;
    let panel_bottom = DETAIL_Y + panel_height;
    let id = svg_id(entity);
    let metric_count = signals.metric_count();

    writeln!(
        svg,
        r#"<g id="detail-{id}" class="detail-panel" data-height="{total_height}" hidden="hidden" aria-hidden="true">
  <rect class="detail-surface" x="{PAGE_MARGIN}" y="{DETAIL_Y}" width="{panel_width}" height="{panel_height}"/>
  <text class="detail-title mono" x="92" y="{}">{}</text>
  <text class="detail-subtitle" x="92" y="{}">{} events &#xB7; {} metric sets &#xB7; {metric_count} metrics &#xB7; shared one-of associations included</text>
  <g class="close-control" data-close="true" role="button" tabindex="0" aria-label="Close details for {}">
    <rect class="close-hit" x="2290" y="{}" width="46" height="46" rx="23"/>
    <path class="close-mark" d="M 2304 {} L 2322 {} M 2322 {} L 2304 {}"/>
  </g>
  <line class="detail-divider" x1="{DETAIL_DIVIDER_X}" y1="{}" x2="{DETAIL_DIVIDER_X}" y2="{}"/>
  <text class="signal-heading" x="92" y="{}">Events</text>
  <text class="signal-count" x="170" y="{}">{} associated names</text>
  <text class="signal-heading" x="1240" y="{}">Metric sets / metrics</text>
  <text class="signal-count" x="1435" y="{}">{} sets &#xB7; {metric_count} instruments</text>"#,
        DETAIL_Y + 50,
        xml_escape(entity),
        DETAIL_Y + 80,
        signals.events.len(),
        signals.metric_sets.len(),
        xml_escape(entity),
        DETAIL_Y + 25,
        DETAIL_Y + 39,
        DETAIL_Y + 57,
        DETAIL_Y + 39,
        DETAIL_Y + 57,
        DETAIL_Y + 106,
        panel_bottom - 34,
        DETAIL_Y + 124,
        DETAIL_Y + 124,
        signals.events.len(),
        DETAIL_Y + 124,
        DETAIL_Y + 124,
        signals.metric_sets.len()
    )?;
    let content_y = DETAIL_Y + DETAIL_CONTENT_TOP;
    render_events(svg, 92, content_y, &signals.events)?;
    render_metric_sets(
        svg,
        1_240,
        content_y,
        &packed_metric_sets,
        &signals.metric_sets,
    )?;
    svg.push_str("</g>\n");
    Ok(())
}

fn render_events(svg: &mut String, x: i32, y: i32, events: &BTreeSet<String>) -> Result<()> {
    if events.is_empty() {
        writeln!(
            svg,
            r#"  <text class="empty" x="{x}" y="{}">No events associated with this entity.</text>"#,
            y + 14
        )?;
        return Ok(());
    }
    let events = events.iter().collect::<Vec<_>>();
    let rows = events.len().div_ceil(EVENT_COLUMNS);
    for column in 0..EVENT_COLUMNS {
        let start = column * rows;
        let end = (start + rows).min(events.len());
        let column_x = x + i32::try_from(column)? * 545;
        for (row, event) in events[start..end].iter().enumerate() {
            let line_y = y + i32::try_from(row)? * SIGNAL_LINE_HEIGHT + 14;
            writeln!(
                svg,
                r#"  <circle class="event-dot" cx="{}" cy="{}" r="3.5"/><text class="event-name mono" x="{}" y="{line_y}">{}</text>"#,
                column_x + 4,
                line_y - 4,
                column_x + 15,
                xml_escape(event)
            )?;
        }
    }
    Ok(())
}

fn pack_metric_sets(
    metric_sets: &BTreeMap<String, BTreeSet<String>>,
    columns: usize,
) -> Vec<Vec<String>> {
    let mut result = vec![Vec::new(); columns];
    let mut heights = vec![0_i32; columns];
    for (set, metrics) in metric_sets {
        let column = heights
            .iter()
            .enumerate()
            .min_by_key(|(index, height)| (**height, *index))
            .map(|(index, _)| index)
            .unwrap_or_default();
        result[column].push(set.clone());
        heights[column] += metric_set_height(metrics.len());
    }
    result
}

fn metric_column_height(sets: &[String], metric_sets: &BTreeMap<String, BTreeSet<String>>) -> i32 {
    sets.iter()
        .filter_map(|set| metric_sets.get(set))
        .map(|metrics| metric_set_height(metrics.len()))
        .sum::<i32>()
        .max(SIGNAL_LINE_HEIGHT)
}

fn metric_set_height(metric_count: usize) -> i32 {
    METRIC_SET_HEADER_HEIGHT
        + i32::try_from(metric_count).unwrap_or(i32::MAX / SIGNAL_LINE_HEIGHT) * SIGNAL_LINE_HEIGHT
        + METRIC_SET_GAP
}

fn render_metric_sets(
    svg: &mut String,
    x: i32,
    y: i32,
    columns: &[Vec<String>],
    metric_sets: &BTreeMap<String, BTreeSet<String>>,
) -> Result<()> {
    if metric_sets.is_empty() {
        writeln!(
            svg,
            r#"  <text class="empty" x="{x}" y="{}">No metrics associated with this entity.</text>"#,
            y + 14
        )?;
        return Ok(());
    }
    for (column, sets) in columns.iter().enumerate() {
        let column_x = x + i32::try_from(column)? * 550;
        let mut block_y = y;
        for set in sets {
            let metrics = metric_sets
                .get(set)
                .ok_or_else(|| anyhow::anyhow!("packed metric set {set} is missing"))?;
            writeln!(
                svg,
                r#"  <rect class="metric-set-bar" x="{column_x}" y="{block_y}" width="5" height="18" rx="2.5"/>
  <text class="metric-set-name mono" x="{}" y="{}">{}</text>"#,
                column_x + 14,
                block_y + 14,
                xml_escape(set)
            )?;
            let branch_x = column_x + 18;
            if !metrics.is_empty() {
                let branch_bottom = block_y
                    + METRIC_SET_HEADER_HEIGHT
                    + i32::try_from(metrics.len().saturating_sub(1))? * SIGNAL_LINE_HEIGHT
                    + 10;
                writeln!(
                    svg,
                    r#"  <line class="metric-branch" x1="{branch_x}" y1="{}" x2="{branch_x}" y2="{branch_bottom}"/>"#,
                    block_y + 22
                )?;
            }
            for (index, metric) in metrics.iter().enumerate() {
                let metric_y = block_y
                    + METRIC_SET_HEADER_HEIGHT
                    + i32::try_from(index)? * SIGNAL_LINE_HEIGHT
                    + 12;
                let short_name = metric
                    .strip_prefix(set)
                    .and_then(|suffix| suffix.strip_prefix('.'))
                    .unwrap_or(metric);
                writeln!(
                    svg,
                    r#"  <g><title>{}</title><line class="metric-branch" x1="{branch_x}" y1="{}" x2="{}" y2="{}"/><text class="metric-name mono" x="{}" y="{metric_y}">{}</text></g>"#,
                    xml_escape(metric),
                    metric_y - 5,
                    branch_x + 10,
                    metric_y - 5,
                    branch_x + 16,
                    xml_escape(short_name)
                )?;
            }
            block_y += metric_set_height(metrics.len());
        }
    }
    Ok(())
}

fn render_interaction_script(svg: &mut String) {
    svg.push_str(
        r#"<script type="application/ecmascript"><![CDATA[
(function () {
  const root = document.getElementById("entity-signal-graph");
  const nodes = Array.from(root.querySelectorAll(".entity-node"));
  const panels = Array.from(root.querySelectorAll(".detail-panel"));
  const closeControls = Array.from(root.querySelectorAll("[data-close]"));
  const baseHeight = root.getAttribute("data-base-height");
  let selected = null;

  function hideSelection() {
    panels.forEach((panel) => {
      panel.setAttribute("hidden", "hidden");
      panel.setAttribute("aria-hidden", "true");
    });
    nodes.forEach((node) => {
      node.classList.remove("is-selected");
      node.setAttribute("aria-expanded", "false");
    });
    selected = null;
  }

  function resize(height) {
    root.setAttribute("viewBox", "0 0 2400 " + height);
    root.setAttribute("height", height);
  }

  function closeDetails(returnFocus) {
    const previous = selected;
    hideSelection();
    resize(baseHeight);
    if (returnFocus && previous) {
      previous.focus();
      previous.scrollIntoView({ block: "center" });
    }
  }

  function openDetails(node) {
    if (selected === node) {
      closeDetails(false);
      return;
    }
    hideSelection();
    const panel = document.getElementById(node.getAttribute("aria-controls"));
    if (!panel) {
      return;
    }
    selected = node;
    node.classList.add("is-selected");
    node.setAttribute("aria-expanded", "true");
    panel.removeAttribute("hidden");
    panel.setAttribute("aria-hidden", "false");
    resize(panel.getAttribute("data-height"));
    window.requestAnimationFrame(() => panel.scrollIntoView({ behavior: "smooth", block: "start" }));
  }

  nodes.forEach((node) => {
    node.addEventListener("click", () => openDetails(node));
    node.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        openDetails(node);
      }
    });
  });

  closeControls.forEach((control) => {
    control.addEventListener("click", () => closeDetails(true));
    control.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        closeDetails(true);
      }
    });
  });

  root.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && selected) {
      closeDetails(true);
    }
  });
})();
]]></script>
"#,
    );
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn svg_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unsupported_named_entities(value: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut rest = value;
        while let Some(start) = rest.find('&') {
            rest = &rest[start + 1..];
            let Some(end) = rest.find(';') else {
                break;
            };
            let entity = &rest[..end];
            if entity
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
                && !matches!(entity, "amp" | "lt" | "gt" | "quot" | "apos")
            {
                result.push(entity.to_owned());
            }
            rest = &rest[end + 1..];
        }
        result
    }

    /// Scenario: the complete checked-in registry is rendered as an interactive entity SVG.
    /// Guarantees: every signal is embedded, one detail panel exists per entity, XML entities are valid, and the checked-in output is deterministic.
    #[test]
    fn checked_in_entity_signal_graph_matches_the_registry() {
        let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("xtask manifest must live in the repository");
        let registry = Registry::load(&repo_root.join("semconv")).unwrap();
        let svg = render_svg(&registry).unwrap();
        assert_eq!(svg, render_svg(&registry).unwrap());
        assert_eq!(
            svg.matches("class=\"entity-node ").count(),
            registry.entities.len()
        );
        assert_eq!(
            svg.matches("class=\"detail-panel\"").count(),
            registry.entities.len()
        );
        assert!(svg.contains("function openDetails(node)"));
        assert!(unsupported_named_entities(&svg).is_empty());
        for event in registry.events.keys() {
            assert!(svg.contains(&xml_escape(event)), "missing event {event}");
        }
        for metric in registry.metrics.keys() {
            assert!(svg.contains(&xml_escape(metric)), "missing metric {metric}");
        }
        let checked_in = fs::read_to_string(repo_root.join(DEFAULT_OUTPUT)).unwrap();
        assert!(
            checked_in == svg,
            "{DEFAULT_OUTPUT} is stale; run `cargo xtask semconv-graph`"
        );
    }
}
