// SPDX-License-Identifier: Apache-2.0
//! NUMA abstraction layer: pluggable providers (sysfs, hwloc) and strategy helpers.
//!
//! This module introduces a trait `NumaProvider` allowing the controller to:
//!  * Discover NUMA topology (nodes -> cores)
//!  * Select cores according to a strategy (spread / pack / auto)
//!  * (Future) Bind memory / perform first-touch preparation
//!  * (Future) Lifecycle hooks for adaptive strategies, telemetry, and rebalancing
//!
//! Two concrete providers:
//!  * SysfsNumaProvider: minimal implementation using /sys/devices/system/node
//!  * HwlocNumaProvider: feature-gated, uses hwloc for richer topology data
//!
//! Extension / insertion points (documented for future work):
//!  * `bind_memory` (memory policy / huge pages / interleave / migrate)
//!  * `on_worker_start` / `on_worker_stop` (per-node arenas, perf counters)
//!  * `rebalance_hint` (adaptive strategy / auto mode)
//!  * `prepare_node_local_state` (planned: preallocate per-node structures)
//!  * `select_cores` (can evolve to consider SMT siblings, cache domains, bandwidth)

use core_affinity::CoreId;
use std::{fs, path::Path};
use otap_df_config::pipeline_group::NumaStrategy;

#[derive(Debug, Clone)]
/// A NUMA node with its numeric id and the list of logical core IDs (processing units) that belong to it.
pub struct NumaNode {
    /// Logical NUMA node identifier (as reported by sysfs / hwloc logical_index).
    pub id: usize,
    /// CPU cores (processing units) assigned to this NUMA node, sorted by core id.
    pub cores: Vec<CoreId>,
}

/// Trait for pluggable NUMA backends.
pub trait NumaProvider: Send + Sync {
    /// Name of provider (e.g. "hwloc", "sysfs").
    fn name(&self) -> &'static str;
    /// Discover nodes -> cores mapping. Returns None if discovery unsupported / failed.
    fn discover(&self, all_cores: &[CoreId]) -> Option<Vec<NumaNode>>;

    /// Select cores given discovered nodes, a strategy, requested core count and preferred nodes.
    /// Default implementation handles spread/pack equally; providers may override for richer policies.
    fn select_cores(&self,
        nodes: &[NumaNode],
        strategy: NumaStrategy,
        requested: usize,
        preferred_nodes: &[usize]
    ) -> Vec<(CoreId, Option<usize>)> {
        let mut filtered: Vec<NumaNode> = if preferred_nodes.is_empty() { nodes.to_vec() } else { nodes.iter().filter(|n| preferred_nodes.contains(&n.id)).cloned().collect() };
        if filtered.is_empty() { return Vec::new(); }
        match strategy {
            NumaStrategy::Pack => select_cores_pack(&mut filtered, requested),
            NumaStrategy::Spread | NumaStrategy::Auto => select_cores_spread(&mut filtered, requested),
        }
    }

    /// Hook: bind memory for current thread to a node (noop default).
    fn bind_memory(&self, _node_id: usize) {}
    /// Hook: executed just after thread start & affinity set.
    fn on_worker_start(&self, _node_id: Option<usize>) {}
    /// Hook: executed just before worker termination.
    fn on_worker_stop(&self, _node_id: Option<usize>) {}
    /// Hook: adaptive strategy hint (e.g. switch spread<->pack).
    fn rebalance_hint(&self) -> Option<NumaStrategy> { None }
    /// Hook: prepare per-node shared state (alloc arenas, caches, metrics). Return bool success.
    fn prepare_node_local_state(&self, _node_id: usize) -> bool { true }
}

/// Build the most capable provider available at compile time.
pub fn build_numa_provider() -> Box<dyn NumaProvider> {
    #[cfg(feature = "numa")] {
        return Box::new(HwlocNumaProvider::new());
    }
    #[allow(unreachable_code)]
    Box::new(SysfsNumaProvider::new())
}

// ---------------- Sysfs Provider ----------------

/// Minimal sysfs-based NUMA provider reading /sys/devices/system/node/*/cpulist.
pub struct SysfsNumaProvider;
impl SysfsNumaProvider { /// Construct a new sysfs provider.
    pub fn new() -> Self { Self } }

impl NumaProvider for SysfsNumaProvider {
    fn name(&self) -> &'static str { "sysfs" }

    fn discover(&self, all_cores: &[CoreId]) -> Option<Vec<NumaNode>> {
        let base = Path::new("/sys/devices/system/node");
        if !base.exists() { return None; }
        let mut nodes: Vec<NumaNode> = Vec::new();
        for entry in fs::read_dir(base).ok()? {
            let entry = entry.ok()?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if !name_str.starts_with("node") { continue; }
            if let Ok(id) = name_str[4..].parse::<usize>() {
                let cpulist_path = entry.path().join("cpulist");
                let mut cores = Vec::new();
                if let Ok(text) = fs::read_to_string(&cpulist_path) {
                    for cpu_id in parse_cpulist(&text) {
                        if let Some(cid) = all_cores.iter().find(|c| c.id == cpu_id) { cores.push(*cid); }
                    }
                }
                if !cores.is_empty() { cores.sort_by_key(|c| c.id); nodes.push(NumaNode { id, cores }); }
            }
        }
        if nodes.is_empty() { None } else { Some(nodes) }
    }

    fn bind_memory(&self, _node_id: usize) {
        // Best-effort simple Linux mbind on sysfs-only builds (optional)
        #[cfg(target_os = "linux")]
        {
            // Without hwloc we keep it minimal: rely on first-touch (no-op)
            // Future: could call libc::set_mempolicy/mbind here.
        }
    }
}

// ---------------- HWLOC Provider ----------------

#[cfg(feature = "numa")]
/// A NUMA provider using hwloc for richer topology information.
pub struct HwlocNumaProvider {
    topo: hwlocality::Topology, // keep topology for potential future adaptive hints
}

#[cfg(feature = "numa")]
impl HwlocNumaProvider {
    /// Constructs a new hwloc NUMA provider with the current topology.
    pub fn new() -> Self {
        let topo = hwlocality::Topology::new().expect("hwloc topology init failed");
        Self { topo }
    }
}

#[cfg(feature = "numa")]
impl NumaProvider for HwlocNumaProvider {
    fn name(&self) -> &'static str { "hwloc" }

    fn discover(&self, all_cores: &[CoreId]) -> Option<Vec<NumaNode>> {
        use hwlocality::object::types::ObjectType;
        use std::collections::HashMap;
        let mut cpu_map: HashMap<usize, CoreId> = HashMap::new();
        for c in all_cores.iter().cloned() { let _ = cpu_map.insert(c.id, c); }
        let pus: Vec<_> = self.topo.objects_with_type(ObjectType::PU).collect();
        let mut out = Vec::new();
        for node in self.topo.objects_with_type(ObjectType::NUMANode) {
            let nid = node.logical_index();
            let mut cores = Vec::new();
            for pu in &pus {
                if let Some(pu_set) = pu.cpuset() { if node.covers_cpuset(pu_set) { if let Some(c) = cpu_map.get(&pu.logical_index()).cloned() { cores.push(c); } } }
            }
            if !cores.is_empty() { cores.sort_by_key(|c| c.id); out.push(NumaNode { id: nid, cores }); }
        }
        if out.is_empty() { None } else { Some(out) }
    }

    fn bind_memory(&self, node_id: usize) {
        use hwlocality::memory::binding::{MemoryBindingFlags, MemoryBindingPolicy};
        // Obtain the node's nodeset directly from topology
        if let Some(node_obj) = self.topo.node_with_os_index(node_id) {
            if let Some(ns_ref) = node_obj.nodeset() {
                let flags = MemoryBindingFlags::ASSUME_SINGLE_THREAD;
                let policy = MemoryBindingPolicy::Bind;
                // Best-effort; ignore errors (future: log once)
                let _ = self.topo.bind_memory(ns_ref, policy, flags);
            }
        }
    }
}

// ---------------- Selection Helpers (shared) ----------------

fn select_cores_pack(nodes: &mut [NumaNode], target: usize) -> Vec<(CoreId, Option<usize>)> {
    let mut out = Vec::with_capacity(target);
    nodes.sort_by_key(|n| n.id);
    'outer: for n in nodes.iter_mut() {
        while !n.cores.is_empty() && out.len() < target {
            out.push((n.cores.remove(0), Some(n.id)));
            if out.len() == target { break 'outer; }
        }
    }
    out
}

fn select_cores_spread(nodes: &mut [NumaNode], target: usize) -> Vec<(CoreId, Option<usize>)> {
    nodes.sort_by_key(|n| n.id);
    let mut out = Vec::with_capacity(target);
    let mut exhausted = vec![false; nodes.len()];
    let mut idx = 0usize;
    while out.len() < target && exhausted.iter().any(|&e| !e) {
        if nodes[idx].cores.is_empty() { exhausted[idx] = true; } else {
            out.push((nodes[idx].cores.remove(0), Some(nodes[idx].id)));
            if nodes[idx].cores.is_empty() { exhausted[idx] = true; }
        }
        idx = (idx + 1) % nodes.len();
    }
    out
}

// ---------------- Utilities ----------------

/// Parse Linux cpulist format (e.g. "0-3,8,10-12\n") into iterator of usize.
pub fn parse_cpulist(s: &str) -> impl Iterator<Item = usize> + '_ {
    s.trim().split(',').filter(|t| !t.is_empty()).flat_map(|part| {
        if let Some(dash) = part.find('-') {
            let (a,b) = part.split_at(dash);
            let start: usize = a.parse().unwrap_or(0);
            let end: usize = b[1..].parse().unwrap_or(start);
            (start..=end).collect::<Vec<_>>()
        } else {
            part.parse().ok().into_iter().collect::<Vec<_>>()
        }
    })
}

// Simple smoke test for parser (kept here to avoid separate test file overhead for now)
#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_config::pipeline_group::NumaStrategy;
    use core_affinity::CoreId;

    #[test]
    fn parse_basic() { let v: Vec<_> = parse_cpulist("0-2,4,6-7").collect(); assert_eq!(v, vec![0,1,2,4,6,7]); }

    struct DummyProvider;
    impl NumaProvider for DummyProvider { fn name(&self) -> &'static str { "dummy" } fn discover(&self, _all: &[CoreId]) -> Option<Vec<NumaNode>> { None } }

    fn mk_core(id: usize) -> CoreId { CoreId { id } }

    #[test]
    fn selection_pack() {
        let nodes = vec![NumaNode { id: 0, cores: vec![mk_core(0), mk_core(1)] }, NumaNode { id: 1, cores: vec![mk_core(2), mk_core(3)] }];
        let dummy = DummyProvider;
        let sel = dummy.select_cores(&nodes, NumaStrategy::Pack, 3, &[]);
        // Expect first two from node0 then first from node1
        assert_eq!(sel.iter().map(|(c,_)| c.id).collect::<Vec<_>>(), vec![0,1,2]);
        assert_eq!(sel.iter().map(|(_,n)| n.unwrap()).collect::<Vec<_>>(), vec![0,0,1]);
    }

    #[test]
    fn selection_spread() {
        let nodes = vec![NumaNode { id: 0, cores: vec![mk_core(0), mk_core(2)] }, NumaNode { id: 1, cores: vec![mk_core(1), mk_core(3)] }];
        let dummy = DummyProvider;
        let sel = dummy.select_cores(&nodes, NumaStrategy::Spread, 4, &[]);
        // Expect alternating: 0,1,2,3
        assert_eq!(sel.iter().map(|(c,_)| c.id).collect::<Vec<_>>(), vec![0,1,2,3]);
    }

    #[cfg(feature = "numa")]
    #[test]
    fn hwloc_provider_discover_does_not_panic() {
        let provider = super::build_numa_provider();
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let _ = provider.discover(&cores); // Accept None or Some
        let _ = provider.name();
    }
}
