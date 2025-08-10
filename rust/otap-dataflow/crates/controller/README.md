# OTAP Dataflow Engine Controller

A controller takes a pipeline configuration and initiates one dataflow engine
per core (or less if the number of CPUs or the percentage of CPUs is
specified).

Each engine is started on a dedicated CPU core (via thread pinning).

## Optional NUMA Support

NUMA (Non-Uniform Memory Access) optimizations are disabled by default to keep
builds lightweight. When enabled (feature `numa`), the controller will:

- Discover the host NUMA topology using a vendored static `hwloc` build
- Select cores according to a strategy (spread across nodes or pack within nodes)
- (Future) Memory binding hooks (currently disabled until API stabilization)
- Allow filtering to a subset of preferred NUMA nodes

### Rationale

Modern multi-socket and multi-chip module servers expose non-uniform memory latency and bandwidth. Accessing memory local to a core's NUMA node can be 1.1×–2× faster and offer materially higher sustained bandwidth than remote node access. A dataflow engine that naively schedules workers can unintentionally amplify remote traffic, causing higher tail latencies, LLC thrash, interconnect contention (UPI/QPI/Infinity Fabric), and wasted power. Making the controller NUMA aware lets us purposely shape how compute threads, memory, and data partitions align to physical locality, unlocking more predictable low latency and higher throughput under load.

### Illustrative 2-Node NUMA Topology (SVG)

<!-- Inline SVG so it renders on GitHub without external assets -->
<p align="center">
<svg width="920" height="470" viewBox="0 0 920 470" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Two-node NUMA topology diagram with latency annotations">
  <style>
    .node { fill:#f5f9ff; stroke:#1d4f91; stroke-width:1.4; }
    .title { font: 600 14px 'Segoe UI', Arial, sans-serif; fill:#0d3060; }
    .sub { font: 500 12px 'Segoe UI', Arial, sans-serif; fill:#0d3060; }
    .core { fill:#ffffff; stroke:#356bb3; stroke-width:1; }
    .core-txt { font: 500 11px 'Segoe UI', Arial, sans-serif; fill:#0d3060; }
    .cache { fill:#e8f2ff; stroke:#5b8cc7; stroke-width:1; }
    .dram { fill:#fff8e6; stroke:#c28a00; stroke-width:1.2; }
    .legend { font: 500 11px 'Segoe UI', Arial, sans-serif; fill:#222; }
    .lat { font: 600 11px 'Segoe UI', Arial, sans-serif; fill:#b30000; }
    .benefit { font: 500 11px 'Segoe UI', Arial, sans-serif; fill:#054c24; }
    .link { stroke:#8a2be2; stroke-width:3; stroke-dasharray:6 4; marker-end:url(#arrow); marker-start:url(#arrow); }
    .remote { stroke:#d9534f; stroke-width:2; stroke-dasharray:4 4; marker-end:url(#arrow); }
    .local { stroke:#2e7d32; stroke-width:2; marker-end:url(#arrow); }
  </style>
  <defs>
    <marker id="arrow" markerWidth="10" markerHeight="10" refX="10" refY="3" orient="auto" markerUnits="strokeWidth">
      <path d="M0,0 L10,3 L0,6 z" fill="#444" />
    </marker>
  </defs>

  <!-- NUMA Node 0 -->
  <g transform="translate(40,40)">
    <rect class="node" x="0" y="0" width="360" height="300" rx="6" />
    <text class="title" x="12" y="22">NUMA Node 0 / Socket 0</text>
    <rect class="cache" x="20" y="40" width="320" height="70" rx="4" />
    <text class="sub" x="32" y="60">Shared LLC (L3)</text>
    <text class="legend" x="32" y="78">(e.g. 64 MB slice aggregate)</text>

    <!-- Cores -->
    <g transform="translate(40,120)">
      <rect class="core" x="0" y="0" width="110" height="60" rx="4" />
      <text class="core-txt" x="10" y="20">Core 0</text>
      <text class="legend" x="10" y="38">L1/L2 private</text>
    </g>
    <g transform="translate(190,120)">
      <rect class="core" x="0" y="0" width="110" height="60" rx="4" />
      <text class="core-txt" x="10" y="20">Core 1</text>
      <text class="legend" x="10" y="38">L1/L2 private</text>
    </g>

    <!-- Local DRAM -->
    <rect class="dram" x="60" y="205" width="240" height="70" rx="6" />
    <text class="sub" x="80" y="230">Local DRAM (channels + ctrl)</text>
    <text class="legend" x="80" y="248">High BW (~100+ GB/s)</text>
  </g>

  <!-- NUMA Node 1 -->
  <g transform="translate(520,40)">
    <rect class="node" x="0" y="0" width="360" height="300" rx="6" />
    <text class="title" x="12" y="22">NUMA Node 1 / Socket 1</text>
    <rect class="cache" x="20" y="40" width="320" height="70" rx="4" />
    <text class="sub" x="32" y="60">Shared LLC (L3)</text>
    <text class="legend" x="32" y="78">(e.g. 64 MB slice aggregate)</text>

    <!-- Cores -->
    <g transform="translate(40,120)">
      <rect class="core" x="0" y="0" width="110" height="60" rx="4" />
      <text class="core-txt" x="10" y="20">Core 2</text>
      <text class="legend" x="10" y="38">L1/L2 private</text>
    </g>
    <g transform="translate(190,120)">
      <rect class="core" x="0" y="0" width="110" height="60" rx="4" />
      <text class="core-txt" x="10" y="20">Core 3</text>
      <text class="legend" x="10" y="38">L1/L2 private</text>
    </g>

    <!-- Local DRAM -->
    <rect class="dram" x="60" y="205" width="240" height="70" rx="6" />
    <text class="sub" x="80" y="230">Local DRAM (channels + ctrl)</text>
    <text class="legend" x="80" y="248">High BW (~100+ GB/s)</text>
  </g>

  <!-- Inter-socket link -->
  <line class="link" x1="400" y1="140" x2="520" y2="140" />
  <text class="lat" x="445" y="126" text-anchor="middle">Inter-socket fabric</text>
  <text class="legend" x="445" y="142" text-anchor="middle">(remote LLC line ~20–30 ns)</text>
  <text class="legend" x="445" y="158" text-anchor="middle">(hop adds +10–20 ns vs local)</text>

  <!-- Remote DRAM arrows (example accesses) -->
  <line class="remote" x1="230" y1="275" x2="520" y2="245" />
  <text class="lat" x="360" y="255" text-anchor="middle">Remote DRAM ~140–180 ns</text>

  <line class="remote" x1="690" y1="275" x2="400" y2="245" />

  <!-- Local DRAM example arrow (Node 0) -->
  <line class="local" x1="150" y1="180" x2="150" y2="205" />
  <text class="lat" x="155" y="195">Local DRAM ~80–90 ns</text>

  <!-- Latency bands / legend -->
  <g transform="translate(40,365)">
    <text class="legend" x="0" y="0">Latency (illustrative):</text>
    <text class="legend" x="0" y="18">L1 hit ~1 ns (≈4 cycles)</text>
    <text class="legend" x="0" y="36">L2 hit ~3–4 ns</text>
    <text class="legend" x="190" y="18">Local LLC hit ~10–15 ns</text>
    <text class="legend" x="190" y="36">Local DRAM ~80–90 ns</text>
    <text class="legend" x="390" y="18">Remote LLC line ~20–30 ns</text>
    <text class="legend" x="390" y="36">Remote DRAM ~140–180 ns</text>
  </g>

  <!-- Benefits callout -->
  <g transform="translate(640,350)">
    <rect x="0" y="-20" width="260" height="120" rx="8" fill="#eefaf3" stroke="#1b6e3d" stroke-width="1.2" />
    <text class="benefit" x="12" y="0">Design Levers (Why NUMA Awareness):</text>
    <text class="benefit" x="12" y="18">• Pin threads -> stable cache locality</text>
    <text class="benefit" x="12" y="36">• Bind memory -> avoid remote page faults</text>
    <text class="benefit" x="12" y="54">• Strategy (pack/spread) -> latency vs BW</text>
    <text class="benefit" x="12" y="72">• Partition data per node -> fewer cross links</text>
  </g>
</svg>
</p>

Approximate Latencies (illustrative; hardware & generation dependent):
  L1 hit:                 ~1 ns  (≈4 cycles)
  L2 hit:                 ~3–4 ns
  L3 (local LLC) hit:     ~10–15 ns
  Local DRAM:             ~80–90 ns
  Remote LLC line:        ~20–30 ns
  Remote DRAM:            ~140–180 ns (≈1.6–2.0× local)
  Cross-socket cache-to-cache line transfer penalty: +10–20 ns vs local

Bandwidth (very approximate order): per-core L1/L2 > local L3 slice aggregate > sum(local DRAM on both nodes) > remote DRAM (effective to a core) > inter-socket link.

Why it matters:
- Pack strategy keeps most accesses within one LLC & DRAM domain (lower average latency, better cache reuse).
- Spread strategy leverages aggregate memory channels & LLC capacity across nodes (higher throughput, more thermal headroom) at the cost of more remote traffic.
- Remote accesses multiply both latency and coherence traffic; binding compute & memory reduces that tax.

### Immediate Benefits (Already Implemented)

- Deterministic core placement: Each worker thread is pinned to a chosen PU (processing unit) — foundation for stable cache & memory locality.
- Strategy-driven spreading/packing: Choose between distributing load across nodes (maximize aggregate memory bandwidth / thermal headroom) or packing (minimize cross-node traffic, concentrate hot working set).
- Node filtering: Restrict execution to a subset of nodes for isolation (e.g., reserving other nodes for different services or experiments).

### Performance Optimization Surface (Current & Planned)

Below is the roadmap of locality-driven optimizations enabled by NUMA awareness:

1. Memory Allocation & Binding (Planned)
   - First-touch or explicit interleave vs. bind-to-node policies.
   - Per-node arenas / slab allocators to reduce remote page faults.
   - Avoiding unintended automatic page migration by keeping allocation & execution co-resident.
2. Data Partition Locality
   - Sharding pipeline state (e.g. hash tables, queues, caches) per NUMA node to eliminate cross-node false sharing.
   - Affinitizing partitions of telemetry batches to the node that will process & export them.
3. Queue / Channel Topology
   - Intra-node lock-free rings vs. inter-node message consolidation.
   - Hierarchical scheduling: Node-local work stealing before global stealing to lower interconnect traffic.
4. Cache & TLB Efficiency
   - Packing strategy keeps working set within fewer LLC domains (better hit rate) for latency-sensitive pipelines.
   - Spreading strategy leverages aggregate MLP & total LLC capacity for throughput modes.
5. Bandwidth & Interconnect Contention Reduction
   - Minimizing remote DRAM accesses decreases utilization of UPI/QPI/Fabric links, unlocking headroom for genuine cross-node transfers (e.g. exporter I/O).
6. Tail Latency Stability
   - Reduced variance from cross-node page misses and cache line bouncing improves p99 / p999 latency for streaming paths.
7. Adaptive Strategy (Planned "auto")
   - Dynamic switch or hybrid: pack while under core budget; spread once node-local bandwidth saturates or memory pressure rises.
   - Feedback loop via lightweight perf counters (remote vs local accesses, LLC miss ratio, queue depth divergence).
8. NUMA-Aware Backpressure & Load Balancing
   - Node-local pressure metrics dictate cross-node task offloading to avoid creating remote hotspots.
9. Export / I/O Affinity
   - Co-locating network export threads with ingestion or separating them to balance memory bandwidth use between ingress parsing and egress serialization.
10. Energy Efficiency / Thermal Distribution
    - Spreading compute across sockets can reduce per-socket frequency throttling under sustained load; packing can enable deeper C-states on idle nodes for power savings in bursty scenarios.
11. Huge Pages & Page Coloring (Future)
    - Per-node huge page pools to reduce TLB pressure; potential alignment with cache slice distribution.
12. Fault Isolation & Graceful Degradation
    - Ability to cordon off a node (e.g. with higher corrected error rate) without reshaping total pipeline logic.

### How Strategies Map to Use Cases

- **spread**: High-throughput ingestion/export when saturation of memory bandwidth and aggregate LLC capacity matter more than single-batch latency.
- **pack**: Latency-sensitive or cache-hot analytic transforms where minimizing remote accesses and maximizing cache reuse dominate.
- **auto (future)**: Start packed until node-local pressure heuristic trips; then incrementally spread.

### Enabling

Single command (vendored hwloc automatically):

```
cargo build -p otap-df-controller --features numa
```

Or from the workspace root for the binary:

```
cargo build --features numa
```

### Runtime CLI Options (when built with `numa`)

- `--numa-strategy <spread|pack|auto>`
- `--preferred-numa-nodes <list>`

Without the feature these flags are parsed but ignored.

### Behavior When Disabled

- Topology queries skipped
- First-N core allocation only
- No memory binding
