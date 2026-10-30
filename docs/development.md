# Development experiments

AgentSQL is a local alpha SDK. The next engineering decisions should follow
measured workloads: whether relevant inputs are tracked, what rechecking costs,
and how storage and recovery behave over time. Passing a synthetic example does
not establish adoption or production capacity.

The [on-call race lab](../experiments/on-call-race/README.md) is a runnable
experiment using the existing published SDK. The three directions below are
proposals. They are not implemented integrations or promised release features.

## 1. Measure input capture in a real integration

Connect one MCP client or agent framework to a small document or data-processing
workflow. Give its tools known file and JSON inputs, including missing keys and
directory listings. Change those inputs between preparation and acceptance.

Record which observations appear in the branch's tracked dependencies, which
tools bypass the SDK, and whether stale proposals are rejected. Include a
negative control that deliberately reads through an untracked host or snapshot
interface. Tool-call history should make the attempt inspectable, but must not
be counted as proof that its inputs were tracked.

Useful evidence would include an executable integration, an input inventory,
conflict reports, and a count of tracked and bypassed observations. This would
help decide whether to build an adapter, improve tool guidance, or narrow the
supported workflow. It would not establish universal framework compatibility.

## 2. Measure the cost of conservative conflicts

Build a workload with fields that change at different rates. Compare storing a
whole document or JSON object under one key with splitting independent inputs
across keys. Read the same logical fields and perform the same computation in
each representation, then vary the timing of updates and acceptance.

Measure accepted proposals, required rechecks, recomputation time, and model
cost if a real model is involved. Check an independent application oracle so a
lower conflict count cannot hide missed dependencies. Version changes can
require rechecking even when the final answer would happen to remain the same;
the current SDK does not prove semantic equivalence.

This experiment could show whether better data modeling or finer tracking is
worth its complexity. It does not commit the SDK to field-level tracking,
automatic merging, selective proposal acceptance, or automatic model retries.

## 3. Qualify capacity and recovery for a concrete workload

Start with a real application's file count, file sizes, retained revisions,
pending branches, and tool-call volume. Run it on the intended local filesystem
with cooperating processes and bounded lock waits. Include checkpoints,
reopening, lock contention, interrupted workers, and recovery after a committed
acceptance whose response is lost.

Measure database growth, memory use, acceptance latency, backup duration, and
restore time. Verify accepted contents and branch provenance after recovery.
Repeat on the deployment's OS and storage instead of treating a single local
measurement as a capacity limit or service guarantee.

The existing [workload benchmark](../tools/benchmark.py),
[correctness tests](../tests), and [operations guide](operations.md) are starting
points. Workload evidence should determine whether compaction, quotas, or other
storage work is needed. Process-crash tests do not simulate every hardware or
power-loss failure, and this direction does not add distributed coordination.
