# Targeted Incremental Processing Plan

This document expands phase 4 of [05 — Local Platform Improvement Plan](05-local_platform_improvement_plan.md), following the verified milestone in [06 — Local Lakehouse Execution](06-local_lakehouse_execution.md).

The next milestone is **a small delta that produces exactly the same Gold outputs as a full rebuild while demonstrably processing less data**. Continue using the local dbt/Trino/Iceberg stack, existing v1 business rules, quality policy, and output contracts.

Today, a delta triggers loading of complete committed source histories and a full SQL rebuild. Implement the steps below in order. Keep the full-rebuild path available as an independent execution strategy for comparison and recovery.

## 1. Establish the verified baseline and comparison contract

Record the current implementation commit and the passing reference, SQL fixture, recovery, and restart evidence. Preserve the existing full-rebuild command and its behavior before changing execution strategy.

Define an incremental run specification containing:

- Exact parent publication and its input/checkpoint state.
- Incoming manifest identity and checksum, dataset/rule/quality-policy versions, and transformation revision.
- Previous and new cutoffs, reporting period, and the chosen execution strategy.
- Source and table snapshots used by the run.
- Frozen correctness and processing-efficiency acceptance criteria.

Compare incremental and full-rebuild outputs using identical source records, reference data, rules, reporting period, and cutoff. Retain exact field/key comparisons and the existing precision/null semantics. Physical file names and snapshot IDs may differ; contracted logical results must match.

**Exit condition:** the full-rebuild baseline is reproducible, inputs are pinned, and the comparison specification is version controlled.

## 2. Select an incremental storage and checkpoint strategy

Avoid carrying the current full-history reload into an implementation that only filters records after loading them.

1. Preserve immutable raw deliveries and validate incoming file/API bytes against their manifest.
2. Maintain reusable Bronze history with batch identity, source record identity/version, file/row provenance, and duplicate-delivery handling.
3. Bind previously committed inputs to their recorded manifest checksums and table snapshots. Keep full raw-history integrity audits available as a separate operation, with their cost reported separately.
4. Record committed source membership and the cutoff separately. Delivery progress and event-time eligibility are different checkpoints.
5. Choose a table layout and access path that lets Trino locate complete histories for selected incident IDs without requiring a Python scan of every historical record.
6. Select how incremental Silver and Gold writes preserve unchanged data. Measure any full-table copy, export, or rewrite overhead explicitly.

Before depending on in-place updates, prove that consumers can still read a previously published Gold snapshot **after** a later update/merge and after a service restart through the selected REST catalog. Test the actual consumer path. If historical snapshots are not accessible through that path, select a supported table/reference isolation strategy and record its copy/storage overhead.

Checkpoint advancement must happen only when the validated publication becomes active. If Bronze writes occur earlier, retries must recognize the same delivery, and later processing must exclude records belonging only to failed or unrelated attempts. Keep the existing single-workspace writer lock.

**Exit condition:** an architecture decision records source membership, table layout, replay identity, historical snapshot access, and publication/checkpoint behavior. A small write/retry test proves the strategy before transformation changes begin.

## 3. Compute the complete set of affected incidents

Build an explicit work table with each incident ID and all reasons it requires processing. Candidate discovery must account for time advancing even when source data does not change.

| Trigger | Required treatment |
|---|---|
| New incident snapshots or events | Include the referenced incident, even when its business timestamp is older than the previous cutoff |
| A corrected snapshot or changed assignment/priority | Include the incident and retain its previously published dimensions for downstream impact calculation |
| Previously delivered future-dated records | Include incidents whose records become eligible between the old and new cutoffs |
| Open or reopened incidents | Recalculate advancing SLA clocks and affected backlog days, including incidents receiving no new records |
| An open on-hold interval | Recalculate interval boundaries against the new cutoff under the existing v1 hold rules |
| Quarantined incidents receiving corrections | Re-evaluate the complete history; retain quarantine when unresolved errors remain |
| Reference changes | Apply the current version policy; fail unsupported v1 reference mutations rather than silently ignoring them |
| Identical repeated deliveries | Record the delivery disposition; avoid transformation work when neither eligibility nor time-dependent outputs change |

Deduplicate work-table IDs while preserving multiple reasons. Record candidate counts by reason and distinguish new, changed, time-driven, and unchanged incidents.

A new cutoff alone is sufficient to require work. Support a manifest-driven batch with no new incident/event rows so this behavior can be tested without inventing business events. Define how an empty delivery is represented before extending the generator/loader.

**Exit condition:** tests independently assert candidate IDs for source-driven, late-arriving, future-eligibility, cutoff-only, duplicate-only, and quarantine-correction scenarios.

## 4. Recalculate complete histories for affected incidents

For the selected IDs, retrieve all required historical snapshots and events from the pinned input state. Apply the same v1 rules as a full rebuild:

- Normalize records and select source versions consistently at the new cutoff.
- Preserve creation-time priority and the relevant dimension history.
- Reconstruct first response, holds, resolution, closing, and reopening across the full incident history.
- Apply quarantine and reconciliation rules consistently, including when a previously valid incident becomes invalid or a rejected incident becomes eligible again.
- Produce changed Silver/incident-level Gold rows and retain enough before/after state to calculate daily impacts.

Keep unaffected incident results from the parent publication. Remove obsolete published rows when an affected history is newly quarantined; do not leave its former Gold result visible.

Maintain full-publication quality accounting. A rejection threshold must use the policy's population denominator, not just the selected work set. Reconcile incoming deliveries separately from the complete published population so operator evidence distinguishes delivery work from retained history.

**Exit condition:** affected incident results match the full-rebuild baseline, unaffected results remain unchanged, and quality thresholds/dispositions retain their defined meaning.

## 5. Identify and rebuild affected daily metrics

An incident can contribute to multiple historical dates and dimension groups. Derive daily impacts from both its previous and recalculated histories.

Include:

- Old and new creation dates and their attribution keys when creation is corrected.
- Old and new final-resolution dates and attribution keys after correction, reopening, or re-resolution.
- Old and new service/priority groups affected by historical reassignment or priority changes.
- Every day whose end-of-day backlog changes, including days between a historical reopen and the new cutoff.
- Newly reportable days when the cutoff advances, including contributions from unchanged open incidents.
- Removal of earlier contributions when an incident becomes quarantined.

For the first implementation, recompute complete affected date/service/priority groups from validated incident histories. Include every contributor to a selected group, not only incidents whose records changed. Record the resulting expansion in work explicitly.

Replace or remove affected aggregate rows according to the sparse-row output contract. Handle groups that lose their final contributor, and preserve `null` for an empty SLA denominator. Retain untouched groups without recomputing the complete reporting period.

**Exit condition:** exact daily fixtures pass for reassignment, earlier-date corrections, reopened incidents, advancing backlog, quarantine changes, disappearing groups, and empty denominators.

## 6. Publish incrementally without changing consumer behavior

Use the publication contract already proven by the local milestone.

1. Read one parent publication and pin all required inputs.
2. Execute the selected incident and daily-group work without modifying what a pinned consumer observes.
3. Validate complete output contracts, key uniqueness, reconciliation, quality policy, and required equivalence checks.
4. Prepare a manifest containing both Gold snapshots, the input/checkpoint state, cutoff, execution strategy, and transformation/rule versions.
5. Activate the new publication and its checkpoint state together through the existing publication coordinator.

Retain inspectable attempt evidence before source validation. Inject failures after input persistence, during selective writes, between Gold products, and before activation. Verify that retries neither duplicate deliveries nor advance checkpoints prematurely. Replaying an older committed batch must not move the active publication backward.

**Exit condition:** all failure points preserve the prior committed publication and its input state; retry succeeds; pinned multi-product reads remain consistent before and after restart.

## 7. Prove equivalence and reduced work

First verify correctness on small deterministic histories, then run the existing Smoke profile. Introduce a larger local experiment only after those checks pass and the machine can support it.

| Scenario | Required proof |
|---|---|
| Initial batch followed by a small delta | Both Gold products match full recomputation |
| Cutoff advances with no new records | SLA and backlog changes are applied correctly |
| Late event or previously future-dated event | Eligibility changes are detected without a delivery-time-only watermark |
| Reopen/re-resolution across days | Historical daily contributions are replaced correctly |
| Service, priority, or creation-date correction | Old contributions are removed and new groups are correct |
| Valid-to-quarantined and corrected quarantine | Publication membership and quality accounting remain correct |
| Duplicate-only delivery and replay | Delivery reconciliation is correct; redundant transformations are avoided where possible |
| Failure and restart | Previous snapshots/checkpoints remain usable and retry is safe |

Use a controlled workload with mostly closed history and a small changed population to demonstrate selective processing. Also measure an open-heavy workload; an advancing cutoff can legitimately affect many incidents. Record each workload's distributions, candidate fraction, table layout, and resource configuration before measurement.

Capture candidate-discovery, incident-transformation, daily-aggregation, validation, and publication work separately:

- Input records/bytes physically scanned, including discovery reads.
- Selected incidents, selected daily groups, and unchanged results retained.
- Bytes/files read and written, including copies and unchanged-data rewrites.
- Stage and end-to-end runtime, resource use, retries, and operator actions.
- Exact differences against the full rebuild and publication/checkpoint identities.

Freeze numeric efficiency targets before measured runs. At minimum, the controlled small-delta workload must read fewer historical transformation bytes than the equivalent full rebuild. A smaller candidate list alone does not prove reduced processing. Report validation/rebuild-comparison overhead separately, and include required runtime quality checks in the end-to-end measurement.

**Exit condition:** zero unexplained output differences, successful recovery/replay, and measured reduction in processing work. Report runtime findings even if orchestration or publication overhead limits the end-to-end speedup.

## 8. Record the result and advance the backlog

Add the next numbered execution record with implementation revision, dataset/configuration versions, chosen storage/checkpoint strategy, equivalence results, processing-efficiency evidence, and remaining limits. Update the README and CI with the implemented commands and regression checks. Clearly distinguish targeted execution from the full-rebuild comparison path.

Do not change business semantics during this phase. Phase 5 remains responsible for longer synthetic histories, business-hour calendars, effective-dated SLA policies, and additional assignment rules. Azure, Airflow, Spark comparison, distributed control-plane behavior, and cloud economics remain separate validation work.
