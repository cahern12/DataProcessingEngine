# Initial POC decision targets

Selected before measured plan-09 comparisons, following the user's instruction to determine targets from the requirements. They are initial engineering/economic decision criteria, not existing production commitments. Exact values are in `acceptance.poc.v1.json` and frozen into each experiment.

Batch-first processing needs useful completion windows: isolated reads have shorter limits; full Small/Medium/Large processing has 3/15/60-minute windows. Recovery has 15/60-minute windows at Medium/Large, with two-minute fault detection and at most one manual action. These limits expose full-restart and operational cost rather than allowing unbounded retries.

The planning scenario is one logical batch per hour, 30 days/month, 90% Small / 9% Medium / 1% Large, with 7-day retained history. Its marginal caps imply a maximum mix-weighted monthly marginal execution cost of $525.60: 720 × (0.90 × $0.50 + 0.09 × $2 + 0.01 × $10). A $750 standing-platform cap leaves a $224.40 contingency under the $1,500 total monthly target. Fully allocated per-run caps are $2/$5/$20. USD is a comparison currency, not an exchange-rate assumption.

Cost/TB is always reported when logical input is positive. The $500 per logical TB acceptance cap applies only at >=10 GB common logical input; tiny fixture/Small inputs should not fail solely because fixed startup is divided by a tiny denominator. Per-run and standing caps still apply. A cutoff-only zero-input delivery has no new-delivery-TB rate.

Low/base/high demand is 6/24/96 runs daily. Volume and worker-count curves need measured runs; no linear performance prediction is inferred from these scenarios. Scale-out from one to four equal-shape workers should provide >=2× speedup with <=2.5× fully allocated workload cost. Four-way concurrency should provide >=1.5× total throughput while keeping completion time <=3× the serial baseline and meeting reliability/quality gates.

The existing runtime is 4 CPUs / 6 GiB. Smoke measurements cannot assess the larger tier, distributed scaling or Azure cost targets. The future cloud campaign planning cap is $300 and 24 environment-hours, split into <=$50 / <=6-hour stages. These are ceilings for a later deployment request; they do not authorize provisioning. A budget alert is not treated as a resource shutoff mechanism.

If calibration or real business demand shows these targets are inappropriate, create a new target version with the rationale before its next decision-grade experiment. Retain prior failures and do not move thresholds retrospectively to make an engine pass.

Before tier calibration, local full-history processing is capped at 64 MiB logical raw input. Python materialization and exact output validation expand raw input in memory while the catalog, storage, Trino, Spark and optional Airflow share 6 GiB. This conservative preflight protects the development environment; it does not establish an engine's measured maximum. Larger tiers retain their original names and remain unassessed when the budget prevents processing.
