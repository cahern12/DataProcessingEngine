# Deferred Azure validation boundary

`validation-spec.template.json` is a preparation checklist with the adopted POC decision targets and campaign ceilings. It is explicitly `not_authorized_not_deployed`. The user's current scope is local execution.

No Azure infrastructure, subscription selection, rate retrieval, cost export job, remote publication coordinator or automatic shutdown has been deployed. Reproducible Azure IaC remains work for the authorized deployment stage; this template is not IaC or evidence of Azure compatibility.

Before that stage, select the subscription/region/SKUs, preserve applicable retail/effective-rate snapshots, define the supported remote single-writer/conditional-publication mechanism, and convert the roles into budgeted infrastructure definitions. Then execute the original S/M/L, worker/concurrency and distributed recovery matrix and reconcile settled provider charges to the workload ledger. The local filesystem lock and S3-compatible test object store do not prove those Azure behaviors.

See [plan 09](../../docs/plan/09-poc_hypothesis_benchmark_and_economics_validation_plan.md), [the execution record](../../docs/plan/10-poc_benchmark_and_economics_execution.md), and [the accounting guide](../../docs/guides/poc_benchmarks_and_economics.md).
