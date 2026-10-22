"""Small-data reference evaluator, verified against hand-authored fixtures.

This is deliberately independent of any future dbt SQL implementation.
"""

from collections import Counter, defaultdict
from datetime import timedelta

from .common import ROOT, canonical, iso, read_json, timestamp, validate_schema

RULES = read_json(ROOT / "rules/business_rules.v1.json")
SCHEMAS = {p.name.split(".")[0]: read_json(p) for p in (ROOT / "contracts/schemas").glob("*.json")}


def normalize(dataset, record):
    row = {k: v.strip() if isinstance(v, str) else v for k, v in record.items()}
    if dataset == "incident_snapshots":
        row["priority"] = RULES["priority_mapping"].get(row.get("priority"), row.get("priority"))
        row["status"] = RULES["status_mapping"].get(row.get("status"), row.get("status"))
        for key in ("source_updated_at", "created_at"):
            row[key] = iso(timestamp(row.get(key)))
    elif dataset == "incident_events":
        row["event_time"] = iso(timestamp(row.get("event_time")))
    validate_schema(row, SCHEMAS[dataset])
    return row


def evaluate(datasets, cutoff, report_start):
    cutoff, report_start = timestamp(cutoff), timestamp(report_start)
    if report_start >= cutoff:
        raise ValueError("report_start must precede cutoff")
    dispositions, histories, event_groups, invalid = [], defaultdict(list), defaultdict(list), defaultdict(list)
    references = {}
    for dataset, key in (("users", "user_id"), ("assignment_groups", "group_id"), ("services", "service_id"), ("sla_targets", "priority")):
        refs = {}
        for index, raw in enumerate(datasets.get(dataset, [])):
            row = normalize(dataset, raw)
            value = row[key]
            if value in refs and refs[value] != row:
                raise ValueError("conflicting reference key: " + dataset + "/" + value)
            dispositions.append({"dataset": dataset, "row": index, "record_key": value,
                                 "incident_id": None, "disposition": "duplicate" if value in refs else "accepted",
                                 "rule_id": "BR-002" if value in refs else None, "reason": None})
            refs[value] = row
        references[dataset] = refs
    if set(references["sla_targets"]) != {"P1", "P2", "P3", "P4"} or any(r["rule_version"] != "v1" for r in references["sla_targets"].values()):
        raise ValueError("complete v1 SLA reference targets required")
    # A rule version is immutable; do not silently accept changed targets under v1.
    import csv
    with (ROOT / "rules/sla_targets.v1.csv").open() as source:
        expected_targets = {r["priority"]: (int(r["response_minutes"]), int(r["resolution_minutes"])) for r in csv.DictReader(source)}
    if any((r["response_minutes"], r["resolution_minutes"]) != expected_targets[p] for p, r in references["sla_targets"].items()):
        raise ValueError("SLA targets do not match rule version")
    for dataset in ("incident_snapshots", "incident_events"):
        seen = {}
        for index, raw in enumerate(datasets.get(dataset, [])):
            key = str(raw.get("incident_id", "")).strip()
            disposition = {"dataset": dataset, "row": index, "record_key": raw.get("event_id", key),
                           "incident_id": key, "disposition": "accepted", "rule_id": None, "reason": None}
            dispositions.append(disposition)
            try:
                row = normalize(dataset, raw)
            except (ValueError, TypeError) as error:
                invalid[key].append(("BR-001", str(error)))
                continue
            clock = timestamp(row["source_updated_at" if dataset == "incident_snapshots" else "event_time"])
            if clock > cutoff:
                disposition["disposition"] = "deferred"
                continue
            identity = (key, row["source_version"]) if dataset == "incident_snapshots" else row["event_id"]
            if identity in seen:
                previous = seen[identity]
                if canonical(previous) == canonical(row):
                    disposition.update(disposition="duplicate", rule_id="BR-002")
                else:
                    invalid[key].append(("BR-002", "conflicting record identity"))
                    invalid[previous["incident_id"]].append(("BR-002", "conflicting record identity"))
                continue
            seen[identity] = row
            (histories if dataset == "incident_snapshots" else event_groups)[key].append(row)

    incidents, details = [], {}
    for key in sorted(set(histories) | set(event_groups)):
        snapshots = sorted(histories[key], key=lambda r: (timestamp(r["source_updated_at"]), r["source_version"]))
        events = sorted(event_groups[key], key=lambda r: (timestamp(r["event_time"]), r["source_sequence"]))
        if not snapshots:
            invalid[key].append(("BR-003", "event has no eligible incident snapshot"))
            continue
        for row in snapshots:
            if row["service_id"] not in references["services"] or row["group_id"] not in references["assignment_groups"]:
                invalid[key].append(("BR-003", "missing service/group reference"))
        if invalid[key]:
            continue
        created = timestamp(snapshots[-1]["created_at"])
        if any(timestamp(r["source_updated_at"]) < created for r in snapshots) or created > cutoff:
            invalid[key].append(("BR-004", "snapshot precedes incident creation"))
            continue
        state, response_at, resolved_at, hold_start = "new", None, None, None
        holds, timeline, flags, seen_order = [], [], set(), set()
        for event in events:
            at, kind = timestamp(event["event_time"]), event["event_type"]
            order = (at, event["source_sequence"])
            if at < created or order in seen_order:
                invalid[key].append(("BR-004", "event before creation or ambiguous sequence"))
                break
            seen_order.add(order)
            transition = RULES["event_transitions"][kind]
            if state not in transition:
                invalid[key].append(("BR-005" if kind in ("hold_start", "hold_end") or state == "on_hold" else "BR-004", "invalid transition " + state + " -> " + kind))
                break
            state = transition[state]
            if kind == "response":
                if event["actor_id"] in references["users"]:
                    response_at = response_at or at
                else:
                    flags.add("unknown_actor")
            if kind == "hold_start":
                hold_start = at
            elif kind == "hold_end":
                holds.append((hold_start, at))
                hold_start = None
            elif kind == "resolve":
                resolved_at = at
            elif kind == "reopen":
                resolved_at = None
            timeline.append((at, state))
        if invalid[key]:
            continue
        for snapshot in snapshots:
            snapshot_state = "new"
            for at, historical_state in timeline:
                if at <= timestamp(snapshot["source_updated_at"]):
                    snapshot_state = historical_state
            if snapshot["status"] != snapshot_state:
                invalid[key].append(("BR-004", "snapshot state disagrees with event history"))
        if invalid[key]:
            continue
        if hold_start:
            holds.append((hold_start, cutoff))
        end = resolved_at or cutoff
        elapsed_seconds = (end - created).total_seconds() - sum(max(0, (min(b, end) - a).total_seconds()) for a, b in holds if a < end)
        response_seconds = (response_at - created).total_seconds() if response_at else (cutoff - created).total_seconds()
        initial, latest = snapshots[0], snapshots[-1]
        requester = latest["requester_id"]
        if requester not in references["users"]:
            requester = "unknown_user"
            flags.add("unknown_requester")
        targets = references["sla_targets"][initial["priority"]]

        def sla(seconds, minutes, complete):
            return "breached" if seconds > minutes * 60 else "passed" if complete else "pending"

        row = {"incident_id": key, "created_at": iso(created), "priority_at_creation": initial["priority"],
               "current_priority": latest["priority"], "state": state, "service_id": latest["service_id"],
               "group_id": latest["group_id"], "requester_id": requester, "quality_flags": sorted(flags),
               "first_response_at": iso(response_at) if response_at else None,
               "resolved_at": iso(resolved_at) if resolved_at else None,
               "response_minutes": round(response_seconds / 60, 6) if response_at else None,
               "resolution_minutes": round(elapsed_seconds / 60, 6) if resolved_at else None,
               "response_sla": sla(response_seconds, targets["response_minutes"], response_at is not None),
               "resolution_sla": sla(elapsed_seconds, targets["resolution_minutes"], resolved_at is not None)}
        validate_schema(row, SCHEMAS["incident_resolution"])
        incidents.append(row)
        details[key] = (snapshots, timeline, elapsed_seconds)
    for item in dispositions:
        if invalid.get(item["incident_id"]) and item["disposition"] == "accepted":
            rule, reason = invalid[item["incident_id"]][0]
            item.update(disposition="quarantined", rule_id=rule, reason=reason)
    counts = defaultdict(Counter)
    for item in dispositions:
        counts[item["dataset"]][item["disposition"]] += 1
    metrics = daily_metrics(incidents, details, report_start, cutoff)
    for row in metrics:
        validate_schema(row, SCHEMAS["service_daily_metrics"])
    return {"incident_resolution": incidents, "service_daily_metrics": metrics,
            "dispositions": dispositions, "reconciliation": {k: dict(v) for k, v in counts.items()},
            "quarantined_incidents": {k: [{"rule_id": rule, "reason": reason} for rule, reason in errors] for k, errors in invalid.items() if errors}}


def daily_metrics(incidents, details, start, cutoff):
    buckets = {}

    def bucket(day, snapshot):
        key = (day.date().isoformat(), snapshot["service_id"], snapshot["priority"])
        if key not in buckets:
            buckets[key] = dict(zip(("date", "service_id", "priority"), key), created_count=0,
                                resolved_count=0, backlog_count=0, response_passed=0,
                                response_breached=0, resolution_passed=0, resolution_breached=0,
                                _resolution_seconds=0)
        return buckets[key]

    for incident in incidents:
        snapshots, timeline, seconds = details[incident["incident_id"]]
        created = timestamp(incident["created_at"])
        resolved = timestamp(incident["resolved_at"]) if incident["resolved_at"] else None

        def snapshot_at(at, inclusive=True):
            selected = snapshots[0]
            for row in snapshots:
                when = timestamp(row["source_updated_at"])
                if when < at or inclusive and when == at:
                    selected = row
            return selected

        if start <= created < cutoff:
            bucket(created, snapshots[0])["created_count"] += 1
        if resolved and start <= resolved < cutoff:
            target = bucket(resolved, snapshot_at(resolved))
            target["resolved_count"] += 1
            target["_resolution_seconds"] += seconds
            for prefix in ("response", "resolution"):
                outcome = incident[prefix + "_sla"]
                if outcome != "pending":
                    target[prefix + "_" + outcome] += 1
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day < cutoff:
            end = min(day + timedelta(days=1), cutoff)
            if created < end:
                state = "new"
                for at, next_state in timeline:
                    if at < end:
                        state = next_state
                if state not in ("resolved", "closed"):
                    bucket(day, snapshot_at(end, inclusive=False))["backlog_count"] += 1
            day += timedelta(days=1)
    for row in buckets.values():
        eligible = row["resolution_passed"] + row["resolution_breached"]
        row["resolution_pass_rate"] = round(row["resolution_passed"] / eligible, 6) if eligible else None
        total = row.pop("_resolution_seconds")
        row["avg_resolution_minutes"] = round(total / row["resolved_count"] / 60, 6) if row["resolved_count"] else None
    return [buckets[key] for key in sorted(buckets)]
