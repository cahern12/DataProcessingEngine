"""Deterministic, bounded-memory source generation with explicit defect provenance."""

import csv
import io
import random
from datetime import timedelta
from pathlib import Path

from . import __version__
from .common import ROOT, canonical, digest, iso, read_json, timestamp, write_json


class BatchWriter:
    def __init__(self, root, chunk_rows):
        self.root = root
        self.chunk_rows = chunk_rows
        self.files = []
        self.current = {}

    def add(self, dataset, record, fmt="jsonl"):
        current = self.current.get(dataset)
        if current is None or current["rows"] == self.chunk_rows:
            if current:
                current["stream"].close()
            part = sum(f["dataset"] == dataset for f in self.files)
            path = dataset + "/part-%05d." % part + fmt
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            current = {"dataset": dataset, "path": path, "format": fmt, "rows": 0,
                       "min_source_time": None, "max_source_time": None,
                       "stream": target.open("w", newline="", encoding="utf-8")}
            self.current[dataset] = current
            self.files.append(current)
            if fmt == "csv":
                current["writer"] = csv.DictWriter(current["stream"], fieldnames=list(record))
                current["writer"].writeheader()
        if fmt == "csv":
            current["writer"].writerow(record)
        else:
            current["stream"].write(canonical(record) + "\n")
        current["rows"] += 1
        source_time = record.get("source_updated_at", record.get("event_time"))
        if source_time:
            try:
                source_time = iso(timestamp(source_time))
                current["min_source_time"] = min(current["min_source_time"] or source_time, source_time)
                current["max_source_time"] = max(current["max_source_time"] or source_time, source_time)
            except ValueError:
                pass  # Malformed timestamp is an explicitly recorded fault scenario.

    def finish(self):
        for item in self.files:
            item["stream"].close()
            item.pop("stream")
            item.pop("writer", None)
            path = self.root / item["path"]
            item["bytes"] = path.stat().st_size
            item["sha256"] = digest(path)
        return self.files


def load_config(path=None):
    config = read_json(path or ROOT / "synthetic/configs/default.json")
    for field in ("users", "groups", "services", "chunk_rows"):
        if not isinstance(config[field], int) or config[field] < 1:
            raise ValueError(field + " must be a positive integer")
    if config["comment_events"] < 0:
        raise ValueError("comment_events must be nonnegative")
    if config["mode"] not in ("baseline", "faults"):
        raise ValueError("mode must be baseline or faults")
    for value in [config["skew_probability"], config["delta_fraction"], config["new_fraction"],
                  *config["fault_rates"].values()]:
        if not 0 <= value <= 1:
            raise ValueError("rates must be between zero and one")
    if sum(config["fault_rates"].values()) > 1:
        raise ValueError("fault rates must sum to <=1; one injected fault per incident")
    if len(config["priority_weights"]) != 4 or any(w < 0 for w in config["priority_weights"]) or sum(config["priority_weights"]) <= 0:
        raise ValueError("four nonnegative priority weights with positive sum required")
    if set(config["scenario_weights"]) != {"resolved", "open", "hold", "reopened"} or any(w < 0 for w in config["scenario_weights"].values()) or sum(config["scenario_weights"].values()) <= 0:
        raise ValueError("invalid scenario weights")
    start, cutoff, delta = (timestamp(config[k]) for k in ("report_start", "cutoff", "delta_cutoff"))
    if cutoff - start < timedelta(days=7) or delta - cutoff < timedelta(days=1):
        raise ValueError("generation requires >=7 initial days and >=1 delta day")
    if config["rule_version"] != "v1" or config["dataset_version"] != "v1":
        raise ValueError("only v1 rules and data contracts are implemented")
    return config


def incident(config, index, new=False):
    rng = random.Random(config["seed"] * 1000000007 + index)
    key = "INC-%010d" % index
    start = timestamp(config["cutoff"] if new else config["report_start"])
    created = start + timedelta(seconds=rng.randrange(3600, 7200) if new else rng.randrange(86400))
    priority = rng.choices(["P1", "P2", "P3", "P4"], config["priority_weights"])[0]
    scenario = "resolved" if new else rng.choices(list(config["scenario_weights"]), list(config["scenario_weights"].values()))[0]
    snapshot = {"incident_id": key, "source_version": 1, "source_updated_at": iso(created),
                "created_at": iso(created), "priority": priority, "status": "new",
                "service_id": "SVC-%03d" % (0 if rng.random() < config["skew_probability"] else rng.randrange(config["services"])),
                "group_id": "GRP-%03d" % (0 if rng.random() < config["skew_probability"] else rng.randrange(config["groups"])),
                "requester_id": "USR-%04d" % rng.randrange(config["users"])}
    events = []

    def event(kind, seconds):
        seq = len(events) + 1
        events.append({"event_id": key + "-E%04d" % seq, "incident_id": key,
                       "event_time": iso(created + timedelta(seconds=seconds)),
                       "source_sequence": seq, "event_type": kind, "actor_id": "USR-0000"})

    response = rng.randrange(30, 1201)
    event("response", response)
    for number in range(config["comment_events"]):
        event("comment", response + number + 1)
    last = response + config["comment_events"] + 1
    if scenario == "hold":
        event("hold_start", last + 60)
        state = "on_hold"
    elif scenario == "open":
        state = "in_progress"
    else:
        resolution = max(last + 120, rng.randrange(180, 7201) if new else rng.randrange(180, 259201))
        event("resolve", resolution)
        state = "resolved"
        if scenario == "reopened":
            event("reopen", resolution + 60)
            state = "in_progress"
    latest = dict(snapshot, source_version=2, source_updated_at=events[-1]["event_time"], status=state)
    return [snapshot, latest], events


def generate(destination, profile="smoke", count=None, config_path=None, mode=None, seed=None):
    config = load_config(config_path)
    if mode is not None:
        config["mode"] = mode
    if seed is not None:
        config["seed"] = seed
    count = config["profiles"][profile] if count is None else count
    if count < 1:
        raise ValueError("incident count must be positive")
    destination = Path(destination)
    if destination.exists():
        raise ValueError("destination exists; choose a new directory to preserve immutable batches")
    destination.mkdir(parents=True)
    result = []
    for kind in ("initial", "delta"):
        root = destination / kind
        writer = BatchWriter(root, config["chunk_rows"])
        defects = []
        for dataset, id_field, prefix, total, width in (
                ("users", "user_id", "USR", config["users"], 4),
                ("assignment_groups", "group_id", "GRP", config["groups"], 3),
                ("services", "service_id", "SVC", config["services"], 3)):
            for n in range(total):
                writer.add(dataset, {id_field: prefix + "-" + str(n).zfill(width),
                                    "display_name" if dataset == "users" else "name": "Fictional " + prefix + " " + str(n)}, "csv")
        for row in csv.DictReader(io.StringIO((ROOT / "rules/sla_targets.v1.csv").read_text())):
            writer.add("sla_targets", row, "csv")
        if kind == "initial":
            for index in range(count):
                snapshots, events = incident(config, index)
                if config["mode"] == "faults":
                    rng = random.Random(config["seed"] + index + 999999)
                    draw, boundary = rng.random(), 0
                    for fault, rate in config["fault_rates"].items():
                        boundary += rate
                        if draw >= boundary:
                            continue
                        disposition, rule = "quarantined", "BR-001"
                        if fault == "duplicate":
                            snapshots.append(dict(snapshots[0]))
                            disposition, rule = "duplicate", "BR-002"
                        elif fault == "missing_service":
                            for row in snapshots:
                                row["service_id"] = "MISSING"
                            rule = "BR-003"
                        elif fault == "invalid_priority":
                            snapshots[0]["priority"] = "urgent"
                        elif fault == "invalid_timestamp":
                            snapshots[0]["created_at"] = "2026-01-01T00:00:00"
                        elif fault == "conflicting_version":
                            snapshots.append(dict(snapshots[0], priority="P4" if snapshots[0]["priority"] != "P4" else "P1"))
                            rule = "BR-002"
                        else:
                            raise ValueError("unknown fault " + fault)
                        defects.append({"incident_id": snapshots[0]["incident_id"], "fault": fault,
                                        "expected_disposition": disposition, "rule_id": rule})
                        break
                for row in snapshots:
                    writer.add("incident_snapshots", row)
                for row in events:
                    writer.add("incident_events", row)
        else:
            for index in range(max(1, int(count * config["delta_fraction"]))):
                snapshots, events = incident(config, index)
                last = snapshots[-1]
                change_time = timestamp(config["cutoff"]) + timedelta(hours=1)
                seq = len(events) + 1
                if last["status"] == "resolved":
                    change = "reopen"
                    state = "in_progress"
                elif last["status"] == "on_hold":
                    change = "hold_end"
                    state = "in_progress"
                else:
                    change = "comment"
                    state = "in_progress"
                writer.add("incident_snapshots", snapshots[0])  # At-least-once delivery.
                # Delivered late, but effective before the initial cutoff.
                writer.add("incident_snapshots", dict(last, source_version=3,
                                                     source_updated_at=iso(timestamp(last["source_updated_at"]) + timedelta(seconds=1)),
                                                     service_id="SVC-%03d" % (1 % config["services"])))
                writer.add("incident_snapshots", dict(last, source_version=4, status=state,
                                                     source_updated_at=iso(change_time), service_id="SVC-%03d" % (1 % config["services"])))
                writer.add("incident_events", {"event_id": last["incident_id"] + "-D1", "incident_id": last["incident_id"],
                                               "event_time": iso(change_time), "source_sequence": seq, "event_type": change, "actor_id": "USR-0000"})
                writer.add("incident_events", {"event_id": last["incident_id"] + "-LATE", "incident_id": last["incident_id"],
                                               "event_time": iso(timestamp(config["cutoff"]) - timedelta(hours=1)),
                                               "source_sequence": seq + 1, "event_type": "comment", "actor_id": "USR-0000"})
            for index in range(count, count + max(1, int(count * config["new_fraction"]))):
                snapshots, events = incident(config, index, new=True)
                for row in snapshots:
                    writer.add("incident_snapshots", row)
                for row in events:
                    writer.add("incident_events", row)
        manifest = {"batch_id": "batch-" + kind + "-" + digest_config(config, count), "batch_kind": kind,
                    "parent_batch_id": result[0]["batch_id"] if result else None,
                    "cutoff": config["cutoff"] if kind == "initial" else config["delta_cutoff"],
                    "report_start": config["report_start"], "dataset_version": config["dataset_version"],
                    "rule_version": config["rule_version"], "generator_version": __version__, "seed": config["seed"],
                    "configuration": dict(config, incident_count=count, profile=profile),
                    "files": writer.finish()}
        write_json(root / "manifest.json", manifest)
        write_json(root / "expected.v1.json", {"batch_id": manifest["batch_id"],
                                               "manifest_sha256": digest(root / "manifest.json"),
                                               "defects": defects})
        result.append(manifest)
    return [{"batch_id": m["batch_id"], "rows": sum(f["rows"] for f in m["files"]),
             "defects": len(read_json(destination / m["batch_kind"] / "expected.v1.json")["defects"])} for m in result]


def digest_config(config, count):
    import hashlib
    return hashlib.sha256(canonical([config, count, __version__]).encode()).hexdigest()[:16]
