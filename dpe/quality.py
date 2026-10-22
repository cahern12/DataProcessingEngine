"""Publication policy: independent of synthetic expected outcomes."""

from .common import ROOT, canonical, read_json
import hashlib


def load_policy(path=None):
    policy = read_json(path or ROOT / "rules/quality_policy.v1.json")
    fraction = policy.get("max_quarantined_incident_fraction")
    count = policy.get("max_quarantined_incidents")
    if policy.get("version") != "v1" or isinstance(fraction, bool) or not isinstance(fraction, (int, float)) or not 0 <= fraction <= 1:
        raise ValueError("invalid quality policy fraction/version")
    if count is not None and (type(count) is not int or count < 0):
        raise ValueError("invalid quality policy count")
    return policy


def enforce_quality(result, datasets, policy):
    identities = {str(row.get("incident_id", "")).strip()
                  for name in ("incident_snapshots", "incident_events") for row in datasets[name]}
    rejected = len(result["quarantined_incidents"])
    fraction = rejected / len(identities) if identities else 0
    evidence = {"policy_version": policy["version"],
                "policy_sha256": hashlib.sha256(canonical(policy).encode()).hexdigest(),
                "incident_count": len(identities), "quarantined_incidents": rejected,
                "quarantined_fraction": fraction, "status": "passed"}
    if fraction > policy["max_quarantined_incident_fraction"] or (
            policy["max_quarantined_incidents"] is not None and rejected > policy["max_quarantined_incidents"]):
        evidence["status"] = "failed"
        raise ValueError("quality policy rejection threshold exceeded: " + canonical(evidence))
    return evidence
