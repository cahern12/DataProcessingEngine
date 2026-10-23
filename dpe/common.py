"""Shared serialization, timestamps, contracts, and integrity checks."""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATASETS = ("users", "assignment_groups", "services", "incident_snapshots",
            "incident_events", "sla_targets")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open('w') as stream:
        json.dump(value,stream,sort_keys=True,indent=2)
        stream.write('\n')


def timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value):
        raise ValueError("timestamp requires ISO-8601 date, time, and explicit UTC offset")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return result.astimezone(timezone.utc)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def validate_schema(value, schema, location="$ "):
    """Validate the JSON Schema subset used by this repo, not arbitrary schemas."""
    types = {"object": dict, "array": list, "string": str, "integer": int,
             "number": (int, float), "boolean": bool, "null": type(None)}
    kind = schema.get("type")
    if kind:
        kinds = kind if isinstance(kind, list) else [kind]
        if not any(isinstance(value, types[k]) and not (
                k in ("integer", "number") and isinstance(value, bool)) for k in kinds):
            raise ValueError(location + ": invalid type")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(location + ": invalid domain")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                raise ValueError(location + ": missing " + key)
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False and set(value) - set(properties):
            raise ValueError(location + ": unknown fields")
        for key in value.keys() & properties.keys():
            validate_schema(value[key], properties[key], location + "." + key)
    if isinstance(value, list):
        for index, item in enumerate(value):
            validate_schema(item, schema.get("items", {}), location + "[" + str(index) + "]")
    if isinstance(value, str):
        if len(value) < schema.get("minLength", 0):
            raise ValueError(location + ": empty string")
        if schema.get("format") == "date-time":
            timestamp(value)
        if schema.get("format") == "date":
            datetime.strptime(value, "%Y-%m-%d")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise ValueError(location + ": below minimum")


def safe_file(root, relative):
    path = (Path(root) / relative).resolve()
    if Path(relative).is_absolute() or not path.is_relative_to(Path(root).resolve()):
        raise ValueError("manifest path escapes batch directory")
    return path
