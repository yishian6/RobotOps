"""Select bundled diagnostic instructions from verified event metadata."""

import hashlib
from importlib.resources import files


def load_incident_skill(incident: dict) -> tuple[str, dict[str, str]] | None:
    """Load a single reusable playbook for an actual ROS 1 silence observation."""
    fact = incident.get("ros_fact")
    if not fact or fact.get("kind") not in {"topic_silent", "topic_startup_timeout"}:
        return None
    name = "ros1-topic-silence"
    raw = files("robotops.resources").joinpath("skills", name, "SKILL.md").read_bytes()
    _, _, body = raw.decode("utf-8").split("---\n", 2)
    return body.strip(), {"name": name, "sha256": hashlib.sha256(raw).hexdigest()}
