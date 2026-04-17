"""Map model confidence + retrieval quality to UI status strings."""

from __future__ import annotations

from typing import Any


def neighbor_has_gl_evidence(n: dict[str, Any]) -> bool:
    t = n.get("training")
    if isinstance(t, dict) and (t.get("account") or t.get("cost_center")):
        return True
    prev = n.get("gl_lines_preview")
    if isinstance(prev, list):
        for gl in prev:
            if isinstance(gl, dict) and (gl.get("account") or gl.get("cost_center")):
                return True
    return False


def derive_status_and_final_confidence(
    model_confidence: float,
    neighbors: list[dict[str, Any]],
) -> tuple[str, float]:
    """Return ``(status, final_confidence)`` for ``statusStyles`` / list views."""
    fc = max(0.0, min(1.0, float(model_confidence)))
    if not neighbors:
        return "Anomaly Flagged", round(min(fc, 0.35), 4)

    has_gl = any(neighbor_has_gl_evidence(n) for n in neighbors)
    sims = [
        float(s)
        for n in neighbors
        if n.get("similarity") is not None
        for s in [n.get("similarity")]
        if s is not None
    ]
    max_sim = max(sims) if sims else 0.0

    if not has_gl:
        fc = min(fc, 0.52)
        return "Needs Review", round(fc, 4)
    if max_sim < 0.25:
        fc = min(fc, 0.58)
        return "Needs Review", round(fc, 4)
    if fc >= 0.85 and max_sim >= 0.35:
        return "Auto-Posted", round(fc, 4)
    if fc >= 0.45:
        return "Needs Review", round(fc, 4)
    return "Anomaly Flagged", round(fc, 4)
