"""The seeded skills may only name tools this plugin registers.

The invite skill shipped for days naming three tools that never existed; the
model read `plow_prepare_invite`, found nothing, and the invite never went
out. Every `plow_*` token in every seed skill is checked against `register`,
except the Latch relay MCP server's own tools, which this plugin does not and
cannot register."""

from __future__ import annotations

import pathlib
import re
from types import SimpleNamespace
from typing import Any

import pytest

from test_adapter import _load

ROOT = pathlib.Path(__file__).resolve().parents[1]
SKILLS = sorted((ROOT / "seed-skills").rglob("SKILL.md"))
# Served by the Latch relay MCP server the agent connects to, not by this plugin.
LATCH_MCP_TOOLS = {"plow_list_skills", "plow_read_skill"}


def _registered_tools(module: Any) -> set[str]:
    names: set[str] = set()
    ctx = SimpleNamespace(
        llm=None,
        deferred_questions=None,
        register_platform=lambda **kw: None,
        register_tool=lambda **kw: names.add(kw["name"]),
        register_hook=lambda *a, **kw: None,
    )
    module.register(ctx)
    return names


@pytest.mark.parametrize("skill", SKILLS, ids=[s.parent.name for s in SKILLS])
def test_a_seed_skill_names_only_tools_the_plugin_registers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, skill: pathlib.Path
) -> None:
    tokens = set(re.findall(r"\bplow_[a-z_]+\b", skill.read_text()))
    registered = _registered_tools(_load(monkeypatch, tmp_path))
    # A skill naming nothing at all means the read or the pattern broke, not
    # that the skill is clean. Checked before the allowlist comes off, because
    # a skill whose whole plow_* surface is Latch's (google-workspace) is
    # legitimately empty afterwards.
    assert tokens, f"{skill} names no plow_* token at all; the read or the pattern is broken"
    named = tokens - LATCH_MCP_TOOLS
    assert named <= registered, f"{skill} names tools the plugin does not register: {sorted(named - registered)}"


def test_the_owners_mac_skill_claims_every_question_about_the_owners_world() -> None:
    """A fresh agent's own stores are empty, and the model reads that as "no
    record" of the owner's world (#128). The model reaches a skill by its
    description, so this one has to name the whole of that world, in
    general terms -- no person, no product line -- and route to the Mac
    rather than the agent's own sessions."""
    text = (ROOT / "seed-skills/productivity/owners-mac/SKILL.md").read_text()
    front = re.match(r"---\n(.*?)\n---\n", text, re.S)
    assert front, "frontmatter missing"
    fields = dict(line.split(": ", 1) for line in front.group(1).splitlines())
    assert fields["name"] == "owners-mac"
    description = fields["description"].lower()
    for must in ("messages", "mail", "calendar", "contacts", "files", "browser",
                 "earlier agent", "not from your own sessions"):
        assert must in description
    body = text[front.end():]
    assert body.index("plow_list_skills") < body.index("plow_read_skill") < body.index("Do what the skill says")
    assert "A request is not work done" in body
    assert "google-workspace" in body, "its rules apply on top of the Mac's where both cover the ask"


def test_the_google_skill_sends_availability_to_a_busy_time_read() -> None:
    """An agent asked "am I free at 2pm" ran the conflict check, got nothing
    back and said the slot was free; the owner had a commitment there on a
    shared calendar. A conflict check reports commitments that overlap each
    other, so a lone event is in neither its result nor the answer."""
    body = (ROOT / "seed-skills/productivity/google-workspace/SKILL.md").read_text()
    assert '"Am I free at 2pm?"' in body
    assert "free/busy read" in body
    assert "not that the\n   owner is free" in body
    # The conflict-override rule keeps its own step; availability is a
    # separate one, and each has to survive an edit to the other.
    assert body.index("--confirm-conflict") < body.index('"Am I free at 2pm?"')
