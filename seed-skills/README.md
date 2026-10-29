# Seed skills

The three skills every Plow agent is seeded with. **This directory is the
canonical copy, and the only one anyone edits by hand.**

| skill |
| --- |
| `growth/plow-invite` |
| `productivity/google-workspace` |
| `productivity/owners-mac` |

[`plow-pbc/plow-hermes-agent`](https://github.com/plow-pbc/plow-hermes-agent),
the base image every hosted Plow agent boots, stages all of these out of this
repository's tarball at the `PLOW_CHAT_PLUGIN_SHA` it already pins — the same
commit it fetches `plow-chat-platform/` from. Nothing is copied by hand, and it
tracks no copy of its own: bumping that one pin moves the plugin and the skills
that describe it together, so a skill can never name a tool the plugin at that
commit does not register.

`tests/test_seed_skills.py` holds every `plow_*` tool a skill names to the set
this plugin registers. The skills reach the owner's machines with the `latch`
command and defer its grammar to the latch skill. `plow-pbc/agent-mgr` (deprecated) still pins these paths by SHA in its
`runtime/stack.json`. The former source under `plow-pbc/plow`
`cloud-agents/hermes/` no longer exists.
