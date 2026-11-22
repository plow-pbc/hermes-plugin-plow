# Review instructions — hermes-plugin-plow

## Cause-first bug fixes

Fix at the cause; never with a workaround that leaves the cause in place. A guard or check at the boundary where the cause lives is a cause-level fix. If a fix adds a maintained path or net complexity, explain why removing the cause directly is insufficient. Label any authorized temporary containment and state when it can be removed. When the bug comes from a duplicated source or identity, remove the duplicate rather than bridging the two. Limits a REVIEW.md already records as accepted stay out of review unless the diff widens them.

Repo-specific reviewer policy. The universal voice posture (Broken-Glass,
pro-simplification, and the don't-propose list) is supplied by the reviewers
themselves and is deliberately not restated here.

## Operating point

Pre-PMF, fewer than 10 users, often a single operator. Iteration speed beats
hardening for scale: prefer loud failures to fallbacks, pragmatic DRY
architecture to defensive layering, and don't guard edge cases that can't
trigger at this scale.

## What this repo is

**One Hermes platform plugin, and it is production.** `plow-chat-platform/` is
installed verbatim into every agent in the fleet by `plow-pbc/agent-mgr`, which
pins this repo by SHA. There is no reference/product distinction here any more —
this repo used to be a SEED, where `ref/` code was explicitly a single-operator
reference realization of a prose spec. That framing is retired along with the
`ref/` layout. The adapter is the shipped artifact.

`just test` is the gate. `README.md` is the only prose contract.


## Check upstream before inventing

This repo extends Hermes, and the single most expensive defect class here is
re-solving something the framework already ships. It is easy to commit and hard
to see: `just test` stubs the `gateway.*` modules, so this repo builds, tests and
ships without upstream ever being on disk. The author — human or agent — is
usually not ignoring prior art; they cannot see it.

Two sibling entries exist so a reviewer can. Both are subtrees of the upstream
mirror, materialized at `.siblings/<slug>/` like any other sibling:

- `NousResearch/hermes-gateway-platforms` — `BasePlatformAdapter` and
  `ADDING_A_PLATFORM.md`, which enumerates what the plugin system already
  handles for an adapter.
- `NousResearch/hermes-platform-adapters` — the framework's own ~22 adapters.
  Read one that faces the same problem before accepting a new mechanism here;
  `bluebubbles` is the closest peer (`PROVIDER = "imessage"`).

**The grep will not find this for you.** Sibling prior-art search fires on names
the diff introduces, and a reinvention coins a *new* name for a mechanism
upstream already has under a different one — so it comes back empty exactly when
the defect is present. `_GROUP_SPEAK_RULE` (#140) returns zero hits everywhere;
upstream's `require_mention` appears in 73 files and `channel_context` in 12.
Ask what the construct *does*, then look for that behaviour upstream by concept.

| DON'T | DO |
|---|---|
| Accept `Prior-art: none` for a new adapter mechanism when the upstream slugs report `missing` — that is a coverage gap, not a clean run. | Flag any new **adapter-plumbing** mechanism (dedup, retry/backoff, reconnect, typing lifecycle, media caching, error classification, mention gating) that upstream already ships, citing `.siblings/<slug>/<path>:<line>`. |
| Demand a repo change for an upstream gap. When upstream's own mechanism is the wrong shape, say so and leave the local one — with the reason recorded. | Flag a config default this repo silently inherits and never sets, where upstream expects an adapter to opt out — `config.typing_indicator` defaults `True` and drives a base `_keep_typing` loop; this repo never disables it. |
| Treat product behaviour as suspect. Goals, grants, trust and message recovery are genuinely this repo's, and upstream has no equivalent. | Prefer the subtractive remedy: adopting the upstream mechanism should usually *delete* local code. If the switch adds LOC, say why. |
## Review priority

| DON'T (suppress / flag-as-shape) | DO (real finding) |
|---|---|
| Demand scale-hardening, extra flags, or defensive edge cases for a three-agent fleet. | Flag anything that can leave a **running agent with no working phone line** — a partial install, an unhandled reconnect path, a config the gateway reads only at boot. |
| Re-flag the missing checkpoint / history backfill. | Flag a **new** silent-loss path: an inbound message that can be dropped without a log line. |
| Treat README wording edits as churn. | Flag **README↔code drift** on the env-var contract in particular. `plugin.yaml` is the authority; the README table is a summary and the two must agree. |
| — | Flag any **literal secret**, or a probe that surfaces secret values (`env`/`printenv`, `cat` of credential files). This adapter holds the chat token. |
| — | Flag a change that alters the **plugin id** `plow-chat-platform`, or the installed file set, without a matching change in `agent-mgr`. Every agent's `config.yaml` names that id, and `agent-mgr`'s installer names that directory. |
| — | Flag **echoing a ticket, token or full frame** into logs. The ws ticket is single-use and short-lived, which is not the same as harmless. |
| — | Flag a change that a **sibling repo owns** per `plow-hermes-agent` README § The repos: the `plow-gog` / Latch tool grammar (latch owns it), per-chat state written under `$HERMES_HOME` instead of plow, boot and gateway config (the base image). The test is who else would have to change if the fact changed. |

## Product context

A Python Hermes platform adapter (`plow_chat`) plus its manifest. The suite
stubs the `gateway.*` modules Hermes supplies at runtime, so it needs no Hermes
install and touches no network.
