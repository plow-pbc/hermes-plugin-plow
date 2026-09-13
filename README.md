# hermes-plugin-plow

The **Plow Chat platform plugin for Hermes** — an agent's phone line. Inbound
arrives over a WebSocket the plugin dials out on; outbound and cron delivery go
back through the chat REST API.

```
plow-chat-platform/     exactly what gets installed, and nothing else
  plugin.yaml           the manifest -- registers the platform id
  __init__.py           the chat adapter and the tools; Hermes loads it from the plugin root
  _transport.py         the transport the chat adapter runs, written to be shared with the email platform tracked in plow-pbc/hermes-plugin-plow#109
  email.py              the email-line adapter: plow_email, on the same transport
tests/                  the adapter suite
```

The directory is named for the plugin id so the install can be a directory copy:
`plow-hermes-agent`'s Dockerfile fetches this repo at the pinned SHA and bakes
`plow-chat-platform/` into the image. Nothing else here — README, tests, justfile — reaches an agent.

> **Ordering.** Quoted replies require [`plow-pbc/plow#1827`](https://github.com/plow-pbc/plow/pull/1827)
> and attachment indexes from [`plow-pbc/plow#1832`](https://github.com/plow-pbc/plow/pull/1832):
> deploy both API changes before pinning this plugin, or reply context will be absent
> or indexed media replies will remain unresolved.
> Invite retry receipts require
> [`plow-pbc/plow#1869`](https://github.com/plow-pbc/plow/pull/1869): without it
> a reopened invite carries no `invite_reopened` marker, so this plugin reads it
> as possibly-delivered and declines the retry that would have worked. It fails
> safe, never sending a duplicate, but deploy that API change before pinning
> this plugin or recoverable invites are silently dropped.
> This plugin also requires a Plow API that serves agent-invite consent,
> `/v1/auth/agent-invites/opportunities`,
> `/v1/auth/agent-invites/opportunities/{opportunity_uid}/send`,
> `POST /v1/chats` (outbound thread creation — `plow_start_group_message`
> 404s against an older API, so that API change deploys before any
> `agent-mgr` SHA advance),
> `PUT /v1/contacts/{handle}` (`plow_name_contact` — the handle-keyed contact
> book, superseding the per-participant contact route of
> [`plow-pbc/plow#1752`](https://github.com/plow-pbc/plow/pull/1752),
> "Owner contacts"), and
> `GET /v1/agents/me` returning an `agent` object (identity and persona name —
> the legacy `/v1/agents/cloud/me` alias serves neither, and its 404 reads as
> "not one agent" and runs on quietly rather than failing loudly). Hermes hosts
> without deferred-question support still run Plow Chat and standing-consent
> invites, but skip the ask-owner-first invite flow. Deploy the API first, and
> only then bump `PLOW_CHAT_PLUGIN_SHA` in `plow-hermes-agent` (and
> `agent-mgr`'s `images.hermes_local` base tag, which can't move past it).
> Installing this plugin before the API is available fails loudly instead of
> silently skipping delivery.
> `plow_email` carries a further prerequisite: until plow lists Gmail threads
> as chats in `GET /v1/chats` and dispatches `POST /v1/chats/{uid}/messages`
> by provider, the adapter gets no inbound turns and its replies fail. That
> work is tracked by [`hermes-plugin-plow#109`](https://github.com/plow-pbc/hermes-plugin-plow/issues/109) —
> don't bump the plugin pin to a SHA where `plow_email` is registered until it ships.

## Where changes go

This repo is one of several that assemble a Plow agent. The map of which repo
owns what is in
[`plow-hermes-agent` README § The repos](https://github.com/plow-pbc/plow-hermes-agent#the-repos);
read it before a change that touches a neighbour. The test is **who else would
have to change if this fact changed** — if the answer is a sibling, the change
belongs there; this repo only follows, by bumping its pin if it holds one.

Not here:

- The `plow-gog` argv grammar, and what a Latch tool says about itself —
  [`plow-pbc/latch`](https://github.com/plow-pbc/latch) vendors the binary,
  pins its version, and owns the only bump checklist. The mail approval hook
  deliberately mirrors Latch's global-flag stripping from `accountAt` and
  `planPlowGog` in `packages/device-core/src/providers/plowGog.ts`; keep that
  list and its value consumption in sync when Latch changes it. This follows
  the same grammar, rather than defining another one: the hook must distinguish
  flag values from the action path, since a value can itself be `gmail`.
  Classification uses a copy; execution and approval hashing retain raw argv.
- Per-chat state the owner sets or clears — trust, contact labels, anything
  keyed by a `cht_` id — [`plow-pbc/plow`](https://github.com/plow-pbc/plow).
  A file written under `$HERMES_HOME` instead is invisible to the dashboard
  and to support.
- Boot, `plow-init`, the gateway config seed, and the base persona —
  [`plow-pbc/plow-hermes-agent`](https://github.com/plow-pbc/plow-hermes-agent),
  which pins this plugin rather than being configured by it.

Examples:

- Adheres: #61 deleted `_invite_message_template` — the `$100 in cloud credits`
  line and the activation-code placeholders — so plow composes the whole invite,
  net −32 LOC: https://github.com/plow-pbc/hermes-plugin-plow/pull/61
- Violates: #64 put ~110 lines of `plow-gog` verb tables, flag parsing and an
  explicit re-implementation of latch's `isHelpInvocation` in this plugin — a
  second copy latch's pin-bump checklist does not know about:
  https://github.com/plow-pbc/hermes-plugin-plow/pull/64

## Who consumes this

[`plow-pbc/plow-hermes-agent`](https://github.com/plow-pbc/plow-hermes-agent),
the base image every hosted Plow agent boots, pins one commit of this repo as
`PLOW_CHAT_PLUGIN_SHA` in its Dockerfile and fetches `plow-chat-platform/` at
build time. That is the production consumer. The Docker fleet below is the
deprecated one.

[`plow-pbc/agent-mgr`](https://github.com/plow-pbc/agent-mgr)'s Docker fleet
gets this plugin the same way — bundled in that base image, not installed
separately. Bumping `PLOW_CHAT_PLUGIN_SHA` there, pointing `runtime/stack.json`'s
`images.hermes_local` at the new base, and running `agent-mgr deploy` moves the
fleet. It lands at `/opt/hermes/plugins/plow_chat/` on the image, as the same
four files and nothing else: `__init__.py`, `_transport.py`, `email.py`,
`plugin.yaml`.

**Pinned by SHA, never vendored.** A branch ref would silently re-point a running
agent on the next push here, and this plugin holds the chat token. A vendored
copy stops receiving fixes — `sams-admin-hermes-agent` proved that, carrying a
v0.1.0 fork until it was archived.

## Configuration

Read from the agent's own dotenv (`$AGENT_HOME/.env`), never from the image or a
URL in git.

| var | required | meaning |
|---|---|---|
| `PLOW_AGENT_TOKEN` | yes | the line-scoped bearer activation mints |
| `PLOW_HOME_CHANNEL` | yes | the home chat, `cht_…` — where cron and default output land, and must be a phone-line chat (the line's `provider_type` is `imessage`). Must be inside the credential's grant; a grant without it refuses to connect |
| `PLOW_API_BASE` | no | API base, default `https://api.plow.co` (no `/v1` suffix) |
| `PLOW_MCP_URL` | no | the Mac relay URL plow-init exports when the account has a Mac; when set, the plugin adds a system-prompt section that makes the Mac the default for owner work |

The persona name shown to the model is read from `GET /v1/agents/me`'s
`agent.name` at reach refresh, not from a dotenv var — the owner sets it
server-side (`PATCH /v1/agents/{uid}`). Falls back to the line's own
`display_name` when unset, including the API's creation default
(`cloud agent`); either way, the server-assigned line name and
the iMessage contact card are untouched.

Diagnostics — agent status frames, 💾 background-review posts, ⏳ long-running
heartbeats, ⚠️ turn-stop warnings — are dropped in **every** room unless the
agent's `verbose_output` setting (the dashboard's "Verbose agent output"
toggle, read from `GET /v1/agents/me`; only a quiet answer is cached, so
turning the toggle off is obeyed on the next line rather than a minute later) is true; the typing indicator already shows the turn is
running. Hermes gives them no metadata of their own, so they are recognised by
the text they open with, and the room rule below deliberately does not reach
them: they are the runtime describing itself, never the turn's answer, so
withholding one can never withhold the message the owner wanted.

The model's own **mid-turn prose** is gated by the same setting, but only
where someone else is listening. What counts as mid-turn is a metadata test,
not a prefix one: Hermes marks the turn-final reply `notify` and a cron
delivery `job_id`, and anything carrying neither, sent while a turn is open, is
the model working out loud.

**The answer goes last, and that is still a prompt rule, because the delivery
seam can withhold prose but cannot recognise an answer.** Hermes reads whatever
the model wrote *last* as the turn's final response. The model's habit is to
write its real message, call one more tool — recording an outcome, per the
variant personas — and then write itself a note, so the note is what gets
marked and the real message is indistinguishable from the commentary.

That is why withholding is confined to rooms with a third party in them. There,
a withheld answer costs a re-ask, while a delivered one can cost a cart, a
shipping address and a card read by somebody who should not have them — the
disclosure this gate exists to stop. In the owner's own 1:1 there is no such
reader, so nothing is withheld and the answer cannot go missing.

Buffering the withheld bodies and flushing the last one at turn end does not
rescue the shared-room case and was removed: picking "the last one" is the same
guess the seam cannot make, so a flush publishes whatever the model happened to
write last — the running commentary, into the room the gate was protecting.
`plow-hermes-agent`'s seed config records the same conclusion from an earlier
attempt, measured live: a whole onboarding introduction disappeared and the
owner's turn became *"Already saved that. Now I'll wait for her next reply."*
The ordering is the model's to get right, and `_ANSWER_LAST` closes every
channel prompt asking for it — appended once in `_channel_prompt`, the one seam
both production paths go through, after the identity opener each prompt has to
start with.

One exception rides with it: a tool that *posts* to the chat is itself the
answer. A successful `plow_send_sequence` has already delivered the turn's
reply, so the guard drops the prose that follows — the rule says so, or a
model that finished its tools first would have its answer suppressed. That
drop is lifted again by a later message or goal wake for the same chat: once
one arrives the lifecycle is ambiguous, and a duplicated line of intro prose
is the price of never losing the reply the wake was queued for.

`plugin.yaml` is the authority on this list; the table is a reader's summary.

**Which chats the agent serves is not configured here at all.** The credential's
grant (`sessions.chat_uids`, served by `GET /v1/chats`) decides, refreshed on
every reconnect. Per-chat checkpoints persist under the agent home, and a
reconnect backfills each granted chat from its checkpoint, so a socket gap
drops nothing.

An agent's first-ever connect (no home checkpoint yet) hands hermes one setup
turn in the home chat, signed by Plow, not the owner, and free to end in
`NO_REPLY`: where the owner's world is (their Mac, through Latch), who the agent
is, and how it behaves among the owner's people, saved as its own memory note.

One person's rapid-fire messages are one turn: inbound is buffered per chat
for a 2s window that resets on each arrival — iMessage's bubble + link-preview
split, or a thought sent as two lines, reaches hermes as a single message
instead of the second interrupting the first. A change of speaker closes the
burst, and a slash-prefixed message closes it on both sides so commands remain
distinct turns. The ack is the burst's last uid, so a restart mid-burst
backfills the whole burst; a hand-off that fails is retried where it sits, with
the rest of the chat waiting behind it.

Inline replies carry the quoted sender, time, and body as untrusted turn data,
with a part label only for media. If the reply has no attachments of its own,
the adapter delivers the quoted parent's media through the normal attachment
path: the matching provider
part when its index is available, otherwise all parent attachments. Everything
comes from the message frame; no parent-message lookup is made.

### The email line (`plow_email`)

The agent's `@plow.co` address is its own Hermes platform, registered by this
same plugin on the same credential and the same transport helpers, each
platform holding its own socket. Every chat resource names its line's
`provider_type`; this adapter serves the `email` ones and the phone-line
adapter serves the `imessage` ones, so a mail never renders as an SMS room
and a text never renders as an email. Sessions are keyed
`plow_email:<dm|group>:<cht_id>`; the platform hint names the line's
address, read off the thread's own agent participant at connect. Replies go
out through the same chat send endpoint — plow dispatches on the provider —
with no approval gate: this is the agent's own line, like its number. Only
the turn's answer, a cron delivery, or a turn-less send is ever mailed;
mid-turn prose and the runtime's diagnostics are dropped. No cron home
(`PLOW_HOME_CHANNEL` stays the phone line's), no roster policy on
multi-address threads, no backfill across a socket gap, and no delivered
attachments in v1 — an attachment-only mail arrives as a placeholder naming
the count ([hermes-plugin-plow#119](https://github.com/plow-pbc/hermes-plugin-plow/issues/119)).

`plow_email` needs no dotenv entry of its own: it reads the same
`PLOW_AGENT_TOKEN` as `plow_chat`, and Hermes's `_enable_plugin_platform`
auto-enables every registered plugin platform whose `check_fn` passes, with
no `is_connected` gate — so it comes up on the pin bump alone, same as the
phone line.

### Group discretion and full trust

The room mode is an owner-scoped, per-chat preference served on `GET
/v1/chats/{uid}`. Before handing off each inbound burst, the adapter refreshes that
chat so a dashboard change applies to the next message. The owner's own turns carry
the owner's authority everywhere -- a DM, an untrusted group, a trusted group. Trust
is the one flag that extends it to anyone else: a human member's turn in a trusted
group carries it, inside that group; it never follows them into a DM. A peer agent's
turn and a goal wake have no human speaker, so trust grants them nothing. In
discretion, a member's ask still waits for the owner's yes given in this thread,
judged from the conversation, disclosing only what answers the request. A standing
secret — a password, backup code, API key, raw token, or full card number — is
refused regardless of authority. Email sends and calendar-conflict overrides need a
turn with the owner's authority; an email's approval posts in the room that asked,
and an override posts none. A turn without authority cannot send to other chats, set
goals, or list the owner's other rooms, and only the owner's own turn writes
contacts. A group the owner deliberately stands up to act on their behalf begins trusted;
ordinary outreach the owner asks for — texting a contractor, a neighbour, a
merchant — begins with discretion, as does a group another member starts. Only
the owner can change that later.

The `plow_set_conversation_trusted` tool writes the same API preference as the
dashboard; opening a trusted thread is owner-only too. Both only succeed on an owner-
authored Plow Chat turn where the model passes `confirm=true` for an explicit owner
request. Member turns and calls outside an active chat turn cannot change either.

This plugin version requires a Plow API that publishes the required `trusted`
chat field and `PUT /v1/chats/{uid}/trusted`. Deploy that API first: against an
older API the per-message refresh fails loudly and the chat waits for retry
rather than guessing a trust state.

### Speaking in another chat

Hermes keeps one session per chat, and this adapter drops the echo of the
agent's own sends. So a message the agent posts to chat B from a turn in chat
A is invisible to chat B's next turn unless it is recorded there. The
`plow_send_message` tool is the one sanctioned way to post cross-chat; it goes
through the adapter's `send()` like every other outbound message (the grant,
and the confinement of a turn without the owner's authority, apply exactly as
for a reply). `plow_list_chats` is where its `cht_` id comes from: a live `GET
/v1/chats` — the same read that establishes reach, so the credential's grant
is the whole listing — reduced to
id, kind, title, the humans by name and handle, and trust. Only `active` rooms
are listed. The route excludes just `failed`, so it serves rooms still being
set up as well; `send` requires `active` and answers a pending one with `409
chat_not_ready`, and a listing whose whole job is to source a sendable id has
no business offering a choice that fails. Titles and names in
it are other people's words, so the result carries the same untrusted marker
every such block does; a title the provider defaulted to the room's own
comma-joined handles is dropped, because that column is how the API says
"nobody named this". It is refused on a turn without the owner's authority for
the reason the alias registry publishes no participant names: a listing that
carries handles must not let one room's members enumerate the owner's others.
Recording lives in that same `send()`: when a turn's message lands in a chat
other than the
turn's own, the adapter mirrors the text into that chat's session as an
assistant turn with upstream's `gateway.mirror` — the mechanism Hermes uses
for cron and `hermes send` deliveries — on the delivery's own coroutine, so a
caller that stopped waiting cannot strand a delivered message unrecorded. A
chat's session is born on its first inbound message, so a chat that has never
spoken has nowhere to record to: the adapter logs a warning and that chat
will not remember the send. A thread `plow_start_group_message` created is
in that state; one it resumed is handled like any other cross-chat send,
which records the opener only where a session already exists (a thread
resumed before anyone replied has none, and logs the same warning). Posting
to the Plow API directly from a
script bypasses all of this and leaves the target chat amnesiac; the tool
exists so the model never has to.

### Recall from other chats

Hermes keeps one session per chat, so a turn in one chat knows nothing of the
others unless told. On every Plow Chat turn the plugin's `pre_llm_call` hook
runs an OR-query over the Hermes session
store and appends up to six dated one-line snippets from other sessions to the
turn (upstream's seam for per-turn recall: the user message, never the system
prompt). The query takes the message's own words first and then the agent's own
last words in this session, so a message with something to say fills the term
budget alone while a bare acknowledgement — the turn where someone is answering
a claim the agent made from another chat — still has a topic to search on. It
matches message content only, and skips a snippet that renders as tool-call
JSON: the store indexes serialized tool calls too, so an unscoped query matches
inside tool arguments. The room is the boundary, not the asker: the home chat (the owner's
own DM) and a full-trust room recall from every chat, the owner's DMs included —
full trust also enables this broader recall. Every other turn, an owner's turn in a
group using discretion included, recalls only from its own chat's earlier sessions.
The current session is never recalled. Snippets are labelled as data, not
instructions, the same way the roster is. A failing store is not caught
here: Hermes isolates and logs a failing hook and the turn proceeds without
recall, so the failure is visible in the gateway log instead of hidden.
Recall is a filter over the store's thirty best matches across all chats, so
in a busy install a room-scoped turn can find nothing even when its own chat
holds matches; that ceiling is deliberate until it is felt. Hermes stamps
injected context onto the turn's wire copy and replays it for the life of
the session, so a snippet recalled once stays in that session's context
afterwards.

### What a group thread is called

The home chat is always `Plow Chat`. Every other granted thread is named from
its own iMessage title — `<title> (<cht_ id>)`, the uid suffix making a title
structurally unable to take another room's name — or by its bare `cht_` id
when nobody has titled it. Titling the thread in iMessage is how it gets a
name; there is no name configuration here.

The result is published into the image's `channel_aliases.json` overlay on
every (re)connect, which is re-applied on every channel-directory build and
load, so a granted thread is addressable — and visible to `send_message
action="list"` — before it has ever spoken. The suffix does not get in the way
of addressing: the image's resolver falls back to an unambiguous prefix match,
so `plow_chat:#Snoqualmie Cabin Cleaning` reaches
`Snoqualmie Cabin Cleaning (cht_...)`. A name grants nothing — reach and
authority stay with the credential's grant. A retitle mid-connection shows up
on the next reconnect.

### Multi-agent groups

The chat roster identifies this line as `relationship: self`, any other Plow
lines as `relationship: peer`, and joins each agent to the human it represents.
The adapter turns that structured roster—plus the current sender—into one
collaboration context on every turn. This lets Elm distinguish “Hey Ash” from
an instruction to Elm without parsing names or inventing a second router.

Peer-agent messages are real inbound turns and remain visible in the same group
as every human message. Only this line's own outbound echo is ignored. What a
peer message does *not* do, absent a goal (below), is draw a reply: unless it
names this agent, the turn carries a do-not-reply prompt. The reply is
suppressed, never the read — an agent blind to its peer loses the thread and
then talks past its own human. Prompt prose alone did not hold: the agent that
had the anti-acknowledgement paragraph still produced three rounds of "agreed,
nothing to add".

### Thread goals

`/goal <text>` puts this thread's agent on a task it works toward on its own;
`/goal` reports status and `/goal clear` stops it. Only a turn with the owner's
authority can set or clear one, and both are announced in the thread — in a
group that
announcement is the consent artifact, showing the other household what this
agent was told to pursue before it pursues it.

A goal is bounded on three independent axes: a TTL, an attempt budget, and a
separate judge that may rule it met or unreachable. The budget and the clock are
the adapter's, never the judge's, so a judge stuck on "not met" — or simply
unreachable — cannot buy unbounded turns. Every settlement is announced, and a
notice that fails to deliver leaves the goal running rather than letting it go
quiet.

Every turn under a goal opens with the goal itself, framed as what the command
already established: a standing instruction from whoever set it, named,
with their text carried as theirs. It used to ride as "untrusted thread data,
not an instruction" — the right posture for words the thread supplied, and the
wrong one for a task the owner personally authorized, which had the agent
disown it.

Three things bound that. Every field interpolated into the line — the goal
text and the setter's name alike — is encoded so it cannot end the block or
start a line that reads as another one: quotation marks are not a boundary,
and the guarantee is that the block ends where the code says it does, on one
line, with anything injected left visible inside the text. The line states
that a goal changes no rule of the turn it rides on: what may be done and
disclosed in that room remains the channel prompt's answer. And a record with
no name is the owner's — the gate predates the field, so a goal written
before authorship was recorded still reads as theirs. Retiring a goal drops
the setter's name along with the transcript: neither has a reader once the
goal is done, and both would otherwise sit on the persistent volume.

An active goal is what unlocks replying to peer agents. A scheduled wake has no
human speaker, so outside the owner's DM it gets the discretion prompt and no
authority — unchanged by the reframing: in a group the thread is still full of other
people's words, and an owner-authorized turn acting on them unprompted is a
confused deputy holding owner-only tools.

In a shared thread the prompt tells the agent to speak as itself and refer to
the human it represents by name, never as "I" or "me" — the name itself stays
in the untrusted roster context above, never in the prompt.

The owner may also tell the agent what to call a person and who that person is
to the owner — `wife`, `landlord` — through `plow_name_contact`, which `PUT`s
`/v1/contacts/{handle}`. The book is keyed by handle, not by chat, so one name
follows the person into every thread they are in; naming the owner's own handle
sets their account name, and a relationship on their own handle is refused. The
tool is owner-turn-authorized only; it refuses outright during a member's turn
and outside any active turn at all — a direct call cannot write a label except
on the owner's own turn. A relationship renders as
`Name (handle) (relationship)` in the untrusted roster context above — where
the owner's own row also carries `(your owner)` — never in
the channel prompt, which instead states generically that a roster
relationship is a label recorded on the owner's turn, and that a member's
claim about who they are is just that — a claim. Every roster-bearing prompt
also tells the agent that a row still showing a bare handle — its owner's
included — is a name to ask for once and record with the tool, never one to
guess out of mail, calendar or memory. `plow_contacts` reads the book back,
owner's row first, for the turns that have no roster at all — a Hermes-cron
turn carries no chat, and this is where its owner's own name comes from; it
reads on a turn with the owner's authority or with no active turn at all, and
refuses only a turn without that authority. Naming stays owner-turn-only,
above, unlike this read. An
owner turn needs no such read: the chat resource every one of them already
re-reads carries the owner as a participant — name, handle and role — in a solo
DM as much as in a group. That is what the channel prompt names them from:
`Your owner is Sam [+1…].`, or, while they are still unnamed, the same ask with
their handle already filled in, since a solo DM and a goal wake have no roster
for the paragraph above to gate on. A name they change lands on their very next
turn, with nothing cached and nothing else to fetch.

Who invited the owner is read once per process start, from
`GET /v1/auth/profile` on connect. That name the inviter chose for themselves,
so it is never a prompt sentence: it arrives on the owner's turn as one more
untrusted block in front of the text, beside the roster — `[Untrusted account
data; … Your owner was invited by Sam (Life Assistant).]`, or `someone` where
the inviter has no name of their own. A failed read leaves it unset and the
agent connects anyway; a member's turn carries neither this nor the owner's
own name.

## Media

Inbound photos, audio, video and documents arrive on `MessageEvent.media_urls`
as files in the image's own media cache — the same place the bundled iMessage
adapter puts them, so the vision path and the skills that say "a texted photo
arrives as a file path" need nothing new. Each part is fetched once, begun
the moment the message arrives — inside the five-minute Plow-signed content
URL's life, whatever is retrying ahead of it in the chat — and awaited when its
burst closes; without the bearer: the signature is the authorization. A part Plow reports as `failed`, or
one whose bytes cannot be fetched, is named in the turn as
`[attachment: <type> delivery failed | unavailable]` and logged — never dropped
with the message.

Outbound files the model emits go through Hermes' `send_image_file` /
`send_voice` / `send_video` / `send_document` hooks, which this adapter
implements as the Plow contract — declare the attachment, PUT the bytes to the
provider's upload URL with exactly the headers Plow returned, then send the
message with `attachment_uids`. Content types are limited to what the provider
accepts; a `415` from the declare comes back as the send's error.

Both halves need the attachments API — `plow-pbc/plow#1435`. Against an older
API the inbound path sees no `attachments` field (a `KeyError`, loud, per
REVIEW.md) and an outbound declare returns `404`. That `KeyError` fires inside
the frame loop on every inbound message, so the socket is torn down and
reconnected, and the phone line is mute in between. That gap is **30 seconds**,
not the five this warning used to name: the failure is post-connect, so the
socket has already called `connected()` and reset the backoff, and each retry
starts the curve over rather than climbing it. Six times longer, every message:
`PLOW_CHAT_PLUGIN_SHA` in `plow-hermes-agent` must not be bumped to this
commit — and `agent-mgr`'s `images.hermes_local` base tag can't move past it
— until `plow-pbc/plow#1435` is deployed to every API the fleet's agents talk
to.

## One implementation, two delivery paths

This adapter is the only plow_chat implementation; see Who consumes this
above for the delivery paths. `plow-pbc/plow`'s blessed exe.dev image bakes
the same tree at the same `PLOW_CHAT_PLUGIN_SHA` pin.
The old second implementation in `plow`'s `cloud-agents/` was retired by the
unification (`plow-pbc/plow#1420`); its multi-chat credential-scope design is
what this adapter now is.

`plow-pbc/plow`'s `cloud-agents/hermes/HERMES_INTEGRATION.md` remains the best
reference for the underlying Hermes behaviour.

## Development

```sh
just test
```

The suite loads the plugin directory as a package and stubs the `gateway.*` modules Hermes
supplies at runtime, so it needs no Hermes install and touches no network.

## Provenance

The history here is `plow-pbc/seed-hermes-plow`'s, carried over in full — this is
where the adapter was written, including the group layer ported onto it in
August 2026. That repo is **archived**: the SEED pattern it belonged to is
retired, and its remaining artifacts (`plow-connectors`,
`create_plow_chat_curl.sh`) stay frozen there at their pinned SHAs, which still
resolve because archived public repos remain readable.

What changed on the way over: the `ref/` layout is gone, and with it the ~30-line
root shim that existed only to bridge it. The adapter sits where Hermes loads it,
so an agent's installed plugin no longer carries a `ref/hermes-plugin/plow_chat/`
directory inside its home.

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE). Copyright 2026 The Plow Collective, Inc.

"Plow" and the Plow logo are trademarks of The Plow Collective, Inc. The license grants no trademark rights.

## Ordered owner-DM delivery

`plow_send_sequence` sends a bounded sequence to the active turn's solo owner
DM. It accepts no destination or file path.

Owners can call it only once the base image bumps its plugin pin: a deployed
agent runs the plugin baked into its image, not this repository, so landing the
tool here does not by itself put it in front of anyone. Bumping the pin is a
post-merge step — it names a merge commit, which does not exist while the change
is still under review — and rebuilding and re-pinning the blessed image follows
it.

Example tool arguments:

```json
{"items":[{"type":"text","body":"Here are the previews."},{"type":"photos","asset_ids":["preview_a","preview_b","preview_c","preview_d"]},{"type":"pause","seconds":4},{"type":"text","body":"What do you think?"}]}
```

The variant image supplies `/srv/plow-assets/manifest.json`:

```json
{"version":1,"assets":{"preview_a":"preview-a.png","preview_b":"preview-b.png","preview_c":"preview-c.png","preview_d":"preview-d.png"}}
```

The manifest, image files and directories through `/` must be root-owned and
not group/world writable. Symlinks and paths outside the asset directory are
refused. Supported images are PNG, JPEG, GIF and WebP, at most 8 MiB each. The
plugin validates and reads every selected asset before any delivery. Assets
are variant-owned; this plugin ships no manifest or life-specific IDs.

Limits: 24 items, 4 photos per item, 16 photos / 32 MiB total, 4,000 characters
per text / 24,000 total, and 60 seconds of total pacing. Pauses accept finite
numbers from 0 to 15 seconds. Adjacent deliveries have a one-second gap; an
explicit pause replaces that gap, including a zero-second pause. Operations
serialize per chat and have a 180-second deadline, including queueing. Ending
the turn or disconnecting cancels its outstanding sequences. Existing turn
typing continues through pauses and is rearmed by delivered messages.

The receipt contains `success`, `completed`, and `failure`. Every completed item
has its zero-based `index`, `type`, and `message_ids` (empty for a pause). A
failure names the first unresolved `index`, a `status` (`rejected`, `failed`, or
`delivery_unknown`) and an error. When a four-photo stack is explicitly
rejected with HTTP 422, the tool may send individual photos using the existing
uploads. If that fallback stops partway, `failure.message_ids` preserves the
confirmed sends and `failure.photo_index` identifies the unresolved photo.
Timeouts, 5xx responses and malformed successful POST responses are delivery
unknown: they never trigger fallback or automatic replay. Inspect chat history
before sending any remaining items; never replay the entire sequence blindly.

Tool arguments and receipts stay in the agent's ordinary tool-call history.
Successful tool delivery already sent the copy: the adapter suppresses subsequent
text replies to that chat for the rest of the active turn, logging the chat and
the suppressed length but never the body. Suppression tracks the turn's latest
sequence, so a failed, rejected, or delivery-unknown sequence — including one
that follows a successful sequence in the same turn — reopens the reply path.
A message or goal wake handed off for the same chat reopens it too, and keeps
it open: no later sequence in that lifecycle can re-arm suppression, because
Hermes may answer the queued event from the turn the sequence belongs to. Suppression runs in the one guard every outbound message passes, so it covers
text, `MEDIA:` delivery and verbose status frames alike. Other chats and later turns retain their ordinary
behavior. The tool does not interpret in-band markers.
