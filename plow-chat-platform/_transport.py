# Copyright 2026 The Plow Collective, Inc
# SPDX-License-Identifier: Apache-2.0
"""The transport the chat adapter runs -- the API base and credential, the
granted socket and its reconnect loop, the reach and identity reads, and the
roster readers -- written to be shared with the email platform tracked in
plow-pbc/hermes-plugin-plow#109. Policy stays with the platform that owns it.
"""
import asyncio
import contextvars
import logging
import os

import aiohttp

BASE = os.environ.get("PLOW_API_BASE", "https://api.plow.co").rstrip("/")
RECONNECT_SECONDS = 5
log = logging.getLogger(__name__)


class _PlowAuthError(Exception):
    """The credential itself was refused (401). Terminal: every retry presents
    the same revoked token, so the caller must stop, not sleep."""


# The terminal stop both platforms report, as `_set_fatal_error` takes it. The
# message reaches the operator through `hermes status`, so it says what to do.
CREDENTIAL_REFUSED = ("credential_refused",
                      "Plow rejected the agent token (401); re-credential this agent")


def _auth_raise_for_status(resp):
    """The one status seam for every request that presents the credential.

    Status BEFORE parse (a proxy 401 is not JSON), and 401 ONLY -- a 403 is
    resource-scoped (removed from one chat) and keeps warn-and-retry.
    """
    if resp.status == 401:
        raise _PlowAuthError
    resp.raise_for_status()


def _bearer():
    return {"Authorization": "Bearer " + os.environ["PLOW_AGENT_TOKEN"]}


async def _granted_chats(http, auth):
    """The credential's chat listing, every provider, as `GET /v1/chats` serves it."""
    async with http.get(f"{BASE}/v1/chats", headers=auth) as resp:
        _auth_raise_for_status(resp)
        body = await resp.json(content_type=None)
    if body["has_more"]:
        raise RuntimeError("the granted chat listing is truncated")
    return body["data"]


async def _read_identity(http, auth):
    """`GET /v1/agents/cloud/me`: the signup block and this agent's number.

    200 answers. 404 is the documented "this token is not one agent" -- a
    wildcard or multi-line grant -- and answers None so the caller keeps what
    it holds. Anything else is not an answer about identity: through the
    credential seam (a 401 is terminal), then fail like the grant read so the
    caller retries rather than silently running without the offer.
    """
    async with http.get(f"{BASE}/v1/agents/cloud/me", headers=auth) as resp:
        if resp.status == 200:
            me = await resp.json(content_type=None)
            return {"signup": me.get("signup"), "number": (me.get("line") or {}).get("provider_key")}
        if resp.status == 404:
            return None
        _auth_raise_for_status(resp)
        raise RuntimeError(f"the identity read returned HTTP {resp.status}")


async def _ticket(http, auth):
    """Mint immediately before connecting: the ticket lives 60s and is
    single-use, and revocation is re-checked at consume, so a cached one is a
    4401 close."""
    async with http.post(f"{BASE}/v1/ws/ticket", json={}, headers=auth) as resp:
        _auth_raise_for_status(resp)
        return (await resp.json(content_type=None))["ticket"]


def _socket(http, ticket):
    return http.ws_connect(f"{BASE.replace('http', 'ws', 1)}/v1/ws?ticket={ticket}", heartbeat=30)


async def _serve(session, on_drop, tag):
    """The reconnect loop the chat adapter runs, written to be shared with
    the email platform tracked in plow-pbc/hermes-plugin-plow#109.

    `session(http)` is one connection attempt -- read reach, mint, connect,
    consume frames until the socket closes or raises. Returns only on a
    revoked credential: every retry would present the same dead token
    (observed on the str agent 2026-08-27 -- one WARNING a minute, the line
    dead, the adapter reporting itself connected). `on_drop` marks the
    adapter disconnected on either exit.

    Returning IS the signal: a revoked credential is the only way out, so the
    caller records the fatal state and tells the gateway itself. Doing that
    from in here would run the runner's handler -- which cancels this very
    task -- inside the loop it is cancelling.
    """
    while True:
        try:
            async with aiohttp.ClientSession() as http:
                await session(http)
        except _PlowAuthError:
            log.error("[%s] credential refused (401) -- stopping the listen loop; "
                      "re-credential this agent", tag)
            on_drop()
            return
        except Exception as exc:              # noqa: BLE001 - reconnect, never die
            # TYPE only: the ticket is a query parameter, so a non-101
            # handshake raises an exception carrying the whole URL, and
            # that ticket is still live.
            log.warning("[%s] websocket error: %s", tag, type(exc).__name__)
            on_drop()
        await asyncio.sleep(RECONNECT_SECONDS)


def _one_line(text):
    """A person-supplied name, made safe to interpolate.

    Whitespace collapses to single spaces -- a newline in a name opens a line
    that reads like a fresh instruction, which matters most where the name
    lands in system authority -- and the result is capped, so no one name can
    crowd out the prompt it sits in. Empty is empty; each caller owns its own
    fallback.
    """
    return " ".join(str(text or "").split())[:100]


def _participant_identity(participant):
    """Choose a one-line server identity: meaningful name, then full handle."""
    handle = str(participant.get("provider_key") or "").strip()
    display = _one_line(participant.get("display_name"))
    return display if display and display != handle else handle


def _self_agent_line(chat):
    """The self agent participant's line dict, {} when the roster lacks one."""
    agent = next((p for p in chat.get("participants") or []
                  if p.get("type") == "agent"
                  and p.get("relationship") in (None, "self")), {})
    return agent.get("line") or {}


def _agent_name(chat):
    """The line's persona name ("Elm"), or None for an unnamed line.

    Read from the chat's own agent participant, so the DB stays the single
    identity source and a rename needs no reprovision — it lands at the next
    reach refresh (reconnect or group-send adoption), which is deliberate: a
    rename is a rare coordinated ops event (it ships a new vCard too), not
    worth an HTTP fetch per delivered message. `.get`-tolerant like the rest
    of the listing readers: a pre-persona server omits `line`, and an unnamed
    line omits `display_name`.
    """
    return _self_agent_line(chat).get("display_name") or None


def _represented_member(chat, agent):
    uid = agent.get("represents_participant_uid")
    return next((p for p in chat.get("participants") or []
                 if p.get("type") == "member" and p.get("uid") == uid), None)


def _is_solo_dm(chat):
    """A 1:1 thread: one human, and no peer agent to collaborate with.

    The gate for the roster prefix. NOT "has no peer" on its own -- a
    human-only group has several people who can speak and a current speaker
    the model needs to tell apart, even with no other agent in the room.
    """
    participants = chat.get("participants") or []
    if any(p.get("type") == "agent" and p.get("relationship") == "peer" for p in participants):
        return False
    return sum(1 for p in participants if p.get("type") == "member") <= 1


def _chat_type(chat):
    """`_is_solo_dm` is the one answer to "is anyone else in this room?", and
    it counts a peer agent as somebody. Counting humans alone called a room
    holding one human and another household's agent a DM, which handed its
    scheduled wake owner authority over peer-written content."""
    return "dm" if _is_solo_dm(chat) else "group"


def _owner_identity(chat):
    """The owner's name and handle, off the chat every owner turn refreshes.

    The chat resource carries its owner as a participant -- name, handle and
    role -- in a solo DM as much as a group, even though a DM renders no roster
    BLOCK. So there is nothing to fetch: the turn already re-read the one
    resource that answers this, and a name the owner changes lands on their
    very next turn with no cache and no second request.

    A chat with no owner participant is a broken contract, not a case to
    render around. The raise names the chat because the email line reads this
    on every turn, not only an owner's, and `_serve` logs the exception TYPE
    only -- so an unnamed StopIteration there reads as a network blip.
    """
    owner = next((p for p in chat.get("participants") or []
                  if p.get("type") == "member" and p.get("role") == "owner"), None)
    if owner is None:
        raise RuntimeError(f"chat {chat.get('uid')} has no owner participant")
    # `_participant_identity` already answers "named, or still a bare handle?"
    # -- it hands back the handle itself when there is no meaningful name.
    handle = _one_line(owner.get("provider_key"))
    name = _participant_identity(owner)
    return (None if name == handle else name, handle)


_NEVER_GUESS = "Never guess a name from mail, calendar, or memory."


def _owner_fact(owner):
    """What an owner turn is told about its own owner.

    A roster block reaches the model on an inbound burst and nowhere else, so
    the owner's own DM -- the room onboarding actually happens in -- and every
    goal wake have no source at all for who their owner is. _NAME_FACT does not
    reach them either: it is gated on there being a roster to read. This is
    that source, and when the name is still missing it carries the ask, with
    the handle already filled in so there is nothing left to guess.
    """
    name, handle = owner
    if name:
        return f"Your owner is {name} [{handle}]."
    return (f"Your owner [{handle}] has not given their name yet: ask once and record it with "
            f"plow_name_contact(handle={handle}). {_NEVER_GUESS}")


def _provider(chat):
    # Which line this chat is, off its own agent participant.
    provider_type = _self_agent_line(chat).get("provider_type")
    if provider_type is None:
        raise RuntimeError(f"chat {chat.get('uid')} has no provider_type")
    return provider_type


def _split(listing, provider):
    """The chats this platform serves, and the uids on the same grant it does
    not: plow fans a frame to both platforms, so one for the other's chat is
    neither unknown (no reach refresh) nor outside the grant (no warning)."""
    served = {chat["uid"]: chat for chat in listing if _provider(chat) == provider}
    return served, frozenset(chat["uid"] for chat in listing) - served.keys()


# Hermes' own diagnostics reach an adapter through plain send() carrying no
# metadata that tells them apart from the model's prose, so they are
# recognised by the text they open with. They are the runtime talking about
# itself, never the turn's answer, so withholding one can never withhold the
# message the owner wanted.
BACKGROUND_REVIEW_PREFIX = "💾 Self-improvement review:"
_WORKING_PREFIX = "⏳ Working —"
# TODO(remove): once the fleet image pin includes srosro/hermes-agent's
# turn-stop-status PR, turn-stop text arrives as status frames and this
# final-response shim is dead code.
_NO_REPLY_PREFIX = "⚠️ No reply: "
_DIAGNOSTIC_PREFIXES = (BACKGROUND_REVIEW_PREFIX, _WORKING_PREFIX, _NO_REPLY_PREFIX)

# The open turn, as every tool handler and send guard reads it. Both
# platforms set it in on_processing_start and clear it in
# on_processing_complete; it lives here so both can.
_ACTIVE_TURN = contextvars.ContextVar("plow_chat_active_turn", default=None)


# A question the turn BLOCKS on: the one mid-turn send that is not working-out,
# because withholding it hangs the turn awaiting an answer nobody was asked for.
# `is_approval_prompt` is the gateway's own marker (`run_turn_runner.py:1374`,
# `slash_commands.py:271`); `clarify_id` is stamped by each platform's
# `send_clarify`, since base's fallback forwards only the turn's thread metadata
# (`base.py:2566`). Metadata only, never text: the wording of a question is the
# model's to write, and matching on it would hand the model this gate.
_ASKS_THE_ROOM = ("clarify_id", "is_approval_prompt")


def _is_chatter(turn, chat_id, metadata):
    """Is this outbound text the model working out loud, or the turn's answer?

    The turn boundary is the classifier: prose the model writes while a turn
    is open, into that turn's own chat, is its working-out. Hermes marks the
    turn-final reply `notify` -- the key telegram, discord, mattermost and a2a
    already read for the same distinction -- and the scheduler marks a cron
    delivery `job_id`. Everything an adapter itself sends (the greeting, a goal
    notice, the send_message tool) runs turn-less or cross-chat, so it falls
    out as not-chatter without needing to say so. Both platforms read an
    outbound message this way; what they do with the verdict is theirs.
    """
    meta = metadata or {}
    return (turn is not None and chat_id == turn["chat_uid"]
            and not meta.get("notify") and "job_id" not in meta
            and not any(meta.get(key) for key in _ASKS_THE_ROOM))
