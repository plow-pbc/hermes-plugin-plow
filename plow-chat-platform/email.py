# Copyright 2026 The Plow Collective, Inc
# SPDX-License-Identifier: Apache-2.0
"""The agent's own email line as a Hermes platform.

A mail thread is a Plow chat whose line serves `provider_type: "email"`, on
the same grant as the phone line and over the same transport helpers (design
§1) -- but on this platform's own socket; this adapter serves those chats and
nothing else. Its identity -- platform name, session namespace, the static
hint -- is the registry entry `register` makes for it. Mail is addressed to
the agent, so there is no approval gate and no roster policy: the agent
writes as itself, from this line.
"""
import asyncio
import logging
import os

import aiohttp
from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, SendResult

from ._transport import (
    BASE,
    CREDENTIAL_REFUSED,
    _ACTIVE_TURN,
    _DIAGNOSTIC_PREFIXES,
    _bearer,
    _chat_type,
    _granted_chats,
    _is_chatter,
    _owner_fact,
    _owner_identity,
    _self_agent_line,
    _serve,
    _socket,
    _split,
    _ticket,
)

PLATFORM_NAME = "plow_email"
PROVIDER = "email"
log = logging.getLogger(__name__)


def hint(address=None):
    """The platform hint (design §5). Static per platform, so the address is
    filled in once reach has read it -- see `_publish_hint`."""
    line = f"your own email line, {address}" if address else "your own email line"
    return f"This is {line}. Mail here is addressed to you; you write as yourself, at email length."


def check_requirements():
    return bool(os.environ.get("PLOW_AGENT_TOKEN"))


class PlowEmailAdapter(BasePlatformAdapter):
    def __init__(self, config):
        super().__init__(config=config, platform=Platform(PLATFORM_NAME))
        config.extra["group_sessions_per_user"] = False
        config.typing_indicator = False  # the base's 2s typing loop is a no-op on email
        self.auth = _bearer()
        self.address = None                  # the line's address, off the first mail chat
        self._chats = {}                     # uid -> chat resource, mail only
        self._foreign = frozenset()          # the phone line's uids on the same grant
        self._seen_events = []
        self._ws_task = None

    @property
    def authorization_is_upstream(self):
        """Plow authenticated the sender and put them on the thread; Hermes
        must not pair on top. See PlowChatAdapter for the reasoning."""
        return True

    def _set_reach(self, listing):
        self._chats, self._foreign = _split(listing, PROVIDER)
        address = next((k for c in self._chats.values()
                        if (k := _self_agent_line(c).get("provider_key"))), None)
        if address and address != self.address:
            self.address = address
            self._publish_hint()

    async def _refresh_reach(self, http):
        self._set_reach(await _granted_chats(http, self.auth))

    def _publish_hint(self):
        """Writes the address onto the platform registry entry the gateway
        reads on every prompt build. Imported lazily -- `gateway.` is a
        runtime module the gateway supplies, not a dependency of this repo."""
        from gateway.platform_registry import platform_registry
        platform_registry.get(PLATFORM_NAME).platform_hint = hint(self.address)

    async def get_chat_info(self, chat_id):
        chat = self._chats[chat_id]
        return {"name": chat.get("display_name") or chat_id, "type": _chat_type(chat), "chat_id": chat_id}

    async def connect(self, *, is_reconnect=False):
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        async with aiohttp.ClientSession() as http:
            await self._refresh_reach(http)
        self._ws_task = asyncio.create_task(self._listen())
        return True

    async def disconnect(self):
        if self._ws_task:
            self._ws_task.cancel()
        self._mark_disconnected()

    async def _listen(self):
        first_connection = True

        async def session(http):
            nonlocal first_connection
            if not first_connection:
                await self._refresh_reach(http)
            first_connection = False
            async with _socket(http, await _ticket(http, self.auth)) as ws:
                self._mark_connected()
                log.info("[plow_email] websocket connected")
                async for frame in ws:
                    if frame.type == aiohttp.WSMsgType.TEXT:
                        await self._on_frame(frame.json(), http)

        await _serve(session, self._mark_disconnected, PLATFORM_NAME)
        self._set_fatal_error(*CREDENTIAL_REFUSED, retryable=False)
        # The gateway learns this line is dead only here -- see `_serve` for
        # why the notify belongs to the task that owns the loop.
        await self._notify_fatal_error()

    async def send_clarify(self, chat_id, question, choices, clarify_id, session_key, metadata=None):
        """Stamp the question so `_is_chatter` does not read it as prose. This
        line withholds chatter in EVERY thread, so see PlowChatAdapter for the
        reasoning -- the consequence here is simply unconditional."""
        return await super().send_clarify(
            chat_id=chat_id, question=question, choices=choices, clarify_id=clarify_id,
            session_key=session_key, metadata={**(metadata or {}), "clarify_id": clarify_id})

    async def _on_frame(self, frame, http):
        if frame.get("type") == "connected":
            return
        chat_uid = frame["chat_id"]
        if chat_uid not in self._chats and chat_uid not in self._foreign:
            await self._refresh_reach(http)  # a thread born since the last read
        if chat_uid in self._foreign:
            return                           # the phone line's room; plow_chat's turn
        if chat_uid not in self._chats:
            log.warning("[plow_email] dropped frame outside the grant: %s", chat_uid)
            return
        if frame["event_type"] != "message_received" or frame["event_id"] in self._seen_events:
            return
        # Recorded before _on_message, unlike the chat adapter -- there is no
        # backfill on this line, so a raise can't be replayed either way.
        self._seen_events.append(frame["event_id"])
        del self._seen_events[:-512]
        await self._on_message(frame["data"]["message"], chat_uid)

    async def _on_message(self, msg, chat_uid):
        if msg["direction"] != "inbound":
            return                           # the echo of our own send
        sender = msg["sender"]
        if sender["type"] != "member":
            log.info("[plow_email] ignored sender.type=%r", sender["type"])
            return
        chat = self._chats[chat_uid]
        info = await self.get_chat_info(chat_uid)
        try:
            channel_prompt = _owner_fact(_owner_identity(chat))
        except RuntimeError as exc:
            # `_serve` logs the exception type only, so log the message here --
            # this mail is already event-deduped and would otherwise vanish silently.
            log.error("[plow_email] %s", exc)
            raise
        body, count = msg["body"].strip(), len(msg["attachments"])
        if not body and count:
            log.info("[plow_email] %s: attachment-only mail (%d attachment(s))", chat_uid, count)
            body = f"(email with {count} attachment(s); attachments are not delivered on this line yet)"
        await self.handle_message(MessageEvent(
            text=body or "(empty email)",
            source=self.build_source(chat_id=chat_uid, chat_name=info["name"], chat_type=info["type"],
                                     user_id=sender["uid"],
                                     user_name=sender.get("display_name") or sender["uid"],
                                     role_authorized=sender.get("role") == "owner"),
            message_id=msg["uid"],
            channel_prompt=channel_prompt,
        ))

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        if chat_id not in self._chats:
            return SendResult(success=False, error=f"Plow Email {chat_id!r} is not one of this line's threads")
        turn, body = _ACTIVE_TURN.get(), content.strip()
        if turn is not None and not turn["owner"] and chat_id != turn["chat_uid"]:
            return SendResult(success=False, error=f"Plow Email member turn is confined to {turn['chat_uid']!r}")
        # Every send here is an email, so the classifier's verdict is final:
        # no verbose read and no owner-DM carve-out, unlike the phone line.
        diagnostic = body.startswith(_DIAGNOSTIC_PREFIXES)
        if diagnostic or _is_chatter(turn, chat_id, metadata):
            log.info("[plow_email] dropped %s for %s",
                     "diagnostic" if diagnostic else "mid-turn prose", chat_id)
            return SendResult(success=True)
        # The chat send endpoint: plow dispatches on the chat's own provider
        # (design §3), so an email leaves by the same door as a text.
        async with aiohttp.ClientSession() as http:
            async with http.post(f"{BASE}/v1/chats/{chat_id}/messages",
                                 json={"body": body}, headers=self.auth) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    return SendResult(success=False, error=f"Plow Email {resp.status}: {data}")
        return SendResult(success=True, message_id=data.get("uid"))

    async def on_processing_start(self, event):
        # An email turn has no room to trust, so its authority is the owner's
        # alone; `email` keeps the Latch mail gate shut -- a reply here goes out
        # from this line, never their Gmail.
        owner = bool(event.source.role_authorized)
        _ACTIVE_TURN.set({"chat_uid": event.source.chat_id, "owner": owner,
                          "dm": False, "authority": owner, "email": True})

    async def on_processing_complete(self, event, outcome):
        _ACTIVE_TURN.set(None)
