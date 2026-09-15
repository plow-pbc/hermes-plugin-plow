"""The plow_email platform: the agent's own email line, as its own Hermes
platform on the chat adapter's transport (design §5).

Loaded through `test_adapter._load`, so the same `gateway.*` stubs and the
same package loader serve both platforms.
"""

from __future__ import annotations

import json
import logging
import pathlib
import sys
import types
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from test_adapter import (
    IDENTITY,
    _HTTP,
    _Resp,
    _SEND_ARGV,
    _attachment,
    _capture_events,
    _chat,
    _envelope,
    _live_tool,
    _load,
    _mark_anchored,
    _settle,
)

ADDRESS = "elm@plow.co"
OWNER = ("Sam", "sam@example.com")


def _mail_chat(uid: str, *, group: bool = False) -> dict[str, Any]:
    """A mail thread as the listing serves it (design §1): a phone-line chat
    but for its line and its addresses -- the line is the email line, the
    owner is on it, and any other address is a member."""
    chat = _chat(uid, name="Re: invoice", group=group, owner_name=OWNER[0])
    agent, owner, *others = chat["participants"]
    agent["line"] = {"uid": "ln_mail", "provider_key": ADDRESS, "display_name": "Elm",
                     "provider_type": "email"}
    owner["provider_key"] = OWNER[1]
    for other in others:
        other.update(display_name="Dana", provider_key="dana@example.com")
    return chat


def _load_email(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> tuple[Any, Any]:
    """The plugin, plus the registry entry `register` would have made for
    plow_email -- the object the gateway reads `platform_hint` from on every
    prompt build (agent/system_prompt.py `_platform_hint`)."""
    module = _load(monkeypatch, tmp_path)
    entry = SimpleNamespace(platform_hint=module.plow_email.hint())
    registry = types.ModuleType("gateway.platform_registry")
    registry.platform_registry = SimpleNamespace(get={"plow_email": entry}.__getitem__)
    monkeypatch.setitem(sys.modules, "gateway.platform_registry", registry)
    return module, entry


def _adapter(module: Any) -> Any:
    return module.plow_email.PlowEmailAdapter(SimpleNamespace(extra={}))


async def test_reach_keeps_the_email_line_and_publishes_its_address_in_the_hint(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    """The hint is static per platform and the address is per agent, so the
    entry is written in place the first time reach reveals a mail chat.
    Zero threads is the normal first state of a new line: reach holds
    empty, nothing is published, and the address-less hint stands -- no turn
    can arrive on this platform before a thread exists."""
    module, entry = _load_email(monkeypatch, tmp_path)
    assert entry.platform_hint == ("This is your own email line. Mail here is addressed to you; "
                                   "you write as yourself, at email length.")
    adapter = _adapter(module)
    adapter._set_reach([_chat("cht_a")])
    assert adapter._chats == {} and adapter._foreign == frozenset({"cht_a"})
    assert adapter.address is None
    assert entry.platform_hint.startswith("This is your own email line. ")

    adapter._set_reach([_chat("cht_a"), _mail_chat("cht_m"), _mail_chat("cht_n", group=True)])
    assert set(adapter._chats) == {"cht_m", "cht_n"} and adapter._foreign == frozenset({"cht_a"})
    assert adapter.address == ADDRESS
    assert entry.platform_hint == (f"This is your own email line, {ADDRESS}. Mail here is addressed "
                                   "to you; you write as yourself, at email length.")
    assert [(uid, (await adapter.get_chat_info(uid))["type"]) for uid in adapter._chats] == [
        ("cht_m", "dm"), ("cht_n", "group")]


@pytest.mark.parametrize(
    ("group", "chat_type", "role", "body", "attachments", "expected_text"),
    [
        pytest.param(False, "dm", "owner", "Can you send the invoice?", None,
                     "Can you send the invoice?", id="owner-dm"),
        pytest.param(True, "group", "member", "Can you send the invoice?", None,
                     "Can you send the invoice?", id="member-group"),
        pytest.param(False, "dm", "owner", "", [_attachment()],
                     "(email with 1 attachment(s); attachments are not delivered on this line yet)",
                     id="attachment-only"),
    ],
)
async def test_a_mail_thread_is_plow_emails_turn_and_never_plow_chats(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture, group: bool, chat_type: str, role: str,
    body: str, attachments: list[dict[str, Any]] | None, expected_text: str,
) -> None:
    """Both adapters hold the same grant and see the same frames. A mail
    frame is a plow_email turn -- platform, chat_type and chat_id are the
    three fields upstream's build_session_key (gateway/session.py:641) joins
    into `<ns>:plow_email:<chat_type>:<chat_uid>` -- and the phone line's
    frame is not this platform's. The prompt is the owner fact, plus the
    roster on an owner's own turn; no trust prose; the hint rides the
    platform entry. On a member's mail it is still the line's owner who is
    named, and only an
    owner's mail carries owner authority. An attachment-only mail is not
    silently "(empty email)": the placeholder names the count and one line is
    logged. A thread whose roster has no owner at all is the one shape that
    cannot be rendered: it must name itself on the way out, because the
    frame is already deduped and the mail is gone."""
    caplog.set_level(logging.INFO)
    module, _entry = _load_email(monkeypatch, tmp_path)
    listing = [_chat("cht_a"), _mail_chat("cht_m", group=group)]
    chat = module.PlowChatAdapter(SimpleNamespace(extra={}))
    chat._set_reach(listing)
    _mark_anchored(chat, "cht_a")
    mail = _adapter(module)
    mail._set_reach(listing)
    mail._identity = IDENTITY
    chat_events, mail_events = _capture_events(monkeypatch, chat), _capture_events(monkeypatch, mail)

    frame = _envelope("evt_1", "cht_m", "msg_1", body=body, attachments=attachments, role=role)
    await chat._on_frame(frame, None)
    await _settle(chat)
    await mail._on_frame(frame, None)
    await mail._on_frame(frame, None)                       # a redelivered event is one turn
    await mail._on_frame(_envelope("evt_2", "cht_a", "msg_2"), None)

    assert chat_events == [], "the email line's turn is never the phone line's"
    [event] = mail_events
    source = event["source"]
    assert (source.platform, source.chat_type, source.chat_id) == ("plow_email", chat_type, "cht_m")
    assert source.role_authorized is (role == "owner") and source.user_id == f"mem_{role}_cht_m"
    assert event["text"] == expected_text and event["message_id"] == "msg_1"
    roster = module._lines_fact(IDENTITY)
    assert "that is you" in roster, "the mail line's own persona is marked"
    assert event["channel_prompt"] == (f"{module._owner_fact(OWNER)} {roster}" if role == "owner"
                                        else module._owner_fact(OWNER))
    if attachments:
        assert "cht_m: attachment-only mail (1 attachment(s))" in caplog.text

    ownerless = _mail_chat("cht_x")
    del ownerless["participants"][1:]        # the email line stays; the owner is gone
    mail._set_reach([ownerless])
    with pytest.raises(RuntimeError, match="cht_x has no owner participant"):
        await mail._on_frame(_envelope("evt_3", "cht_x", "msg_3"), None)
    # `_serve` logs the TYPE only -- an aiohttp handshake error stringifies a
    # live ticket -- so an unnamed raise reads exactly like a network blip.
    assert "cht_x has no owner participant" in caplog.text


async def test_an_unknown_thread_is_delivered_even_when_the_roster_read_is_down(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    """The refresh an unknown thread triggers reads the grant alone. The roster
    is read once per connect, so a /v1/lines outage cannot cost the thread its
    first email (the mail line has no backfill)."""
    module, _entry = _load_email(monkeypatch, tmp_path)
    mail = _adapter(module)
    mail._set_reach([_chat("cht_a")])                    # cht_m is not known yet
    events = _capture_events(monkeypatch, mail)

    class _GrantOnlyHTTP:
        def get(self, url: str, **kwargs: Any) -> _Resp:
            if url.endswith("/v1/lines"):
                return _Resp({}, status=503)
            return _Resp({"object": "list", "data": [_chat("cht_a"), _mail_chat("cht_m")], "has_more": False})

    await mail._on_frame(_envelope("evt_1", "cht_m", "msg_1"), _GrantOnlyHTTP())

    [event] = events
    assert event["source"].chat_id == "cht_m" and event["message_id"] == "msg_1"


@pytest.mark.parametrize("role", ["owner", "member"])
async def test_an_email_turn_confines_the_chat_tools_and_never_sends_from_the_owners_gmail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, role: str,
) -> None:
    """The tools and the Latch mail gate read one turn slot. A member's email
    turn is refused the contact book like a member's chat turn, and may not
    rename the owner -- plow_name_contact is shared with the chat platform, so
    the owner_handle stamp off this thread's roster has to hold here too, case
    variants included; and on ANY email turn a plow-gog send is blocked -- the
    reply goes out from this line, which is the ghostwriting bug this design
    exists to end."""
    module, _entry = _load_email(monkeypatch, tmp_path)
    mail = _adapter(module)
    mail._set_reach([_mail_chat("cht_m")])
    record: list[Any] = []
    _live_tool(module, monkeypatch, "name_contact",
               result={"display_name": "Sam", "relationship": None}, record=record)
    event = SimpleNamespace(source=SimpleNamespace(chat_id="cht_m", chat_type="dm",
                                                   role_authorized=role == "owner"))
    await mail.on_processing_start(event)
    owner = role == "owner"
    assert module._ACTIVE_TURN.get() == {"chat_uid": "cht_m", "owner": owner, "dm": False,
                                         "authority": owner, "email": True, "owner_handle": OWNER[1]}
    contacts = json.loads(module._plow_contacts({}))
    assert contacts["success"] is False
    assert ("without the owner's authority" in contacts["error"]) == (role == "member")
    rename = json.loads(module._plow_name_contact({"handle": OWNER[1].upper(), "display_name": "Sam"}))
    assert rename["success"] is owner
    assert record == ([(OWNER[1].upper(), {"display_name": "Sam"})] if owner else [])
    gate = module._pre_tool_call("mcp__latch__plow_run_command", {"argv": _SEND_ARGV}, session_id="s1")
    assert gate["action"] == "block"
    await mail.on_processing_complete(event, None)
    assert module._ACTIVE_TURN.get() is None


@pytest.mark.parametrize(
    ("target", "turn", "metadata", "body", "posted", "success"),
    [
        pytest.param("cht_m", None, None, "Attached below.", True, True, id="turn-less"),
        pytest.param("cht_m", ("cht_m", True), {"notify": True}, "Attached below.", True, True, id="the-answer"),
        pytest.param("cht_m", None, {"job_id": "j1"}, "Weekly digest", True, True, id="cron"),
        pytest.param("cht_m", ("cht_m", True), None, "Looking that up now.", False, True, id="mid-turn-prose"),
        pytest.param("cht_m", None, {"notify": True}, "⏳ Working — still on it", False, True, id="diagnostic"),
        pytest.param("cht_m", ("cht_m", False), {"notify": True}, "Here it is.", True, True, id="member-reply"),
        pytest.param("cht_n", ("cht_m", False), {"notify": True}, "Here it is.", False, False, id="member-cross-thread"),
        pytest.param("cht_a", None, {"notify": True}, "Hi", False, False, id="not-an-email-thread"),
        pytest.param("cht_m", None, {"notify": True}, "Attached below.", True, False, id="plow-refused-it"),
    ],
)
async def test_a_reply_goes_to_the_chat_send_endpoint_and_only_the_answer_goes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
    target: str, turn: tuple[str, bool] | None, metadata: dict[str, Any] | None,
    body: str, posted: bool, success: bool,
) -> None:
    """Every send here is an email, so only the turn's answer (`notify`), a
    cron delivery (`job_id`) or a turn-less send goes out -- the model's
    working-out and Hermes' own diagnostics never do, and there is no verbose
    carve-out. The endpoint is the chat send; plow dispatches on the chat's
    provider (design §3). A member's turn is confined to its own thread, and
    a send plow refuses fails loudly rather than reading as delivered."""
    module, _entry = _load_email(monkeypatch, tmp_path)
    mail = _adapter(module)
    mail._set_reach([_chat("cht_a"), _mail_chat("cht_m"), _mail_chat("cht_n")])
    http = _HTTP(status=400 if posted and not success else 200)
    monkeypatch.setattr(module.plow_email.aiohttp, "ClientSession", lambda *a, **k: http)
    if turn:
        module._ACTIVE_TURN.set({"chat_uid": turn[0], "owner": turn[1], "dm": False,
                                 "authority": turn[1], "email": True})

    result = await mail.send(target, body, metadata=metadata)

    assert result.success is success
    assert http.posts == ([(f"{module.BASE}/v1/chats/{target}/messages", {"body": body})] if posted else [])
    assert result.message_id == ("msg_sent" if posted and success else None)
    assert success or result.error.startswith("Plow Email")


async def test_the_email_line_names_its_own_terminal_stop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    """`_serve` is shared with the phone line, so the stop is reported the same
    way -- but the message an operator reads must name the line that died."""
    module, _entry = _load_email(monkeypatch, tmp_path)
    mail = _adapter(module)
    mail._credential_refused()
    assert mail._fatal_error_code == "credential_refused"
    assert mail._fatal_error_retryable is False
    assert mail._fatal_error_message.startswith("Plow Email")


def test_register_declares_both_platforms_on_one_transport(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    """One plugin, two registry entries: the identity split -- name, label,
    hint, session namespace -- lives there, not in the directory layout
    (design §4). The email line declares no cron home: it has no standing
    thread for a delivery to land in."""
    module = _load(monkeypatch, tmp_path)
    ctx = mock.Mock()
    module.register(ctx)
    entries = {call.kwargs["name"]: call.kwargs for call in ctx.register_platform.call_args_list}
    assert list(entries) == ["plow_chat", "plow_email"]
    email = entries["plow_email"]
    assert email["label"] == "Plow Email"
    assert email["platform_hint"] == module.plow_email.hint()
    assert "cron_deliver_env_var" not in email
    assert email["check_fn"]()
    assert isinstance(email["adapter_factory"](SimpleNamespace(extra={})), module.plow_email.PlowEmailAdapter)
