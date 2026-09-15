---
name: google-workspace
description: "Gmail and Google Calendar through the owner's Mac."
version: 2.5.0
---

# Google Workspace — through the owner's Mac

This agent holds no Google credential. There is no local OAuth token
(no `google_token.json` exists in this home), and local Google OAuth
scripts from older copies of this skill do not work here. Never run
them and never start a local Google OAuth setup flow.

Gmail and Google Calendar are reached through the owner's Mac, over
the Plow relay MCP server — the configured server whose tools start
with `plow_` (its name varies by install: `plow` here, `latch` on
Mac-managed instances):

1. Call `plow_list_skills`. If it lists `google-workspace`, read it
   with `plow_read_skill` and follow it exactly. That skill is the
   only source for the command and its arguments — do not carry a
   spelling from memory or from this file. The Mac mints its own
   short-lived Google token; you never see or need one.
2. A new email you send as yourself is not `gmail send` at all: use
   `plow_send_message` with the address in `to` and a `subject`, and it
   leaves from your own mailbox with your owner copied, under the same
   authority a text needs and with no approval card. `gmail send` below
   is only for mail your owner wants out of their own account.
3. Sending an email from your owner's account needs a turn with the
   owner's authority — the owner anywhere, or a human in a group the
   owner trusts. On such a turn the
   gateway posts the command into that same room and waits for
   `/approve`, which anyone there may answer. Compose the whole message
   — recipients, subject, body — in the one `gmail send` command; that
   command is all the room sees. A draft sent by id is refused, because
   the room would see only the id. A refusal there means the room
   declined; nothing was sent. From a turn without the owner's authority
   a send is blocked outright and nothing is sent — ask the owner, or a
   human in a trusted group, to make the request. Any other command may
   show the owner an approval card on their Mac; if it hangs, it is
   waiting there, and a refusal there is a denial on the Mac.
4. Calendar conflicts are yours to judge, not something a chat approves.
   A calendar create that overlaps an existing commitment is refused,
   never queued for approval: the check covers every connected account,
   and the refusal comes back to you. To book anyway, re-send the same
   create — the same command, same attendees, same calendar, same
   everything — with `--confirm-conflict` added, and only when a turn
   with the owner's authority fixed the time in the request ("book it
   regardless", "hold those exact dates", a named slot insisted on).
   Never rebuild a smaller create to get past the refusal; you would
   drop what you left out. Otherwise tell the room what the slot
   overlaps and ask. When you do book over one, say so in the reply and
   name the overlap. Whether you are asking about a conflict or
   reporting one you booked over, in a shared room the overlap is "an
   existing commitment" and never the other event's name. Only a turn
   with the owner's authority can fix a time, so from any other turn the
   override is blocked and nothing is booked — ask the owner, or a human
   in a trusted group, to confirm.
5. If a connected MCP server lists no `google-workspace` skill, Google is
   not available to this agent. Say exactly that — do not fall back to
   local OAuth. If no MCP server with `plow_*` tools is connected at all,
   that is not the same thing: Latch is unreachable, so say the owner's Mac
   has to be awake with Latch running. Do not fall back to local OAuth in
   either case.
