---
name: plow-invite
description: When someone who is not your owner shows genuine interest in Plow or asks how to get an agent of their own, offer them one.
version: 1.5.0
---

# Plow invite: delight-triggered referral

<!-- The only hand-edited copy: plow-pbc/hermes-plugin-plow
     seed-skills/growth/plow-invite. The base image stages this file out of
     this repo's tarball at the plugin SHA it pins, so there is nothing to copy
     by hand. Configuration comes only from the environment (PLOW_API_BASE /
     PLOW_AGENT_TOKEN) and every path is relative to this skill's own
     directory, never an absolute home. -->

## When to act

Someone who is NOT your owner shows genuine, unprompted interest in Plow or in
what you just did, or asks how to get an agent like you for themselves. In a
group, only on a turn that is yours to answer -- judge that as your speak rule
says; an unaddressed remark in a group is not an opening, however warm. You are looking for real,
spontaneous interest, not only effusive praise: plain mild enthusiasm counts
too. Real examples of the bar: "Well done Plow!" · "Ah, love the plow text
interaction" · "oh, that is so cool" · "how do I get one of these?"

Do NOT act on: sarcasm or ambiguous praise; praise you solicited ("do you like
it?"); anything from your owner (answer them from the signup line in your
prompt instead); anyone you have already invited (check your memory first).

## What to do

Call `plow_offer_invite`. It takes no arguments: the tool binds the current
turn to a durable server record, and everything else is the server's: the
person, the message, the phrase, the number, the owner's consent.

Read its result:

- `skipped: consent_declined` or `skipped: no_invite_opportunity`: reply
  naturally to what they said, with no invite.
- `question_id`: the owner is being asked for consent. Reply naturally and do
  not mention an invite; the server resumes the invite when the owner answers.
  Never leave the person with an empty response while you wait -- on a turn
  that is yours to answer.
- `skipped: deferred_consent_unavailable`: this host cannot ask the owner.
  Reply naturally, with no invite.
- `invite_status`: the invite went out in this thread, written by Plow. Do
  not echo it, restate the phrase, or add a number of your own. Do not
  announce that you sent it or tell them to check their messages. It is
  already here in this thread, so a follow-up like "just sent you an invite,
  check your messages" only confuses them.

The owner's standing answer is recorded by
`python3 <this skill's dir>/scripts/mint_invite.py --consent granted` or
`--consent declined`; run it only when the owner tells you their answer in
their own thread. The install root differs by runtime, so never assume an
absolute path.

Never use cron, a scheduled job, or a generic cross-chat send for an invite.
The durable opportunity is the only continuation path.

## If the tool fails

Never loop on the tool within this turn, and never substitute the public
signup phrase or any other route -- the invite is the only path that carries
the referral, so an invite that failed is not an invitation to improvise one.

Read the receipt; it says which of three happened.

`delivery_unknown` -- the server could not confirm the outcome. The invite may
already have reached them, so say nothing in either thread and do NOT call
again: a second call would mint a second live invite for the same person.

`calling again ... is safe` -- the send definitively did not happen, whether it
never started or Plow reopened it. Say nothing now; you may call
`plow_offer_invite` again on a later qualifying turn.

`Plow declined (<status>)` -- terminal. Nothing was sent, and calling again
gets the same answer. Tell them briefly and plainly why, in your own words,
and leave it there.
