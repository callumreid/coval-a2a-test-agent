"""A2A v2 JSON-RPC test agent.

Mocks the minimum surface of ServiceNow's A2A v2 chat protocol so Coval's
MODEL_TYPE_CHAT_A2A simulator can run end-to-end without depending on a real
ServiceNow instance. See SIM-362 and the a2a-chat-simulator design doc.
"""

import asyncio
import logging
import os
import time
from collections import deque
from uuid import uuid4

from fastapi import FastAPI, Form, HTTPException, Request

from coval_tracing import (
    buffered_span_count,
    flush_for_simulation,
    init_tracing,
    start_span,
)

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Coval A2A Test Agent")
init_tracing()

STATIC_TOKEN = "test-token-abc123"

# contextId -> turn count. In-memory only; Fly machine restart resets state.
TURN_STATE: dict[str, int] = {}

# access_token -> simulation_output_id (captured during the per-call OAuth init
# Coval performs via initialization_endpoint+initialization_payload). Stays
# in-memory and is the correlation path used when Coval CHAT_A2A doesn't
# honour the agent's custom_headers template.
TOKEN_TO_SIM: dict[str, str] = {}
# contextId -> simulation_output_id, set on turn 1 from the bearer token
# lookup so later turns of the same conversation keep flushing with the same
# simulation_id even though Coval re-uses the same connection.
CONTEXT_TO_SIM: dict[str, str] = {}

# Pre-registered simulation_output_ids waiting to be claimed by the next
# new conversation. Used as the PSTN-style fallback because CHAT_A2A's
# Coval simulator does not honour `metadata.custom_headers` /
# `initialization_payload` template substitution. A small launcher script
# posts /v1/runs, polls /v1/simulations for the new sim_id, and pushes it
# here via POST /register-simulation before the chat session starts.
SIM_QUEUE: deque[tuple[float, str]] = deque()
SIM_QUEUE_MAX = 32
SIM_QUEUE_TTL_SECONDS = 120
# Shared-secret header for /register-simulation. Re-uses COVAL_API_KEY at
# startup so customers don't have to add a second secret.
REGISTRATION_SECRET = os.environ.get("COVAL_API_KEY", "")

# One canned reply per turn. The final entry flips status.state to "completed"
# so the simulator's end-state detector fires and the conversation terminates.
REPLIES: list[tuple[str, str]] = [
    ("Hello! How can I help you today?", "working"),
    ("Got it, let me check on that for you.", "working"),
    ("I can see there is a reported outage in your area affecting roughly 200 customers. Our engineers are aware and actively working on it.", "working"),
    ("Current ETA for restoration is within the next two hours. Would you also like me to sign you up for SMS notifications when the outage is resolved?", "working"),
    ("Done — I've enrolled your number for SMS updates. You'll get a text as soon as service is restored.", "working"),
    ("All done! Is there anything else I can help you with today?", "completed"),
]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _purge_stale_sims() -> None:
    cutoff = time.time() - SIM_QUEUE_TTL_SECONDS
    while SIM_QUEUE and SIM_QUEUE[0][0] < cutoff:
        SIM_QUEUE.popleft()


@app.post("/register-simulation")
async def register_simulation(req: Request) -> dict[str, object]:
    if not REGISTRATION_SECRET:
        raise HTTPException(status_code=503, detail="COVAL_API_KEY not configured")
    if req.headers.get("x-internal-key", "") != REGISTRATION_SECRET:
        raise HTTPException(status_code=401, detail="invalid x-internal-key")
    body = await req.json()
    sim_id = (body or {}).get("simulation_output_id")
    if not isinstance(sim_id, str) or not sim_id:
        raise HTTPException(status_code=400, detail="simulation_output_id required")
    _purge_stale_sims()
    while len(SIM_QUEUE) >= SIM_QUEUE_MAX:
        SIM_QUEUE.popleft()
    SIM_QUEUE.append((time.time(), sim_id))
    logging.getLogger("coval.tracing").info(
        "registered simulation_output_id=%s queue_depth=%d", sim_id, len(SIM_QUEUE)
    )
    return {"status": "ok", "queue_depth": len(SIM_QUEUE)}


@app.post("/oauth_token.do")
async def oauth_token(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
) -> dict[str, object]:
    t0 = time.time()
    logging.getLogger("coval.tracing").info(
        "oauth_token.do start client_id=%r queue_depth=%d", client_id, len(SIM_QUEUE)
    )
    # Path A: if Coval ever substitutes `{{simulation_output_id}}` into the
    # initialization_payload's client_id for CHAT_A2A, mint a per-call bearer
    # token now and remember the mapping. As of 2026-05-25 the simulator
    # passes the literal `{{simulation_output_id}}` here, so this branch is
    # dormant but ready for the day it activates.
    looks_like_sim_id = (
        len(client_id) == 22 and client_id.replace("-", "").replace("_", "").isalnum()
    )
    if looks_like_sim_id:
        token = f"sim-{uuid4().hex}"
        TOKEN_TO_SIM[token] = client_id
        logging.getLogger("coval.tracing").info(
            "minted per-call bearer token for simulation_output_id=%s", client_id
        )
        return {
            "access_token": token,
            "expires_in": 1800,
            "token_type": "Bearer",
        }

    # Path B: wait briefly for the launcher (`scripts/launch_and_register.py`)
    # to POST a freshly-polled simulation_output_id to /register-simulation.
    # Coval makes the initialization request before the first chat turn, so
    # blocking here is the safest natural sync point. Keep the wait short so
    # ordinary smoke tests without a launcher fall through quickly.
    wait_deadline = time.time() + 45.0
    while time.time() < wait_deadline:
        _purge_stale_sims()
        if SIM_QUEUE:
            _ts, sim_id = SIM_QUEUE.popleft()
            token = f"sim-{uuid4().hex}"
            TOKEN_TO_SIM[token] = sim_id
            logging.getLogger("coval.tracing").info(
                "minted per-call bearer token from FIFO for simulation_output_id=%s",
                sim_id,
            )
            return {
                "access_token": token,
                "expires_in": 1800,
                "token_type": "Bearer",
            }
        await asyncio.sleep(0.1)

    logging.getLogger("coval.tracing").info(
        "oauth_token.do fallback after wait=%.2fs queue_depth=%d",
        time.time() - t0,
        len(SIM_QUEUE),
    )
    return {
        "access_token": STATIC_TOKEN,
        "expires_in": 1800,
        "token_type": "Bearer",
    }


def _user_text(message: dict) -> str:
    parts = message.get("parts") or []
    out = []
    for p in parts:
        if isinstance(p, dict) and p.get("kind") == "text":
            t = p.get("text")
            if isinstance(t, str):
                out.append(t)
    return " ".join(out)


@app.post("/api/sn_aia/a2a/v2/agent/id/{agent_id}")
async def a2a_message(agent_id: str, req: Request) -> dict[str, object]:
    auth_hdr = req.headers.get("authorization", "")
    bearer = auth_hdr[7:] if auth_hdr[:7].lower() == "bearer " else ""
    if bearer != STATIC_TOKEN and bearer not in TOKEN_TO_SIM:
        raise HTTPException(status_code=401, detail="invalid bearer token")

    # Correlation lookup order:
    # 1. X-Simulation-ID header (works if Coval ever honours custom_headers for
    #    CHAT_A2A; today it does not).
    # 2. The per-call bearer token we minted in /oauth_token.do when Coval ran
    #    its initialization handshake with `client_id={{simulation_output_id}}`
    #    (CHAT_A2A does not substitute the template, so today this path is
    #    inactive but kept for the day it does).
    # 3. FIFO claim from SIM_QUEUE — populated by a launcher script that polls
    #    `GET /v1/simulations?filter=run_id=...` immediately after starting the
    #    run and POSTs each new simulation_output_id to /register-simulation.
    simulation_id = (
        req.headers.get("x-simulation-id")
        or req.headers.get("x-simulation-output-id")
        or TOKEN_TO_SIM.get(bearer, "")
    )

    body = await req.json()
    params = body.get("params", {}) or {}
    message = params.get("message", {}) or {}

    # Simulator may set contextId/taskId at either params.* or params.message.*.
    # Prefer params.* since that's what a2a_chat_transport injects first.
    context_id = params.get("contextId") or message.get("contextId") or ""
    task_id = params.get("taskId") or message.get("taskId") or ""

    # Turn 1: mint a new conversation.
    new_conversation = not context_id
    if new_conversation:
        context_id = f"ctx-{uuid4().hex[:8]}"
        task_id = f"task-{uuid4().hex[:8]}"

    turn = TURN_STATE.get(context_id, 0) + 1
    TURN_STATE[context_id] = turn

    # If no upstream source produced one, claim from the FIFO on turn 1.
    if not simulation_id and new_conversation:
        _purge_stale_sims()
        if SIM_QUEUE:
            _ts, simulation_id = SIM_QUEUE.popleft()
            logging.getLogger("coval.tracing").info(
                "claimed FIFO simulation_output_id=%s for context=%s",
                simulation_id,
                context_id,
            )

    # Remember the simulation_id for this context_id so that subsequent turns
    # of the same conversation can still flush with the correct ID even when
    # the bearer token is rotated or absent.
    if simulation_id:
        CONTEXT_TO_SIM[context_id] = simulation_id
    elif context_id in CONTEXT_TO_SIM:
        simulation_id = CONTEXT_TO_SIM[context_id]

    text, state = REPLIES[min(turn, len(REPLIES)) - 1]
    user_text = _user_text(message)

    turn_attrs: dict[str, object] = {
        "agent.id.path": agent_id,
        "a2a.method": body.get("method") or "message/send",
        "a2a.rpc.id": str(body.get("id") or ""),
        "turn.index": turn,
        "turn.is_first": new_conversation,
        "turn.is_final": state == "completed",
        "a2a.context_id": context_id,
        "a2a.task_id": task_id,
        "user.message.length_chars": len(user_text),
        "agent.message.length_chars": len(text),
        "a2a.state": state,
        "coval.simulation_id_present": bool(simulation_id),
        "transport.kind": "http",
    }

    # On turn 1 we also open a conversation root span so the trace shows a
    # full session band. SimpleSpanProcessor will surface both to the buffer.
    if new_conversation:
        with start_span(
            "conversation",
            context_id=context_id,
            attributes={
                "agent.id.path": agent_id,
                "a2a.context_id": context_id,
                "a2a.task_id": task_id,
                "conversation.started": 1,
            },
        ) as conv_span:
            conv_span.add_event("conversation.opened")

    with start_span(
        "turn",
        context_id=context_id,
        attributes=turn_attrs,
    ) as turn_span:
        # The "LLM" here is a 6-entry canned reply table, not a real provider
        # call. Per the trace span-schema guidance for hosted/marker spans we
        # tag it as a metadata marker so customers do not compute false LLM
        # latency or TTFB metrics from it. The numeric reply.length_chars
        # attribute is still safe to aggregate.
        with start_span(
            "llm",
            context_id=context_id,
            attributes={
                "llm.kind": "canned",
                "llm.reply.index": min(turn, len(REPLIES)) - 1,
                "llm.reply.length_chars": len(text),
                "llm.finish_reason": "stop",
                "trace.source": "a2a_canned_reply",
                "trace.timing": "metadata_marker",
                "trace.duration_note": (
                    "duration is dict-lookup time only; do not use for provider TTFB"
                ),
            },
        ):
            pass

        if state == "completed":
            turn_span.add_event(
                "conversation.completed",
                attributes={"turn.final_index": turn},
            )
            turn_span.set_attribute("workflow.completed", 1)
        else:
            turn_span.set_attribute("workflow.completed", 0)

    # One conversation-summary span per simulation, emitted only on the
    # terminal turn. This gives trace metrics that aggregate over
    # `conversation` (workflow.completed average, turn-count average) one
    # well-formed row per simulation instead of mixing per-turn rows.
    if state == "completed":
        with start_span(
            "conversation",
            context_id=context_id,
            attributes={
                "conversation.phase": "end",
                "conversation.total_turns": turn,
                "conversation.terminal_state": state,
                "workflow.completed": 1,
                "agent.id.path": agent_id,
                "a2a.context_id": context_id,
                "a2a.task_id": task_id,
            },
        ) as end_span:
            end_span.add_event("conversation.summary")

    # If Coval gave us a simulation_id, flush this conversation's spans now.
    exported = 0
    if simulation_id:
        exported = flush_for_simulation(context_id, simulation_id)
    pending = buffered_span_count(context_id)
    logging.getLogger("coval.tracing").info(
        "turn=%s state=%s context=%s simulation_id_present=%s exported=%d pending=%d",
        turn,
        state,
        context_id,
        bool(simulation_id),
        exported,
        pending,
    )

    return {
        "jsonrpc": "2.0",
        "id": body.get("id"),
        "result": {
            "contextId": context_id,
            "id": task_id,
            "artifacts": [{"parts": [{"kind": "text", "text": text}]}],
            "status": {"state": state},
        },
    }
