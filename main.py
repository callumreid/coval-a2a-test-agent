"""A2A v2 JSON-RPC test agent.

Mocks the minimum surface of ServiceNow's A2A v2 chat protocol so Coval's
MODEL_TYPE_CHAT_A2A simulator can run end-to-end without depending on a real
ServiceNow instance. See SIM-362 and the a2a-chat-simulator design doc.
"""

from uuid import uuid4

from fastapi import FastAPI, Form, HTTPException, Request

app = FastAPI(title="Coval A2A Test Agent")

STATIC_TOKEN = "test-token-abc123"

# contextId -> turn count. In-memory only; Fly machine restart resets state.
TURN_STATE: dict[str, int] = {}

# One canned reply per turn. Turn 3 flips status.state to "completed" so the
# simulator's end-state detector fires and the conversation terminates.
REPLIES: list[tuple[str, str]] = [
    ("Hello! How can I help you today?", "working"),
    ("Got it, let me check on that for you.", "working"),
    ("All done — is there anything else I can help with?", "completed"),
]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/oauth_token.do")
async def oauth_token(
    grant_type: str = Form(...),
    client_id: str = Form(...),
    client_secret: str = Form(...),
) -> dict[str, object]:
    return {
        "access_token": STATIC_TOKEN,
        "expires_in": 1800,
        "token_type": "Bearer",
    }


@app.post("/api/sn_aia/a2a/v2/agent/id/{agent_id}")
async def a2a_message(agent_id: str, req: Request) -> dict[str, object]:
    if req.headers.get("authorization", "") != f"Bearer {STATIC_TOKEN}":
        raise HTTPException(status_code=401, detail="invalid bearer token")

    body = await req.json()
    params = body.get("params", {}) or {}
    message = params.get("message", {}) or {}

    # Simulator may set contextId/taskId at either params.* or params.message.*.
    # Prefer params.* since that's what a2a_chat_transport injects first.
    context_id = params.get("contextId") or message.get("contextId") or ""
    task_id = params.get("taskId") or message.get("taskId") or ""

    # Turn 1: mint a new conversation.
    if not context_id:
        context_id = f"ctx-{uuid4().hex[:8]}"
        task_id = f"task-{uuid4().hex[:8]}"

    turn = TURN_STATE.get(context_id, 0) + 1
    TURN_STATE[context_id] = turn

    text, state = REPLIES[min(turn, len(REPLIES)) - 1]

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
