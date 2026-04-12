# Coval A2A Test Agent

Always-on mock of ServiceNow's A2A v2 JSON-RPC chat protocol. Lets
Coval's `MODEL_TYPE_CHAT_A2A` simulator run end-to-end without touching a real
ServiceNow instance. Deployed to Fly.io as `coval-a2a-test-agent`.

Tracked by [SIM-362](https://linear.app/coval/issue/SIM-362). Contract taken
from the [A2A Chat Simulator design doc](https://github.com/coval-ai/internal-engineering-docs/pull/53).

## Endpoints

### `GET /health`
Liveness probe. Returns `{"status": "ok"}`.

### `POST /oauth_token.do`
OAuth2 client-credentials token endpoint. Accepts any `client_id` / `client_secret`
and returns a static bearer token. The Coval simulator extracts `access_token`
from the response and sends it as `Authorization: Bearer ...` on subsequent
chat requests.

```
Content-Type: application/x-www-form-urlencoded
Body: grant_type=client_credentials&client_id=...&client_secret=...

Response:
{
  "access_token": "test-token-abc123",
  "expires_in": 1800,
  "token_type": "Bearer"
}
```

### `POST /api/sn_aia/a2a/v2/agent/id/{agent_id}`
JSON-RPC `message/send` receiver. Requires `Authorization: Bearer test-token-abc123`.

Turn tracking is keyed by `contextId`:
- **Turn 1:** mints a new `contextId` + `taskId`, replies `"Hello! How can I help you today?"`, `status.state = "working"`
- **Turn 2:** echoes the same `contextId` / `taskId`, replies `"Got it, let me check on that for you."`, `status.state = "working"`
- **Turn 3+:** replies `"All done..."` with `status.state = "completed"` — triggers the simulator's end-state detection

State is in-memory per Fly machine and resets on restart. That's fine for a
validation mock; each simulation run opens a fresh `contextId`.

## Local run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --port 8000 --reload
```

## Smoke test

```bash
# Health
curl -sS http://localhost:8000/health

# OAuth token
curl -sS -X POST http://localhost:8000/oauth_token.do \
  -d 'grant_type=client_credentials&client_id=x&client_secret=y'

# Turn 1 (no contextId)
curl -sS -X POST http://localhost:8000/api/sn_aia/a2a/v2/agent/id/test-agent-001 \
  -H 'Authorization: Bearer test-token-abc123' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"sim-1","method":"message/send","params":{"message":{"role":"user","kind":"message","messageId":"sim-1_1","parts":[{"kind":"text","text":"hi"}]}}}'

# Turn 2 (echo contextId/taskId from turn 1)
curl -sS -X POST http://localhost:8000/api/sn_aia/a2a/v2/agent/id/test-agent-001 \
  -H 'Authorization: Bearer test-token-abc123' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":"sim-1","method":"message/send","params":{"contextId":"<from turn 1>","taskId":"<from turn 1>","message":{"role":"user","kind":"message","messageId":"sim-1_2","parts":[{"kind":"text","text":"followup"}]}}}'

# Bad token → 401
curl -sS -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/api/sn_aia/a2a/v2/agent/id/test-agent-001 \
  -H 'Authorization: Bearer wrong' \
  -H 'Content-Type: application/json' -d '{}'
```

## Deploy

Fly app already configured in `fly.toml`. First deploy:

```bash
fly apps create coval-a2a-test-agent
fly deploy
```

Subsequent deploys:

```bash
fly deploy
```

Verify:

```bash
curl -sS https://coval-a2a-test-agent.fly.dev/health
```

## Coval registration (for SIM-366)

Register as a `MODEL_TYPE_CHAT_A2A` agent after backend PR #3486 is merged and
the agents lambda is deployed:

```json
{
  "model_type": "MODEL_TYPE_CHAT_A2A",
  "chat_endpoint": "https://coval-a2a-test-agent.fly.dev/api/sn_aia/a2a/v2/agent/id/test-agent-001",
  "initialization_endpoint": "https://coval-a2a-test-agent.fly.dev/oauth_token.do",
  "initialization_content_type": "application/x-www-form-urlencoded",
  "initialization_payload": "{\"grant_type\":\"client_credentials\",\"client_id\":\"test\",\"client_secret\":\"test\"}",
  "response_message_path": "result.artifacts.0.parts.0.text",
  "response_state_extraction": {"contextId": "result.contextId", "taskId": "result.id"},
  "end_state_path": "result.status.state",
  "end_state_values": ["completed", "failed"]
}
```
