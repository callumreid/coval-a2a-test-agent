"""Coval OpenTelemetry tracing helper for the A2A test agent.

Buffers spans per-conversation until a Coval simulation_output_id arrives on
the inbound `X-Simulation-ID` header, then flushes them to Coval's trace
ingest with that header attached. Keeps main.py focused on JSON-RPC routing.

Correlation path: Coval injects `X-Simulation-ID` on every chat POST via the
agent's `metadata.custom_headers` ({{simulation_output_id}} template). This
file fails closed (drops spans, never blocks the chat reply) when the env
var COVAL_API_KEY is missing or the header is absent.

Endpoint: https://api.coval.dev/v1/traces
Auth header: x-api-key (env COVAL_API_KEY)
Target header: X-Simulation-Id

Only one canonical exporter is used: OTLPSpanExporter (HTTP/JSON via the
opentelemetry-exporter-otlp-proto-http package). No literal secrets in code.
"""

from __future__ import annotations

import atexit
import logging
import os
import threading
from contextlib import contextmanager
from typing import Iterator

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.trace import Span, SpanKind, Status, StatusCode

logger = logging.getLogger("coval.tracing")

COVAL_TRACES_ENDPOINT = "https://api.coval.dev/v1/traces"
COVAL_EXPORT_TIMEOUT_S = 30
SERVICE_NAME = "coval-a2a-test-agent"

# Max spans we will buffer per conversation before we drop. Bounded so a
# never-correlated context can't grow forever.
MAX_BUFFERED_SPANS_PER_CONTEXT = 256


class _BufferingExporter(SpanExporter):
    """Holds finished spans by `coval.context_id` until they are released.

    When `release(context_id, simulation_id)` is called we hand the buffered
    spans to a fresh per-call `OTLPSpanExporter` whose `X-Simulation-Id`
    header is set to `simulation_id`. Spans whose context never correlates are
    eventually evicted by the per-context cap.
    """

    def __init__(self, api_key: str | None) -> None:
        self._api_key = api_key
        self._lock = threading.Lock()
        self._buffers: dict[str, list[ReadableSpan]] = {}

    def export(self, spans):  # type: ignore[override]
        if not spans:
            return SpanExportResult.SUCCESS
        with self._lock:
            for span in spans:
                attrs = span.attributes or {}
                ctx_id = attrs.get("coval.context_id") or "_orphan"
                buf = self._buffers.setdefault(ctx_id, [])
                if len(buf) >= MAX_BUFFERED_SPANS_PER_CONTEXT:
                    continue
                buf.append(span)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        with self._lock:
            self._buffers.clear()

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # type: ignore[override]
        return True

    def release(self, context_id: str, simulation_id: str) -> int:
        """Flush any spans currently buffered for `context_id` with the given
        simulation_id. Safe to call repeatedly: each call pops only the spans
        that have accumulated since the previous flush, so chat handlers can
        invoke it after every turn without producing duplicate ingestion
        against Coval's append-only trace store.
        """
        if not self._api_key:
            logger.warning(
                "coval tracing: COVAL_API_KEY not set, dropping %d buffered spans for context=%s",
                len(self._buffers.get(context_id, [])),
                context_id,
            )
            with self._lock:
                self._buffers.pop(context_id, None)
            return 0
        with self._lock:
            spans = self._buffers.pop(context_id, [])
        if not spans:
            return 0
        exporter = OTLPSpanExporter(
            endpoint=COVAL_TRACES_ENDPOINT,
            headers={
                "x-api-key": self._api_key,
                "X-Simulation-Id": simulation_id,
            },
            timeout=COVAL_EXPORT_TIMEOUT_S,
        )
        try:
            result = exporter.export(spans)
        except Exception:
            logger.exception(
                "coval tracing: OTLP export failed simulation_id=%s span_count=%d",
                simulation_id,
                len(spans),
            )
            return 0
        finally:
            exporter.shutdown()
        if result == SpanExportResult.SUCCESS:
            logger.info(
                "coval tracing: activated OTLP export simulation_id=%s span_count=%d context_id=%s",
                simulation_id,
                len(spans),
                context_id,
            )
            return len(spans)
        logger.error(
            "coval tracing: OTLP export non-success result=%s simulation_id=%s span_count=%d",
            result,
            simulation_id,
            len(spans),
        )
        return 0


_tracer: trace.Tracer | None = None
_buffer: _BufferingExporter | None = None
_provider: TracerProvider | None = None


def init_tracing() -> None:
    """Idempotent initialization. Safe to call multiple times."""
    global _tracer, _buffer, _provider
    if _tracer is not None:
        return

    api_key = os.environ.get("COVAL_API_KEY")
    if not api_key:
        logger.warning(
            "coval tracing: COVAL_API_KEY env var not set; spans will be buffered then dropped"
        )

    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "service.namespace": "coval-test-agents",
            "agent.name": "a2a-test-agent",
            "agent.provider": "custom-fastapi",
            "coval.agent_type": "MODEL_TYPE_CHAT_A2A",
            "coval.correlation.method": "custom_header.X-Simulation-ID",
        }
    )
    provider = TracerProvider(resource=resource)

    buffer = _BufferingExporter(api_key=api_key)
    # SimpleSpanProcessor so finished spans land in the buffer immediately,
    # which lets `release()` see every span for the context, including the
    # turn span that just closed.
    provider.add_span_processor(SimpleSpanProcessor(buffer))

    trace.set_tracer_provider(provider)
    _provider = provider
    _buffer = buffer
    _tracer = trace.get_tracer("coval.a2a-test-agent")
    atexit.register(_shutdown)


def _shutdown() -> None:
    if _provider is not None:
        try:
            _provider.shutdown()
        except Exception:  # noqa: BLE001
            pass


def get_tracer() -> trace.Tracer:
    if _tracer is None:
        init_tracing()
    assert _tracer is not None
    return _tracer


@contextmanager
def start_span(
    name: str,
    *,
    context_id: str,
    kind: SpanKind = SpanKind.INTERNAL,
    attributes: dict[str, object] | None = None,
) -> Iterator[Span]:
    tracer = get_tracer()
    attrs: dict[str, object] = {"coval.context_id": context_id}
    if attributes:
        attrs.update(attributes)
    with tracer.start_as_current_span(name, kind=kind, attributes=attrs) as span:
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)[:200]))
            raise


def flush_for_simulation(context_id: str, simulation_id: str) -> int:
    """Release buffered spans for `context_id` with the given simulation ID.

    Returns the number of spans exported. Safe to call once per call; later
    spans for the same context are dropped to avoid duplicates on Coval's
    append-only trace store.
    """
    if _buffer is None:
        init_tracing()
    assert _buffer is not None
    return _buffer.release(context_id, simulation_id)


def buffered_span_count(context_id: str) -> int:
    if _buffer is None:
        return 0
    return len(_buffer._buffers.get(context_id, []))  # noqa: SLF001
