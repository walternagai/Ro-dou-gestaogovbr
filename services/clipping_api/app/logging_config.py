"""Log estruturado (JSON) para a API do clipping."""

import json
import logging
import sys

_EXTRA_FIELDS = (
    "event",
    "route",
    "status",
    "latency_ms",
    "key_id",
    "run_id",
    "request_id",
    "traceparent",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for field in _EXTRA_FIELDS:
            if hasattr(record, field):
                value = getattr(record, field)
                if value is not None:
                    payload[field] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> logging.Logger:
    """Configura o logger da API sem depender do logging do uvicorn."""
    logger = logging.getLogger("clipping.api")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(isinstance(h.formatter, JsonFormatter) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.handlers = [handler]

    return logger


logger = configure_logging()
