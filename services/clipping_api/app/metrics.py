"""Métricas simples em memória, expostas em formato Prometheus."""

import threading
from collections import Counter


class ApiMetrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self.requests: Counter = Counter()
            self.latency_ms_sum = 0
            self.latency_ms_count = 0
            self.throttled = 0
            self.triggers = 0

    def record(self, route: str, status: int, latency_ms: int) -> None:
        with self._lock:
            self.requests[(route, status)] += 1
            self.latency_ms_sum += latency_ms
            self.latency_ms_count += 1
            if status == 429:
                self.throttled += 1

    def record_trigger(self) -> None:
        with self._lock:
            self.triggers += 1

    def render(self, source_statuses: dict | None = None) -> str:
        with self._lock:
            requests = dict(self.requests)
            latency_sum = self.latency_ms_sum
            latency_count = self.latency_ms_count
            throttled = self.throttled
            triggers = self.triggers

        lines = [
            "# HELP clipping_api_requests_total Requisições por rota e status.",
            "# TYPE clipping_api_requests_total counter",
        ]
        for (route, status), count in sorted(requests.items()):
            lines.append(
                f'clipping_api_requests_total{{route="{route}",status="{status}"}} '
                f"{count}"
            )

        lines += [
            "# HELP clipping_api_request_duration_ms_sum Soma das latências.",
            "# TYPE clipping_api_request_duration_ms_sum counter",
            f"clipping_api_request_duration_ms_sum {latency_sum}",
            "# HELP clipping_api_request_duration_ms_count Requisições medidas.",
            "# TYPE clipping_api_request_duration_ms_count counter",
            f"clipping_api_request_duration_ms_count {latency_count}",
            "# HELP clipping_api_throttled_total Respostas 429.",
            "# TYPE clipping_api_throttled_total counter",
            f"clipping_api_throttled_total {throttled}",
            "# HELP clipping_api_triggers_total Disparos de execução aceitos.",
            "# TYPE clipping_api_triggers_total counter",
            f"clipping_api_triggers_total {triggers}",
        ]

        if source_statuses:
            lines += [
                "# HELP clipping_api_source_status Último status conhecido da fonte.",
                "# TYPE clipping_api_source_status gauge",
            ]
            for source, row in sorted(source_statuses.items()):
                status = row.get("status") or "unknown"
                lines.append(
                    f'clipping_api_source_status{{source="{source}",'
                    f'status="{status}"}} 1'
                )

        return "\n".join(lines) + "\n"


metrics = ApiMetrics()
