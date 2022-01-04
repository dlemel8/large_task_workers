from prometheus_client import Histogram, start_http_server
from prometheus_client.utils import INF

from consumer.application.processor import Milliseconds, ProcessorReporter, SelectorReporter

HISTOGRAM_BUCKETS = [10 ** x for x in range(5)] + [INF]
TASK_CONSUMER_DURATIONS = Histogram('consumer_duration_ms', '', buckets=HISTOGRAM_BUCKETS)
SELECTOR_DURATIONS = Histogram('selector_duration_ms', '', buckets=HISTOGRAM_BUCKETS)
PROCESSOR_DURATIONS = Histogram('processor_duration_ms', '', labelnames=('type', 'success'), buckets=HISTOGRAM_BUCKETS)


class PrometheusReporter(SelectorReporter, ProcessorReporter):
    def selected_processor(self, took: Milliseconds) -> None:
        SELECTOR_DURATIONS.observe(took)

    def processed_task(self, took: Milliseconds, type_: str, success: bool) -> None:
        PROCESSOR_DURATIONS.labels(type=type_, success=str(success)).observe(took)


def serve_prometheus_metrics(metrics_port: int) -> None:
    start_http_server(metrics_port)
