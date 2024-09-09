from newrelic_telemetry_sdk import EventClient, LogClient, MetricClient, SpanClient
from newrelic_telemetry_sdk import (
    Event,
    Log,
    CountMetric,
    GaugeMetric,
    SummaryMetric,
    Span,
)
from newrelic_telemetry_sdk.metric import DEFAULT as METRIC_DEFAULT
from urllib3.response import HTTPResponse
from newrelic_telemetry_sdk.client import Client
from concurrent.futures import ThreadPoolExecutor
import time
import traceback
import threading
import weakref


class AnalyticsManager:

    def __init__(
        self,
        license_key: str,
        environment_name: str,
        batch_report_stop_check_interval: int = 10,
        batch_report_sleep_interval: int = 90,
    ):
        print("Initializing Analytics Manager")
        self.__environment_name = environment_name

        self.__event_client = EventClient(license_key)
        self.__log_client = LogClient(license_key)
        self.__metric_client = MetricClient(license_key)
        self.__span_client = SpanClient(license_key)

        self._events = []
        self._logs = []
        self._metrics = []
        self._spans = []

        self.__BATCH_REPORT_STOP_CHECK_INTERVAL = batch_report_stop_check_interval
        self.__BATCH_REPORT_SLEEP_INTERVAL = batch_report_sleep_interval

        self.__executor = ThreadPoolExecutor(max_workers=5)
        self.__stop_reporting = False
        self.__report()

        # Shutdown the Analytics Manager on destruction
        weakref.finalize(self, self.__shutdown)

    @property
    def data_to_report_exists(self):
        return (
            len(self._events) > 0
            or len(self._logs) > 0
            or len(self._metrics) > 0
            or len(self._spans) > 0
        )

    def __shutdown(self):
        """Shutdown the Analytics Manager."""
        print("Shutting down Analytics Manager")
        self.__stop_reporting = True
        self.__batch_reporting_thread.join()
        if self.data_to_report_exists:
            self.__send_data()  # Send any remaining data using the main thread before shutdown
        self.__executor.shutdown(wait=True)

    def __report(self):
        """Periodically send data to New Relic."""

        def worker():
            elapsed_time = 0

            while not self.__stop_reporting:
                while elapsed_time < self.__BATCH_REPORT_SLEEP_INTERVAL:
                    # Sleep for the interval time
                    time.sleep(self.__BATCH_REPORT_STOP_CHECK_INTERVAL)
                    # Increment the elapsed time
                    elapsed_time += self.__BATCH_REPORT_STOP_CHECK_INTERVAL
                    # Check if the reporting should be stopped
                    if self.__stop_reporting:
                        break
                try:
                    self.__send_data()
                except Exception as e:
                    traceback.print_exc()

                elapsed_time = 0  # IMP: Do not remove, Reset the elapsed time

        # Batch reporting thread, need to be daemonized to avoid blocking the main thread on exit
        self.__batch_reporting_thread = threading.Thread(target=worker)
        self.__batch_reporting_thread.daemon = True
        self.__batch_reporting_thread.start()

    def __send_batch(self, dt_type: str, client: Client):
        """Send a batch of data to New Relic.

        Args:
            dt_type (str): The type of data.
            client (Client): The client to send the data.

        Raises:
            Exception: If the data fails to send.
        """
        data = getattr(self, dt_type)
        if len(data) == 0:
            return

        try:
            resp: HTTPResponse = client.send_batch(data)
            if resp.status in [200, 201, 202]:
                setattr(self, dt_type, [])
                print(
                    f"Successfully sent data of type: {dt_type.lstrip('_')}, status code: {resp.status}"
                )
            else:
                raise Exception(
                    f"Failed to send data of type: {dt_type.lstrip('_')}, status code: {resp.status}"
                )
        except Exception as e:
            traceback.print_exc()

    def __send_data(self):
        """Send all data to New Relic."""
        if not self.data_to_report_exists:
            return

        print("Sending data to New Relic")

        events_batch = None
        logs_batch = None
        metrics_batch = None
        span_batch = None

        def print_runtime_error(e):
            print("Failed to send data, Error: ", e)
            print("Retrying on the batch reporting thread")

        # On Destruction of AnalyticsManager, the executor throws a RuntimeError (cannot schedule new futures after interpreter shutdown)
        # Hence, we need to catch the exception and send the data on the main thread
        try:
            events_batch = self.__executor.submit(
                self.__send_batch, "_events", self.__event_client
            )
        except RuntimeError as e:
            print_runtime_error(e)
            self.__send_batch("_events", self.__event_client)
        try:
            logs_batch = self.__executor.submit(
                self.__send_batch, "_logs", self.__log_client
            )
        except RuntimeError as e:
            print_runtime_error(e)
            self.__send_batch("_logs", self.__log_client)
        try:
            metrics_batch = self.__executor.submit(
                self.__send_batch, "_metrics", self.__metric_client
            )
        except RuntimeError as e:
            print_runtime_error(e)
            self.__send_batch("_metrics", self.__metric_client)
        try:
            span_batch = self.__executor.submit(
                self.__send_batch, "_spans", self.__span_client
            )
        except RuntimeError as e:
            print_runtime_error(e)
            self.__send_batch("_spans", self.__span_client)

        for batch in [events_batch, logs_batch, metrics_batch, span_batch]:
            if batch:
                batch.result()

    def __send_imediate(self, data, client: Client):
        try:
            resp: HTTPResponse = client.send(data)
            if resp.status in [200, 201, 202]:
                print(f"Successfully sent data, status code: {resp.status}")
            else:
                raise Exception(f"Failed to send data, status code: {resp.status}")
        except Exception as e:
            traceback.print_exc()

    def __send(self, data, client: Client):
        """Send data to New Relic.

        Args:
            data (Union[Event, Log, CountMetric, GaugeMetric, SummaryMetric, Span]): The data to send.
            client (Client): The client to send the data.
        """
        self.__executor.submit(self.__send_imediate, data, client)

    def __add_environment(self, attributes: dict):
        """Add the environment name to the attributes.

        Args:
            attributes (dict): The attributes to add the environment name to.

        Returns:
            dict: The attributes with the environment name added.
        """
        attributes["environment"] = self.__environment_name
        return attributes

    def __validate_attributes(self, attributes: dict = {}):
        """Validate the attributes.

        Args:
            attributes (dict): The attributes to validate.

        Raises:
            ValueError: If any attribute has a value of None.

        Returns:
            dict: The validated attributes.
        """
        for key, value in attributes.items():
            # No value should be None
            if value is None:
                del attributes[key]
            # Values should be string, int, float, list or dict
            if not isinstance(value, (str, int, float, list, dict)):
                raise ValueError(f"Attribute {key} has invalid value {value}")

        return self.__add_environment(attributes)

    def report_event(
        self,
        event_name: str,
        attributes: dict = {},
        timestamp_ms: int = None,
        immediate: bool = False,
    ):
        """Report an event to New Relic.

        Args:
            event_name (str): The name of the event.
            attributes (dict): The attributes of the event.
            timestamp_ms (int, optional): The timestamp of the event. Defaults to None.
            immediate (bool, optional): Whether to send the event immediately. Defaults to False.
        """
        event = Event(
            event_type=event_name,
            tags=self.__validate_attributes(attributes),
            timestamp_ms=timestamp_ms,
        )
        if immediate:
            self.__send(event, self.__event_client)
        else:
            self._events.append(event)

    def report_log(
        self,
        log_name: str,
        attributes: dict = {},
        timestamp: int = None,
        immediate: bool = False,
    ):
        """Report a log to New Relic.

        Args:
            log_name (str): The name of the log.
            attributes (dict): The attributes of the log.
            timestamp (int, optional): The timestamp of the log. Defaults to None.
            immediate (bool, optional): Whether to send the log immediately. Defaults to False.
        """
        attributes = self.__validate_attributes(attributes)
        log = Log(
            message=log_name,
            timestamp=timestamp,
            **attributes,
        )
        if immediate:
            self.__send(log, self.__log_client)
        else:
            self._logs.append(log)

    def report_count_metric(
        self,
        metric_name: str,
        value: float,
        interval_ms: int,
        attributes: dict = {},
        end_time_ms: int = METRIC_DEFAULT,
        immediate: bool = False,
    ):
        """Report a count metric to New Relic.

        Args:
            metric_name (str): The name of the metric.
            value (float): The value of the metric.
            interval_ms (int): The interval of the metric.
            attributes (dict, optional): The attributes of the metric. Defaults to None.
            end_time_ms (int, optional): The end time of the metric. Defaults to METRIC_DEFAULT.
            immediate (bool, optional): Whether to send the metric immediately. Defaults to False.
        """

        metric = CountMetric(
            name=metric_name,
            value=value,
            interval_ms=interval_ms,
            tags=self.__validate_attributes(attributes),
            end_time_ms=end_time_ms,
        )
        if immediate:
            self.__send(metric, self.__metric_client)
        else:
            self._metrics.append(metric)

    def report_gauge_metric(
        self,
        metric_name: str,
        value: float,
        attributes: dict = {},
        end_time_ms: int = METRIC_DEFAULT,
        immediate: bool = False,
    ):
        """Report a gauge metric to New Relic.

        Args:
            metric_name (str): The name of the metric.
            value (float): The value of the metric.
            attributes (dict, optional): The attributes of the metric. Defaults to None.
            end_time_ms (int, optional): The end time of the metric. Defaults to METRIC_DEFAULT.
            immediate (bool, optional): Whether to send the metric immediately. Defaults to False.
        """
        metric = GaugeMetric(
            name=metric_name,
            value=value,
            tags=self.__validate_attributes(attributes),
            end_time_ms=end_time_ms,
        )
        if immediate:
            self.__send(metric, self.__metric_client)
        else:
            self._metrics.append(metric)

    def report_summary_metric(
        self,
        metric_name: str,
        count: int,
        sum: float,
        min: float,
        max: float,
        interval_ms: int | float,
        attributes: dict = {},
        end_time_ms: int = METRIC_DEFAULT,
        immediate: bool = False,
    ):
        """Report a summary metric to New Relic.

        Args:
            metric_name (str): The name of the metric.
            count (int): The count of the metric.
            sum (float): The sum of the metric.
            min (float): The minimum value of the metric.
            max (float): The maximum value of the metric.
            interval_ms (int | float): The interval of the metric.
            attributes (dict, optional): The attributes of the metric. Defaults to None.
            end_time_ms (int, optional): The end time of the metric. Defaults to METRIC_DEFAULT.
            immediate (bool, optional): Whether to send the metric immediately. Defaults to False.
        """
        metric = SummaryMetric(
            name=metric_name,
            count=count,
            sum=sum,
            min=min,
            max=max,
            interval_ms=interval_ms,
            tags=self.__validate_attributes(attributes),
            end_time_ms=end_time_ms,
        )
        if immediate:
            self.__send(metric, self.__metric_client)
        else:
            self._metrics.append(metric)

    def report_span(
        self,
        span_name: str,
        attributes: dict = {},
        trace_id: str = None,
        start_time_ms: int = None,
        duration_ms: int = None,
        guid: str = None,
        parent_id: str = None,
        immediate: bool = False,
    ):
        """Report a span to New Relic.

        Args:
            span_name (str): The name of the span.
            attributes (dict): The attributes of the span.
            guid (str, optional): The GUID of the span.
            trace_id (str, optional): The trace ID of the span.
            parent_id (str, optional): The parent ID of the span.
            start_time_ms (int, optional): The start time of the span.
            duration_ms (int, optional): The duration of the span.
            immediate (bool, optional): Whether to send the span immediately. Defaults to False.
        """
        span = Span(
            name=span_name,
            tags=self.__validate_attributes(attributes),
            guid=guid,
            trace_id=trace_id,
            parent_id=parent_id,
            start_time_ms=start_time_ms,
            duration_ms=duration_ms,
        )
        if immediate:
            self.__send(span, self.__span_client)
        else:
            self._spans.append(span)
