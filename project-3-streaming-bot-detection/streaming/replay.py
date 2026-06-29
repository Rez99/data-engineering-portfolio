"""Replay historical clickstream events into the streaming pipeline.

The M1 replay engine reads the October 2019 clickstream CSV incrementally,
emits events according to their original event timestamps, and routes prepared
events through logical producer workers. Events can be written to the console
for local replay inspection or published to the raw Kafka-compatible
``clickstream-raw`` topic through Redpanda's bundled ``rpk`` CLI.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import random
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable, TextIO
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen


DEFAULT_SOURCE_URL = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_PATH = PROJECT_DIR / "data/source/2019-Oct.csv.gz"
DEFAULT_COMPOSE_FILE = PROJECT_DIR / "infra/compose/kafka.yml"
DEFAULT_KAFKA_TOPIC = "clickstream-raw"
DEFAULT_KAFKA_BROKERS = "localhost:9092"
SUPPORTED_EVENT_TYPES = {"view", "cart", "purchase", "remove_from_cart"}
CORRUPTION_SCENARIOS = (
    ("null user_session", "user_session", ""),
    ("invalid timestamp", "event_time", "not-a-timestamp"),
    ("unknown event_type", "event_type", "unknown_event"),
    ("missing product_id", "product_id", ""),
)


@dataclass(frozen=True)
class ReplayConfig:
    """Validated runtime configuration for the replay application."""

    dataset_path: Path
    source_url: str
    speed: float
    start_row: int
    rows: int | None
    loop_seconds: float
    delay_probability: float
    mean_delay_seconds: float
    corrupt_probability: float
    random_seed: int
    sink: str
    kafka_topic: str
    kafka_brokers: str
    compose_file: Path
    sleep: bool
    debug: bool


@dataclass(frozen=True)
class SourceEvent:
    """A source row paired with its parsed event timestamp."""

    row_number: int
    event_time: datetime
    event: dict[str, str]


@dataclass(frozen=True)
class PreparedEvent:
    """An event prepared for dispatch during a replay tick."""

    row_number: int
    event_time: datetime
    send_time: datetime
    event: dict[str, str]
    producer_name: str
    delayed: bool
    corruption_reason: str | None


def parse_speed(value: str) -> float:
    """Parse replay speed values such as ``1x``, ``100x``, or ``100``."""
    normalized = value.strip().lower()
    if normalized.endswith("x"):
        normalized = normalized[:-1]

    try:
        speed = float(normalized)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "speed must be a number, e.g. 1x or 100x"
        ) from error

    if speed <= 0:
        raise argparse.ArgumentTypeError("speed must be greater than zero")
    return speed


def positive_int(value: str) -> int:
    """Parse a positive integer CLI value."""
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error

    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def non_negative_float(value: str) -> float:
    """Parse a non-negative float CLI value."""
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be a number") from error

    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
    return parsed


def positive_float(value: str) -> float:
    """Parse a positive float CLI value."""
    parsed = non_negative_float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def probability(value: str) -> float:
    """Parse a probability in the closed interval [0, 1]."""
    parsed = non_negative_float(value)
    if parsed > 1:
        raise argparse.ArgumentTypeError("probability must be between 0 and 1")
    return parsed


def non_negative_int(value: str) -> int:
    """Parse a non-negative integer CLI value."""
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error

    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser."""
    parser = argparse.ArgumentParser(
        description="Prepare the October 2019 clickstream dataset for replay.",
    )
    parser.add_argument(
        "--dataset-path",
        type=Path,
        default=DEFAULT_DATASET_PATH,
        help=f"Local source CSV path. Default: {DEFAULT_DATASET_PATH}",
    )
    parser.add_argument(
        "--source-url",
        default=DEFAULT_SOURCE_URL,
        help=f"Source CSV URL. Default: {DEFAULT_SOURCE_URL}",
    )
    parser.add_argument(
        "--speed",
        type=parse_speed,
        default=parse_speed("1x"),
        help="Replay speed multiplier, e.g. 1x or 100x. Default: 1x",
    )
    parser.add_argument(
        "--start-row",
        type=non_negative_int,
        default=0,
        help="Zero-based data row offset to begin replaying from. Default: 0",
    )
    row_group = parser.add_mutually_exclusive_group()
    row_group.add_argument(
        "--rows",
        type=positive_int,
        default=None,
        help="Maximum number of data rows to replay. Omit to replay the full dataset.",
    )
    row_group.add_argument(
        "--full",
        action="store_true",
        help="Replay the full dataset. This is the default when --rows is omitted.",
    )
    parser.add_argument(
        "--loop-seconds",
        type=positive_float,
        default=1.0,
        help="Real seconds between replay ticks. Default: 1",
    )
    parser.add_argument(
        "--delay-probability",
        type=probability,
        default=0.01,
        help="Probability that an event receives artificial network delay. Default: 0.01",
    )
    parser.add_argument(
        "--mean-delay-seconds",
        type=non_negative_float,
        default=5.0,
        help="Mean artificial delay in event-time seconds. Default: 5",
    )
    parser.add_argument(
        "--corrupt-probability",
        type=probability,
        default=0.001,
        help="Probability that an event receives artificial field corruption. Default: 0.001",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=1,
        help="Random seed for reproducible delay and corruption simulation. Default: 1",
    )
    parser.add_argument(
        "--sink",
        choices=("console", "kafka"),
        default="console",
        help="Output sink for replayed events. Default: console",
    )
    parser.add_argument(
        "--kafka-topic",
        default=DEFAULT_KAFKA_TOPIC,
        help=f"Kafka topic used when --sink kafka. Default: {DEFAULT_KAFKA_TOPIC}",
    )
    parser.add_argument(
        "--kafka-brokers",
        default=DEFAULT_KAFKA_BROKERS,
        help=f"Broker address used inside the Redpanda container. Default: {DEFAULT_KAFKA_BROKERS}",
    )
    parser.add_argument(
        "--compose-file",
        type=Path,
        default=DEFAULT_COMPOSE_FILE,
        help=f"Docker Compose file used for rpk publishing. Default: {DEFAULT_COMPOSE_FILE}",
    )
    parser.add_argument(
        "--no-sleep",
        action="store_true",
        help="Advance replay ticks without waiting in real time.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print extra runtime information useful during local development.",
    )
    return parser


def load_config(argv: list[str] | None = None) -> ReplayConfig:
    """Parse and validate command-line arguments."""
    args = build_parser().parse_args(argv)
    return ReplayConfig(
        dataset_path=args.dataset_path,
        source_url=args.source_url,
        speed=args.speed,
        start_row=args.start_row,
        rows=args.rows,
        loop_seconds=args.loop_seconds,
        delay_probability=args.delay_probability,
        mean_delay_seconds=args.mean_delay_seconds,
        corrupt_probability=args.corrupt_probability,
        random_seed=args.random_seed,
        sink=args.sink,
        kafka_topic=args.kafka_topic,
        kafka_brokers=args.kafka_brokers,
        compose_file=args.compose_file,
        sleep=not args.no_sleep,
        debug=args.debug,
    )


def source_name(source_url: str) -> str:
    """Return a compact source name for status messages."""
    parsed = urlparse(source_url)
    return parsed.geturl() if parsed.scheme == "file" else source_url


def download_dataset(source_url: str, dataset_path: Path) -> bool:
    """Download the source CSV if it is not already present.

    Returns ``True`` when a new file was downloaded and ``False`` when the
    existing dataset was left untouched.
    """
    if dataset_path.exists():
        print(f"Dataset already present; skipping download: {dataset_path}")
        return False

    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading dataset from {source_name(source_url)}")
    print(f"Destination: {dataset_path}")

    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{dataset_path.name}.",
            suffix=".tmp",
            dir=dataset_path.parent,
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            with urlopen(source_url) as response:
                shutil.copyfileobj(response, temp_file)

        temp_path.replace(dataset_path)
        print(f"Downloaded dataset: {dataset_path}")
        return True
    except (OSError, URLError) as error:
        if temp_path and temp_path.exists():
            temp_path.unlink()
        raise RuntimeError(f"Failed to download dataset from {source_url}") from error


def print_config(config: ReplayConfig, downloaded: bool) -> None:
    """Print resolved runtime settings."""
    row_limit = "full dataset" if config.rows is None else f"{config.rows:,}"
    print("Replay application ready.")
    print(f"Dataset path: {config.dataset_path}")
    print(f"Dataset action: {'downloaded' if downloaded else 'reused existing file'}")
    print(f"Replay speed: {config.speed:g}x")
    print(f"Start row: {config.start_row:,}")
    print(f"Rows: {row_limit}")
    print(f"Loop interval: {config.loop_seconds:g}s")
    print(f"Delay probability: {config.delay_probability:g}")
    print(f"Mean delay: {config.mean_delay_seconds:g}s")
    print(f"Corrupt probability: {config.corrupt_probability:g}")
    print(f"Random seed: {config.random_seed}")
    print(f"Sink: {config.sink}")
    if config.sink == "kafka":
        print(f"Kafka topic: {config.kafka_topic}")
        print(f"Kafka brokers: {config.kafka_brokers}")
        print(f"Compose file: {config.compose_file}")
    print(f"Sleep between ticks: {'yes' if config.sleep else 'no'}")
    print(f"Debug mode: {'on' if config.debug else 'off'}")


def open_csv(path: Path) -> TextIO:
    """Open a plain or gzip-compressed CSV file as text."""
    if path.suffix == ".gz":
        return gzip.open(path, mode="rt", newline="", encoding="utf-8")
    return path.open(mode="r", newline="", encoding="utf-8")


def parse_event_time(value: str) -> datetime:
    """Parse event timestamps from the October 2019 source dataset."""
    normalized = value.strip()
    if normalized.endswith(" UTC"):
        normalized = normalized.removesuffix(" UTC")
        return datetime.strptime(normalized, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)

    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iter_source_events(path: Path, start_row: int, rows: int | None) -> Iterable[SourceEvent]:
    """Yield source events from the CSV without loading the full file."""
    emitted = 0
    with open_csv(path) as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader):
            if row_number < start_row:
                continue
            if rows is not None and emitted >= rows:
                break

            try:
                event_time = parse_event_time(row["event_time"])
            except (KeyError, ValueError) as error:
                raise RuntimeError(
                    f"Could not parse event_time on data row {row_number}"
                ) from error

            emitted += 1
            yield SourceEvent(row_number=row_number, event_time=event_time, event=row)


def producer_name(event_type: str) -> str:
    """Return the temporary console producer name for an event type."""
    names = {
        "view": "ViewProducer",
        "cart": "CartProducer",
        "purchase": "PurchaseProducer",
        "remove_from_cart": "RemoveFromCartProducer",
    }
    return names.get(event_type, "UnknownProducer")


def maybe_delay(
    event_time: datetime,
    rng: random.Random,
    delay_probability: float,
    mean_delay_seconds: float,
) -> tuple[datetime, bool]:
    """Return a send timestamp and whether delay was applied."""
    if delay_probability == 0 or mean_delay_seconds == 0:
        return event_time, False
    if rng.random() >= delay_probability:
        return event_time, False

    delay_seconds = rng.expovariate(1 / mean_delay_seconds)
    return event_time + timedelta(seconds=delay_seconds), True


def maybe_corrupt(
    event: dict[str, str],
    rng: random.Random,
    corrupt_probability: float,
) -> tuple[dict[str, str], str | None]:
    """Return a possibly corrupted event copy and a corruption reason."""
    prepared = dict(event)
    if corrupt_probability == 0 or rng.random() >= corrupt_probability:
        return prepared, None

    reason, field, value = rng.choice(CORRUPTION_SCENARIOS)
    prepared[field] = value
    return prepared, reason


def prepare_event(
    source_event: SourceEvent,
    rng: random.Random,
    config: ReplayConfig,
) -> PreparedEvent:
    """Apply simulated delay and corruption to a source event."""
    send_time, delayed = maybe_delay(
        source_event.event_time,
        rng,
        config.delay_probability,
        config.mean_delay_seconds,
    )
    event, corruption_reason = maybe_corrupt(
        source_event.event,
        rng,
        config.corrupt_probability,
    )
    return PreparedEvent(
        row_number=source_event.row_number,
        event_time=source_event.event_time,
        send_time=send_time,
        event=event,
        producer_name=producer_name(event.get("event_type", "")),
        delayed=delayed,
        corruption_reason=corruption_reason,
    )


def format_timestamp(value: datetime) -> str:
    """Format timestamps for human-readable replay traces."""
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def print_trace(
    tick: int,
    config: ReplayConfig,
    prepared_events: list[PreparedEvent],
    last_event_time_sent: datetime | None,
) -> datetime | None:
    """Print one replay tick and return the latest event-time sent so far."""
    print(f"Replay: {config.speed:g}x | Tick: {tick}")
    if not prepared_events:
        print("(no events dispatched)")
        print()
        return last_event_time_sent

    for event in prepared_events:
        flags: list[str] = []
        if event.delayed:
            flags.append("DELAYED")
        if last_event_time_sent and event.event_time < last_event_time_sent:
            flags.append("OUT OF ORDER")
        if event.corruption_reason:
            flags.append(f"CORRUPTED: {event.corruption_reason}")

        flag_text = "   " + " ".join(f"[{flag}]" for flag in flags) if flags else ""
        event_type = event.event.get("event_type", "")
        print(
            f"{format_timestamp(event.event_time)}  -> {format_timestamp(event.send_time)}   "
            f"{event_type.upper():<16} {event.producer_name:<22}{flag_text}"
        )
        if last_event_time_sent is None or event.event_time > last_event_time_sent:
            last_event_time_sent = event.event_time

    print()
    return last_event_time_sent


class EventSink:
    """Output target for prepared replay events."""

    def start(self) -> None:
        """Open resources needed before replay starts."""

    def send(self, prepared_event: PreparedEvent) -> None:
        """Send one prepared event."""

    def close(self) -> None:
        """Close resources after replay completes."""


class ConsoleSink(EventSink):
    """No-op sink used with the human-readable console trace."""

    def send(self, prepared_event: PreparedEvent) -> None:
        return


class RpkKafkaSink(EventSink):
    """Publish JSON records to Kafka through the local Redpanda container."""

    def __init__(self, config: ReplayConfig) -> None:
        self.config = config
        self.process: subprocess.Popen[str] | None = None

    def start(self) -> None:
        if not self.config.compose_file.exists():
            raise RuntimeError(f"Compose file not found: {self.config.compose_file}")

        command = [
            "docker",
            "compose",
            "-f",
            str(self.config.compose_file),
            "exec",
            "-T",
            "redpanda",
            "rpk",
            "topic",
            "produce",
            self.config.kafka_topic,
            "--brokers",
            self.config.kafka_brokers,
            "--format",
            "%v{json}\n",
            "--output-format",
            "",
        ]
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            text=True,
        )

    def send(self, prepared_event: PreparedEvent) -> None:
        if self.process is None or self.process.stdin is None:
            raise RuntimeError("Kafka sink is not started")
        if self.process.poll() is not None:
            raise RuntimeError("Kafka producer process exited before replay finished")

        payload = json.dumps(prepared_event.event, separators=(",", ":"))
        try:
            self.process.stdin.write(payload + "\n")
        except BrokenPipeError as error:
            raise RuntimeError("Kafka producer process closed unexpectedly") from error

    def close(self) -> None:
        if self.process is None:
            return

        if self.process.stdin:
            self.process.stdin.close()

        exit_code = self.process.wait()
        self.process = None
        if exit_code != 0:
            raise RuntimeError(f"Kafka producer exited with status {exit_code}")


def build_sink(config: ReplayConfig) -> EventSink:
    """Create the configured replay output sink."""
    if config.sink == "kafka":
        return RpkKafkaSink(config)
    return ConsoleSink()


def run_replay(config: ReplayConfig) -> None:
    """Replay source events to the configured output sink."""
    source_iterator = iter(iter_source_events(config.dataset_path, config.start_row, config.rows))
    try:
        next_event = next(source_iterator)
    except StopIteration:
        print("No events to replay.")
        return

    rng = random.Random(config.random_seed)
    cursor = next_event.event_time
    cursor_increment = timedelta(seconds=config.speed * config.loop_seconds)
    tick = 1
    total_dispatched = 0
    last_event_time_sent: datetime | None = None
    pending_events: list[PreparedEvent] = []
    sink = build_sink(config)

    print("Producer workers started: ViewProducer, CartProducer, PurchaseProducer, RemoveFromCartProducer")
    if config.sink == "kafka":
        print(f"Publishing to Kafka topic: {config.kafka_topic}")
    else:
        print("Writing producer output to the console trace only.")
    print()

    sink.start()
    try:
        while next_event is not None or pending_events:
            while next_event is not None and next_event.event_time <= cursor:
                pending_events.append(prepare_event(next_event, rng, config))
                try:
                    next_event = next(source_iterator)
                except StopIteration:
                    next_event = None

            prepared_events = [
                event
                for event in pending_events
                if event.send_time <= cursor
            ]
            pending_events = [
                event
                for event in pending_events
                if event.send_time > cursor
            ]
            prepared_events.sort(key=lambda event: (event.send_time, event.row_number))

            for prepared_event in prepared_events:
                sink.send(prepared_event)

            last_event_time_sent = print_trace(
                tick,
                config,
                prepared_events,
                last_event_time_sent,
            )
            total_dispatched += len(prepared_events)

            if next_event is None and not pending_events:
                break

            tick += 1
            cursor += cursor_increment
            if config.sleep:
                time.sleep(config.loop_seconds)
    finally:
        sink.close()

    print(f"Replay complete. Dispatched {total_dispatched:,} event(s).")


def main(argv: list[str] | None = None) -> int:
    """Run the M1.1 replay application shell."""
    try:
        config = load_config(argv)
        downloaded = download_dataset(config.source_url, config.dataset_path)
        print_config(config, downloaded)
        run_replay(config)
    except (argparse.ArgumentError, RuntimeError, OSError) as error:
        print(f"replay.py failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
