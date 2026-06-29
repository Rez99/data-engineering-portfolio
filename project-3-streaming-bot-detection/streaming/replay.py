"""Replay historical clickstream events into the streaming pipeline.

Milestone M1.1 establishes the replay application shell: runtime configuration,
idempotent source dataset download, and a safe executable entrypoint. The event
replay loop and Kafka publishing are introduced in later milestones.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen


DEFAULT_SOURCE_URL = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
PROJECT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATASET_PATH = PROJECT_DIR / "data/source/2019-Oct.csv.gz"


@dataclass(frozen=True)
class ReplayConfig:
    """Validated runtime configuration for the replay application."""

    dataset_path: Path
    source_url: str
    speed: float
    start_row: int
    rows: int | None
    debug: bool


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
    parser.add_argument(
        "--rows",
        type=positive_int,
        default=None,
        help="Maximum number of data rows to replay. Omit to replay the full dataset.",
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
    print(f"Debug mode: {'on' if config.debug else 'off'}")
    print("Replay engine and Kafka publishing will be implemented in M1.2+.")


def main(argv: list[str] | None = None) -> int:
    """Run the M1.1 replay application shell."""
    try:
        config = load_config(argv)
        downloaded = download_dataset(config.source_url, config.dataset_path)
        print_config(config, downloaded)
    except (argparse.ArgumentError, RuntimeError, OSError) as error:
        print(f"replay.py failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
