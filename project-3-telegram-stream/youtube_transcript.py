"""Print the available English transcript for one public YouTube video."""

from __future__ import annotations

import argparse
from urllib.parse import parse_qs, urlparse

from youtube_transcript_api import YouTubeTranscriptApi


def extract_video_id(url: str) -> str:
    """Extract a video ID from common YouTube URL formats."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    path_parts = [part for part in parsed.path.split("/") if part]

    if host in {"youtube.com", "www.youtube.com", "m.youtube.com", "music.youtube.com"}:
        if parsed.path == "/watch":
            video_id = parse_qs(parsed.query).get("v", [None])[0]
        elif len(path_parts) >= 2 and path_parts[0] in {"shorts", "embed", "live"}:
            video_id = path_parts[1]
        else:
            video_id = None
    elif host == "youtu.be":
        video_id = path_parts[0] if path_parts else None
    else:
        video_id = None

    if not video_id:
        raise ValueError(f"Unsupported or incomplete YouTube URL: {url}")
    return video_id


def get_youtube_transcript_text(url: str, languages: list[str] | None = None) -> str:
    """Fetch a transcript and return only its text snippets."""
    video_id = extract_video_id(url)
    transcript = YouTubeTranscriptApi().fetch(video_id, languages=languages or ["en"])
    return "\n".join(snippet.text for snippet in transcript)


def main() -> None:
    parser = argparse.ArgumentParser(description="Print a public YouTube video's transcript.")
    parser.add_argument("url", help="YouTube watch, short, embed, live, or youtu.be URL")
    parser.add_argument(
        "--languages",
        nargs="+",
        default=["en"],
        help="Preferred language codes, in priority order (default: en)",
    )
    args = parser.parse_args()
    print(get_youtube_transcript_text(args.url, args.languages))


if __name__ == "__main__":
    main()
