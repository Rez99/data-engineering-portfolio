# Project 3: Live Telegram Listener

The smallest proof of concept for the first leg of the planned pipeline:

```text
Public Telegram channels -> Telethon listener -> stdout
                                      \\-> Kafka (later)
```

It connects using your own Telegram account and prints every **new** message from a small configured set of public channels. It does not backfill history, send messages, use Kafka, create embeddings, or summarize content.

## Prerequisites

- macOS with Python 3.10 or newer (`python3 --version`)
- A Telegram account able to receive a login code
- Access to the public channels you want to monitor

## Get Telegram API credentials

Telethon needs Telegram's developer API credentials. These identify your local client application; they are different from a bot token.

1. Sign in to [my.telegram.org](https://my.telegram.org) with the phone number for the Telegram account that will run the listener.
2. Open **API development tools**.
3. Create an application (the name and URL can be simple local-development values).
4. Copy the displayed `api_id` and `api_hash`. Treat the hash like a secret: do not commit or share it.

## Subscribe to public channels

This listener uses the account you sign in with on its first run. Before starting it, open each public channel in the Telegram app and click **Join**. Then use either its username (`channel_name`) or its public link (`https://t.me/channel_name`) in `TELEGRAM_CHANNELS`. Telegram's browser-preview form (`https://t.me/s/channel_name`) is also accepted.

For example, for `https://t.me/s/sitreports`, configure `sitreports` (or use the full link). Private channels, invite links, and channels that your account has not joined are outside this proof of concept.

## Setup and run

From VS Code's integrated terminal:

```bash
cd project-3-telegram-stream
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
cp .env.example .env
```

Edit `.env` and set your real API ID, API hash, and one or more comma-separated channel names:

```dotenv
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_real_api_hash
TELEGRAM_SESSION_NAME=telegram_listener
TELEGRAM_CHANNELS=channel_one,channel_two
```

Start the listener:

```bash
python telegram_listener.py
```

On the first run, Telethon prompts for your phone number and the login code sent by Telegram (and your two-step verification password, if enabled). It saves an authenticated `telegram_listener.session` file locally, so later starts do not normally prompt again. The session file and `.env` are ignored by Git.

When a configured channel posts, the terminal prints its UTC timestamp, channel identifier, message ID, and text immediately. Leave the process running; press `Ctrl+C` to stop it.

## Project layout

```text
project-3-telegram-stream/
├── .env.example          # Safe configuration template
├── .gitignore            # Keeps secrets/session state out of Git
├── README.md             # Local setup and operating guide
├── requirements.txt      # Python dependencies
└── telegram_listener.py  # Telegram connection and stdout event sink
```

## Troubleshooting

- **`Missing required environment variable(s)`**: make sure you copied `.env.example` to `.env`, and filled in its required values.
- **Channel fails during startup**: verify the public username/link and join the channel in the same Telegram account used by the listener.
- **No output after startup**: the listener only receives messages posted after it starts. Ask a channel you control to publish a test message.
- **Login/security warning**: Telegram may show a new-login notification the first time you authenticate; that is expected for this local client session.

## Next seam

`handle_new_message` is the single event sink. A later iteration can convert the event to a durable message schema and publish it to Kafka there, leaving authentication, channel resolution, and subscription handling intact.

## World Monitor source sampler

World Monitor is useful as a source-discovery reference, but its Telegram endpoint is a proxy to a privately configured relay rather than a documented public ingestion API. This project therefore does not scrape it or attempt to bypass its access controls.

Instead, `worldmonitor_source_listener.py` samples two original public providers that World Monitor lists among its sources: USGS earthquakes and NASA EONET natural events. It prints newline-delimited JSON: a current snapshot on startup, then only newly observed record IDs on later polls.

```bash
python worldmonitor_source_listener.py
```

Optional `.env` settings (these have safe defaults):

```dotenv
WORLD_MONITOR_SOURCES=usgs,nasa_eonet
WORLD_MONITOR_POLL_SECONDS=60
```

This is polling, not a push stream. The USGS feed updates every minute; use a poll interval of at least 60 seconds in normal use. The compact `Signal` envelope is intentionally separate from the Telegram handler so each later source can become an independent adapter before a common Kafka publisher is introduced.

## YouTube transcript helper

`youtube_transcript.py` prints the available captions for one public video. It is a one-off retrieval helper, not a live source.

```bash
pip install -r requirements.txt
python youtube_transcript.py "https://www.youtube.com/watch?v=MM-Qhlxf1pM"
```

It requests English captions by default. Pass preferred language codes if needed:

```bash
python youtube_transcript.py "https://youtu.be/VIDEO_ID" --languages ar en
```

The video must have an accessible transcript; age-restricted, unavailable, or transcript-less videos will fail. Use this only for content you are entitled to retrieve and in accordance with YouTube's terms.
