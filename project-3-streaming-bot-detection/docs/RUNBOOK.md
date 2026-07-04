# Streaming Bot Detection Runbook

Use the project as a three-state system.

## Table Of Contents

- [State Diagram](#state-diagram)
- [Quick Start](#quick-start)
- [Useful URLs](#useful-urls)

## State Diagram

```mermaid
flowchart LR
    A["Start"]
    B["Platform Ready"]
    C["Data Present"]

    A -->|setup_all_infra.sh| B
    B -->|replay_data.py| C
    C -->|reset_data.sh| B
    B -->|reset_all_infra.sh| A
```

## Quick Start

Start from the project root:

```bash
cd /Users/rezwanhoppe-islam/data-engineering-portfolio/project-3-streaming-bot-detection
```

Ask the project where you are:

```bash
./infra/scripts/state.sh
```

The script prints:

```text
State: Start | Platform Ready | Data Present
Next valid action: ...
Why: ...
```

Follow the `Next valid action` returned by the script. After that action finishes, run `./infra/scripts/state.sh` again.

## Useful URLs

```text
Redpanda Console: http://localhost:8080
Flink Web UI:     http://localhost:8081
Grafana:          http://localhost:3000
```

Grafana login:

```text
Username: admin
Password: admin
Dashboard: Streaming Bot Detection Live
```
