# Hermes MAX Messenger Adapter

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A gateway platform adapter for [Hermes Agent](https://hermes-agent.nousresearch.com) that connects to the **MAX (max.ru)** Bot API.

Enables Hermes Agent to send and receive messages, files, images, and interactive buttons through MAX — a popular messenger in Russia.

## Features

- ✅ **Two-way messaging** — send and receive text messages
- ✅ **File transfer** — send and receive documents, images, video, audio
- ✅ **Media upload** — automatic file upload via MAX API (`/uploads`)
- ✅ **Markdown formatting** — bold, italic, code blocks, links
- ✅ **Inline keyboards** — clarify buttons, exec approval (Approve/Deny)
- ✅ **Long Polling** — no public URL required for development
- ✅ **Webhook mode** — HTTPS webhooks for production
- ✅ **Reply-to support** — reply to specific messages with `link`
- ✅ **Cron delivery** — standalone sender for scheduled tasks
- ✅ **Message splitting** — auto-splits messages over 4000 chars
- ✅ **SSL for Минцифры** — custom CA bundle for Russian certificates
- ✅ **Typing indicator** — `send_typing` support

## Requirements

- Python 3.10+
- `aiohttp` (Hermes dependency)
- Hermes Agent installed and configured
- Registered bot on [MAX for Developers](https://dev.max.ru)

## Installation

### 1. Clone to Hermes plugins

```bash
git clone https://github.com/Sen9405/hermes-max-adapter.git ~/.hermes/plugins/platforms/max
```

### 2. Configure in `~/.hermes/config.yaml`

```yaml
gateway:
  platforms:
    max:
      enabled: true
      extra:
        token: "<your_bot_token>"
        webhook_url: "https://..."  # optional: HTTPS for production
        poll_interval: 5            # optional: long polling interval
        allowed_users: []           # empty = allow all
        home_channel: ""            # for cron delivery
```

Or via environment variables:

```bash
export MAX_TOKEN="<your_bot_token>"
export MAX_WEBHOOK_URL="https://..."  # optional
export MAX_POLL_INTERVAL=5            # optional
export MAX_ALLOW_ALL_USERS=true       # dev only
```

### 3. Enable and restart

```bash
hermes plugins enable max
hermes gateway restart
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MAX_TOKEN` | ✅ Yes | Bot token from MAX → Чат-боты → Расширенные настройки |
| `MAX_WEBHOOK_URL` | ❌ No | Public HTTPS URL for webhook (required for production) |
| `MAX_POLL_INTERVAL` | ❌ No | Long polling interval in seconds (default: 5) |
| `MAX_ALLOWED_USERS` | ❌ No | Comma-separated user IDs allowed to interact |
| `MAX_ALLOW_ALL_USERS` | ❌ No | Allow all users (default: false, dev only) |
| `MAX_HOME_CHANNEL` | ❌ No | Chat ID for cron job delivery |

## SSL Certificates

MAX API (`platform-api2.max.ru`) uses certificates issued by the Russian Ministry of Digital Development (Минцифры). The adapter supports loading a custom CA bundle:

```bash
# Place CA certificates in ~/.hermes/ca-bundle.pem
# Or set HERMES_CA_BUNDLE env var to a custom path
```

## API Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `POST /me` | Bot authentication |
| `POST /messages` | Send messages with optional attachments |
| `POST /uploads` | Upload files (2-step: get URL → upload) |
| `GET /updates` | Long polling for incoming events |
| `POST /subscriptions` | Webhook subscription management |
| `POST /chats/{id}/actions` | Typing indicator |
| `GET /chats/{id}` | Chat info |

## Project Structure

```
hermes-max-adapter/
├── hermes_plugin_max/
│   ├── __init__.py          # Plugin entry point
│   ├── adapter.py           # Main adapter implementation
│   └── plugin.yaml          # Plugin metadata
├── README.md
├── LICENSE
└── .gitignore
```

## License

MIT License — see [LICENSE](LICENSE)
