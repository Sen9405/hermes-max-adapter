"""
MAX Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to the MAX (max.ru) Bot API
and relays messages to/from the Hermes agent. Uses webhooks for production
and long polling as fallback.

Configuration in config.yaml:

    gateway:
      platforms:
        max:
          enabled: true
          extra:
            token: "<your_bot_token>"
            webhook_url: "https://..."    # optional: HTTPS for production
            poll_interval: 5              # optional: long polling interval
            allowed_users: []             # empty = allow all
            home_channel: ""              # for cron delivery

Or via environment variables (overrides config.yaml):
    MAX_TOKEN, MAX_WEBHOOK_URL, MAX_POLL_INTERVAL,
    MAX_ALLOWED_USERS, MAX_ALLOW_ALL_USERS, MAX_HOME_CHANNEL
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import ssl
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import aiohttp

logger = logging.getLogger(__name__)

# Hermes CA bundle path — includes Минцифры certificates for MAX API
_HERMES_CA_BUNDLE = os.path.join(
    os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes")),
    "ca-bundle.pem",
)

# ---------------------------------------------------------------------------
# Lazy imports from Hermes core
# ---------------------------------------------------------------------------

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_BASE = "https://platform-api2.max.ru"
MAX_MESSAGE_LENGTH = 3990
DEFAULT_POLL_INTERVAL = 5
MAX_POLL_INTERVAL = 60
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0
WEBHOOK_TIMEOUT = 30

# Update types we subscribe to
DEFAULT_UPDATE_TYPES = [
    "message_created",
    "message_callback",
    "bot_started",
    "bot_added",
    "message_edited",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": token,
        "Content-Type": "application/json",
    }


def _create_ssl_context() -> ssl.SSLContext:
    """Create SSL context with Минцифры CA bundle if available.

    The MAX API (platform-api2.max.ru) uses certificates issued by
    the Russian Ministry of Digital Development (Минцифры).  The
    default system CA bundle on most Linux distros doesn't include
    these, so we supplement with a custom bundle at
    ~/.hermes/ca-bundle.pem containing:

        - Russian Trusted Root CA  (root)
        - Russian Trusted Sub CA   (intermediate, signs *.max.ru)
    """
    ctx = ssl.create_default_context()
    ca_bundle = os.environ.get(
        "HERMES_CA_BUNDLE",
        _HERMES_CA_BUNDLE,
    )
    if os.path.isfile(ca_bundle):
        try:
            ctx.load_verify_locations(cafile=ca_bundle)
            logger.debug("Loaded CA bundle from %s", ca_bundle)
        except Exception as e:
            logger.warning("Failed to load CA bundle %s: %s", ca_bundle, e)
    return ctx


def _validate_config(config) -> bool:
    """Check if the platform config has enough info to connect."""
    token = os.getenv("MAX_TOKEN") or getattr(config, "token", "") or ""
    if not token and hasattr(config, "extra"):
        token = config.extra.get("token", "")
    return bool(token.strip())


def check_requirements() -> bool:
    """Check if MAX is configured with a token."""
    token = os.getenv("MAX_TOKEN", "")
    return bool(token.strip())


# ---------------------------------------------------------------------------
# MAX Adapter
# ---------------------------------------------------------------------------


class MAXAdapter(BasePlatformAdapter):
    """MAX Bot API adapter implementing BasePlatformAdapter interface."""
    supports_code_blocks: bool = True
    splits_long_messages: bool = True

    def __init__(self, config):
        from gateway.config import Platform as Plat
        try:
            platform = Plat("max")
        except Exception:
            platform = None
        super().__init__(config, platform)

        # Parse config
        extra = getattr(config, "extra", {}) or {}
        self._token = (
            os.getenv("MAX_TOKEN")
            or getattr(config, "token", "")
            or extra.get("token", "")
        )
        self._webhook_url = (
            os.getenv("MAX_WEBHOOK_URL") or extra.get("webhook_url", "") or ""
        )
        self._poll_interval = float(
            os.getenv("MAX_POLL_INTERVAL") or extra.get("poll_interval", DEFAULT_POLL_INTERVAL)
        )
        self._poll_interval = max(1, min(self._poll_interval, MAX_POLL_INTERVAL))

        # State
        self._api_base = API_BASE
        self._session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._bot_info: Optional[Dict[str, Any]] = None
        self._last_update_id: Optional[int] = None
        self._webhook_server: Optional[Any] = None
        self._chat_to_user: Dict[str, str] = {}  # chat_id → user_id mapping

        logger.info(
            "MAXAdapter initialized (webhook=%s, poll=%ss)",
            bool(self._webhook_url),
            self._poll_interval,
        )

    # ------------------------------------------------------------------
    # BasePlatformAdapter required methods
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to MAX and start listening for updates."""
        logger.info("Connecting MAX adapter...")

        if not self._token:
            logger.error("No MAX token configured")
            return False

        # Create SSL context with custom CA bundle for Минцифры certificates
        ssl_ctx = _create_ssl_context()

        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        self._session = aiohttp.ClientSession(
            connector=connector,
            headers=_make_headers(self._token),
            timeout=aiohttp.ClientTimeout(total=30),
        )

        # Verify token by calling /me
        bot_info = await self._api_request("GET", "/me")
        if not bot_info:
            logger.error("Failed to verify MAX token — /me returned nothing")
            await self._cleanup_session()
            return False

        self._bot_info = bot_info
        logger.info(
            "MAX bot authenticated: %s (id=%s)",
            bot_info.get("name", "?"),
            bot_info.get("user_id", "?"),
        )

        self._running = True

        # Start update listener
        if self._webhook_url:
            task = asyncio.create_task(self._webhook_listener(), name="max-webhook")
        else:
            task = asyncio.create_task(self._long_poll_loop(), name="max-poll")
        self._tasks.append(task)

        logger.info("MAX adapter connected successfully")
        return True

    async def disconnect(self):
        """Stop listeners and clean up."""
        logger.info("Disconnecting MAX adapter...")
        self._running = False

        # Cancel tasks
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Unregister webhook if we registered one
        if self._webhook_url:
            try:
                await self._api_request("DELETE", "/subscriptions")
            except Exception:
                pass

        await self._cleanup_session()
        logger.info("MAX adapter disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Any = None,
        **kwargs,
    ) -> SendResult:
        """Send a text message to a chat.

        Splits messages longer than MAX_MESSAGE_LENGTH into multiple chunks.
        Tries markdown formatting first, falls back to plain text on error.
        """
        text = content or ""
        if not text and not kwargs.get("attachments"):
            return SendResult(success=False, error="Empty message")

        # Split long text into multiple messages
        if len(text) > MAX_MESSAGE_LENGTH:
            chunks = [
                text[i : i + MAX_MESSAGE_LENGTH]
                for i in range(0, len(text), MAX_MESSAGE_LENGTH)
            ]
            first_result: Optional[SendResult] = None
            for i, chunk in enumerate(chunks):
                # Only first chunk gets reply_to; subsequent are just follow-ups
                chunk_reply = reply_to if i == 0 else None
                result = await self._send_single(
                    chat_id, chunk, reply_to=chunk_reply,
                    metadata=metadata, **kwargs,
                )
                if i == 0:
                    first_result = result
            return first_result or SendResult(success=False, error="All chunks failed")

        return await self._send_single(
            chat_id, text, reply_to=reply_to, metadata=metadata, **kwargs,
        )

    async def _send_single(
        self,
        chat_id: str,
        text: str,
        reply_to: Optional[str] = None,
        metadata: Any = None,
        **kwargs,
    ) -> SendResult:
        """Send a single message chunk with markdown → plain-text fallback.

        Handles MEDIA:<path> tags by extracting them, uploading the file,
        and sending it as an attachment alongside the cleaned text.
        """
        # Extract MEDIA:<path> tags and upload files
        media_paths = re.findall(r"MEDIA:\s*(\S+)", text)
        media_attachments = []
        for mp in media_paths:
            mp = os.path.expanduser(mp.strip().strip("`\"'"))
            if os.path.isfile(mp):
                ext = os.path.splitext(mp)[1].lower()
                # Determine media type by extension
                _IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
                _VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
                _AUDIO_EXTS = {".mp3", ".wav", ".ogg", ".opus", ".m4a", ".flac"}
                if ext in _IMAGE_EXTS:
                    mtype = "image"
                elif ext in _VIDEO_EXTS:
                    mtype = "video"
                elif ext in _AUDIO_EXTS:
                    mtype = "audio"
                else:
                    mtype = "file"
                logger.info(
                    "[Max] Uploading MEDIA: %s (type=%s)", mp, mtype
                )
                result = await self._upload_file(mp, mtype)
                if result and "file_id" in result:
                    payload = {"file_id": result["file_id"]}
                    if result.get("token"):
                        payload["token"] = result["token"]
                    media_attachments.append({
                        "type": mtype,
                        "payload": payload,
                    })
                else:
                    logger.warning(
                        "[Max] Failed to upload MEDIA file: %s", mp
                    )

        # Clean MEDIA tags from text
        text = re.sub(r"MEDIA:\s*\S+", "", text).strip()
        # Collapse multiple newlines
        text = re.sub(r"\n{3,}", "\n\n", text)

        target_user = self._chat_to_user.get(chat_id, chat_id)
        params = {"user_id": target_user}

        # Try with markdown formatting first (MAX renders it nicely)
        body: Dict[str, Any] = {"text": text, "format": "markdown"}
        attachments = kwargs.get("attachments", [])
        if media_attachments:
            attachments = media_attachments + attachments
        if attachments:
            body["attachments"] = attachments

        # Add reply_to as link (MAX API: NewMessageBody.link)
        # Requires type field since ~June 2026: {"type": "reply", "message_id": "mid.xxx"}
        if reply_to:
            body["link"] = {"type": "reply", "mid": reply_to}

        result = await self._api_request("POST", "/messages", json=body, params=params)
        if result and "message" in result:
            msg = result["message"]
            return SendResult(
                success=True,
                message_id=str(msg.get("message_id", "")),
            )

        # Fallback: retry without markdown (handles split mid-formatting)
        body.pop("format", None)
        result = await self._api_request("POST", "/messages", json=body, params=params)
        if result and "message" in result:
            msg = result["message"]
            return SendResult(
                success=True,
                message_id=str(msg.get("message_id", "")),
            )
        return SendResult(success=False, error=str(result))

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send typing indicator via MAX API (POST /chats/{chatId}/actions)."""
        # chat_id here is the raw chat ID from the message recipient
        try:
            await self._api_request(
                "POST",
                f"/chats/{chat_id}/actions",
                json={"action": "typing_on"},
            )
        except Exception as e:
            logger.debug("send_typing failed for chat %s: %s", chat_id, e)

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send an image from URL. Uses upload first, then sends."""
        return await self._send_media(chat_id, image_url, caption, media_type="image", reply_to=kwargs.get("reply_to"))

    async def get_chat_info(self, chat_id: str) -> dict:
        """Get chat info for a given chat ID."""
        result = await self._api_request("GET", f"/chats/{chat_id}")
        if result:
            return {
                "name": result.get("name", ""),
                "type": result.get("type", "chat"),
                "chat_id": chat_id,
            }
        return {"name": "", "type": "chat", "chat_id": chat_id}

    # ------------------------------------------------------------------
    # Optional media sends
    # ------------------------------------------------------------------

    async def send_document(
        self,
        chat_id: str,
        file_path: str = "",
        path: str = "",
        caption: Optional[str] = None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        path = file_path or path
        logger.info("[Max] send_document called: chat=%s path=%s caption=%s", chat_id, path, caption)
        return await self._send_media(chat_id, path, caption, media_type="file", reply_to=kwargs.get("reply_to"))

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str = "",
        path: str = "",
        metadata=None,
        **kwargs,
    ) -> SendResult:
        path = audio_path or path
        return await self._send_media(chat_id, path, None, media_type="audio", reply_to=kwargs.get("reply_to"))

    async def send_video(
        self,
        chat_id: str,
        video_path: str = "",
        path: str = "",
        caption: Optional[str] = None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        path = video_path or path
        return await self._send_media(chat_id, path, caption, media_type="video", reply_to=kwargs.get("reply_to"))

    async def send_image_file(
        self,
        chat_id: str,
        path: str,
        caption: Optional[str] = None,
        metadata=None,
        **kwargs,
    ) -> SendResult:
        return await self._send_media(chat_id, path, caption, media_type="image", reply_to=kwargs.get("reply_to"))

    # ------------------------------------------------------------------
    # Interactive buttons (clarify, exec approval)
    # ------------------------------------------------------------------

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: List[str],
        clarify_id: str,
        session_key: str,
        **kwargs,
    ) -> SendResult:
        """Render a clarify multi-choice as inline keyboard buttons."""
        buttons = []
        for idx, choice in enumerate(choices):
            buttons.append([
                {
                    "type": "callback",
                    "text": choice[:40],
                    "payload": f"cl:{clarify_id}:{idx}",
                }
            ])

        attachments = [{
            "type": "inline_keyboard",
            "payload": {"buttons": buttons},
        }]

        return await self.send(
            chat_id,
            text=question,
            attachments=attachments,
        )

    async def send_exec_approval(
        self,
        chat_id: str,
        command: str,
        session_key: str,
        description: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Render exec approval as Approve/Deny buttons."""
        text = f"⚠️ Execute command?\n\n`{command}`"
        if description:
            text += f"\n\n{description}"

        # Extract approval_id from kwargs or generate from session
        approval_id = kwargs.get("approval_id", session_key)

        buttons = [
            [
                {
                    "type": "callback",
                    "text": "✅ Approve",
                    "payload": f"appr:{approval_id}:approve",
                },
                {
                    "type": "callback",
                    "text": "❌ Deny",
                    "payload": f"appr:{approval_id}:deny",
                },
            ]
        ]

        return await self.send(
            chat_id,
            text=text,
            attachments=[{
                "type": "inline_keyboard",
                "payload": {"buttons": buttons},
            }],
        )

    # ------------------------------------------------------------------
    # Internal: media upload + send
    # ------------------------------------------------------------------

    async def _send_media(
        self,
        chat_id: str,
        media_source: str,
        caption: Optional[str],
        media_type: str,
        reply_to: Optional[str] = None,
    ) -> SendResult:
        """Upload a media file and send it as a message.

        Supports reply_to via MAX API NewMessageBody.link.
        """
        try:
            # Upload the file
            upload_result = await self._upload_file(media_source, media_type)
            if not upload_result or "file_id" not in upload_result:
                return SendResult(success=False, error="Upload failed")

            file_id = upload_result["file_id"]
            token = upload_result.get("token", "")

            # Send as message with attachment
            caption_text = (caption or "")[:MAX_MESSAGE_LENGTH]
            body: Dict[str, Any] = {
                "text": caption_text,
                "format": "markdown",
            }

            # Attachment type mapping
            type_map = {
                "image": "image",
                "video": "video",
                "audio": "audio",
                "file": "file",
            }
            attach_type = type_map.get(media_type, "file")

            # Payload: MAX API requires both file_id (int) and token (str)
            payload: Dict[str, Any] = {"file_id": file_id}
            if token:
                payload["token"] = token

            body["attachments"] = [{
                "type": attach_type,
                "payload": payload,
            }]

            # Add reply_to as link (MAX API: NewMessageBody.link)
            # Requires type field since ~June 2026: {"type": "reply", "message_id": "mid.xxx"}
            if reply_to:
                body["link"] = {"type": "reply", "mid": reply_to}

            result = await self._api_request("POST", "/messages", json=body, params={"user_id": self._chat_to_user.get(chat_id, chat_id)})
            if result and "message" in result:
                msg = result["message"]
                return SendResult(
                    success=True,
                    message_id=str(msg.get("message_id", "")),
                )
            return SendResult(success=False, error=str(result))

        except Exception as e:
            logger.error("Media send failed: %s", e)
            return SendResult(success=False, error=str(e))

    async def _upload_file(
        self,
        source: str,
        media_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Upload a file to MAX via POST /uploads → upload URL → POST file.

        Two-step process per MAX API docs at dev.max.ru/docs:
          1. POST /uploads?type={type} → receives {"url": "...", ...}
          2. POST file_data to the received url with multipart field `data`
             → receives {"fileId": N, "token": "..."}

        Returns dict with "file_id" (int) and "token" (str).
        """
        if not self._session:
            return None

        # Determine file data
        if source.startswith(("http://", "https://")):
            async with self._session.get(source) as resp:
                if resp.status != 200:
                    logger.error("Failed to download media from %s: %d", source, resp.status)
                    return None
                file_data = await resp.read()
                content_type = resp.headers.get("Content-Type", "application/octet-stream")
                filename = source.rsplit("/", 1)[-1] or "file"
        else:
            with open(source, "rb") as f:
                file_data = f.read()
            filename = os.path.basename(source)
            content_type = "application/octet-stream"

        # Step 1: Get upload URL
        type_map_for_url = {
            "image": "image",
            "video": "video",
            "audio": "audio",
            "file": "file",
        }
        upload_type = type_map_for_url.get(media_type, "file")
        get_url_result = await self._api_request(
            "POST", f"/uploads?type={upload_type}"
        )
        if not get_url_result or "url" not in get_url_result:
            logger.error(
                "Failed to get upload URL: %s", str(get_url_result)[:200]
            )
            return None

        target_url = get_url_result["url"]

        # Step 2: Upload file data to the received URL
        # Note: upload URL (fu.oneme.ru) does NOT need Authorization header.
        # We use a raw POST with multipart via aiohttp, mirroring:
        #   curl -F "data=@file;filename=name;type=mime" $UPLOAD_URL
        ssl_ctx = _create_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=60),
        ) as upload_session:
            # Manually build multipart body to match curl's -F format
            boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
            body_bytes = []
            body_bytes.append(f"--{boundary}\r\n".encode())
            body_bytes.append(
                f'Content-Disposition: form-data; name="data"; filename="{filename}"\r\n'.encode()
            )
            body_bytes.append(f"Content-Type: {content_type}\r\n\r\n".encode())
            body_bytes.append(file_data)
            body_bytes.append(f"\r\n--{boundary}--\r\n".encode())
            payload = b"".join(body_bytes)

            headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}
            async with upload_session.post(
                target_url, data=payload, headers=headers
            ) as resp:
                if resp.status not in (200, 201):
                    text = await resp.text()
                    logger.error("File upload failed: %d %s", resp.status, text[:200])
                    return None
                upload_result = await resp.json()

        file_id = upload_result.get("fileId")
        token = upload_result.get("token")
        if not file_id or not token:
            logger.error(
                "Upload response missing fileId or token (fileId=%s, has_token=%s)",
                bool(file_id), bool(token),
            )
            return None

        return {"file_id": file_id, "token": token}

    # ------------------------------------------------------------------
    # Update listeners
    # ------------------------------------------------------------------

    async def _webhook_listener(self):
        """Register webhook subscription and listen for incoming events.

        MAX sends events to our webhook URL. We need only the subscription
        registration; the actual HTTP server runs in the gateway.
        """
        try:
            # Register webhook
            body = {
                "url": self._webhook_url,
                "update_types": DEFAULT_UPDATE_TYPES,
            }
            result = await self._api_request("POST", "/subscriptions", json=body)
            if result:
                logger.info("Webhook registered at %s", self._webhook_url)
            else:
                logger.warning("Webhook registration returned no data")

            # Keep alive: check subscription periodically
            while self._running:
                await asyncio.sleep(300)  # check every 5 minutes
                try:
                    await self._api_request("GET", "/subscriptions")
                except Exception as e:
                    logger.warning("Webhook health check failed: %s", e)
                    # Re-register
                    try:
                        await self._api_request("POST", "/subscriptions", json=body)
                        logger.info("Webhook re-registered")
                    except Exception:
                        pass

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("Webhook listener error: %s", e)

    async def _long_poll_loop(self):
        """Long polling loop for updates when no webhook URL is configured."""
        delay = RECONNECT_BASE_DELAY
        # Use a dedicated session with longer timeout for long polling
        ssl_ctx = _create_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        async with aiohttp.ClientSession(
            connector=connector,
            headers=_make_headers(self._token),
            timeout=aiohttp.ClientTimeout(total=120),
        ) as poll_session:
            while self._running:
                try:
                    params = {}
                    if self._last_update_id is not None:
                        params["marker"] = self._last_update_id
                    params["timeout"] = 60  # Long poll timeout in seconds

                    url = urljoin(self._api_base, "/updates")
                    async with poll_session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                        else:
                            data = None

                    if data is None:
                        await asyncio.sleep(self._poll_interval)
                        delay = RECONNECT_BASE_DELAY
                        continue

                    # Extract marker and updates from response
                    marker = data.get("marker") if isinstance(data, dict) else None
                    updates_list = []
                    if isinstance(data, list):
                        updates_list = data
                    elif isinstance(data, dict):
                        updates_list = data.get("updates", data.get("events", []))

                    if marker is not None:
                        self._last_update_id = marker

                    for update in updates_list:
                        if not isinstance(update, dict):
                            continue
                        await self._process_update(update)

                    delay = RECONNECT_BASE_DELAY

                except asyncio.CancelledError:
                    break
                except asyncio.TimeoutError:
                    # Timeout is normal for long polling - just retry immediately
                    delay = RECONNECT_BASE_DELAY
                except aiohttp.ClientError as e:
                    logger.warning("Long poll HTTP error: %s (retry in %.1fs)", e, delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, RECONNECT_MAX_DELAY)
                except Exception as e:
                    logger.error("Long poll error: %s (retry in %.1fs)", e, delay)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, RECONNECT_MAX_DELAY)

    # ------------------------------------------------------------------
    # Update processing
    # ------------------------------------------------------------------

    async def _process_update(self, update: Dict[str, Any]):
        """Process a single update from MAX long polling or webhook."""
        msg = update.get("message")
        if not msg:
            logger.debug("MAX update without message field: %s", str(update)[:200])
            return

        timestamp = msg.get("timestamp", int(time.time() * 1000))
        recipient = msg.get("recipient", {})
        sender = msg.get("sender", {})
        body = msg.get("body", {})

        chat_id = recipient.get("chat_id")
        user_id = sender.get("user_id")
        user_name = sender.get("name") or sender.get("first_name", "")
        msg_text = body.get("text", "")

        # Process incoming attachments (files/images from user)
        attachments = body.get("attachments", [])
        if attachments:
            file_parts = []
            for att in attachments:
                filename = att.get("filename", "file")
                payload = att.get("payload", {})
                file_url = payload.get("url", "")
                att_type = att.get("type", "file")
                file_parts.append(f"[{att_type}: {filename}]")
                # Download file to ~/.hermes/uploads/
                if file_url:
                    try:
                        save_dir = os.path.join(
                            os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes")),
                            "uploads",
                        )
                        os.makedirs(save_dir, exist_ok=True)
                        dest = os.path.join(save_dir, os.path.basename(filename))
                        ssl_ctx = _create_ssl_context()
                        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
                        async with aiohttp.ClientSession(
                            connector=connector,
                            timeout=aiohttp.ClientTimeout(total=30),
                        ) as dl_session:
                            async with dl_session.get(file_url) as resp:
                                if resp.status == 200:
                                    data = await resp.read()
                                    with open(dest, "wb") as f:
                                        f.write(data)
                                    logger.info(
                                        "[Max] Saved attachment %s (%d bytes)", dest, len(data)
                                    )
                    except Exception as e:
                        logger.warning("[Max] Failed to download %s: %s", filename, e)
            if file_parts:
                attach_text = " 📎 " + ", ".join(file_parts)
                msg_text = (msg_text + "\n" + attach_text) if msg_text else attach_text

        # Rewrite !command → /command for MAX clients that intercept / commands
        if msg_text.startswith("!"):
            msg_text = "/" + msg_text[1:]
        message_id = body.get("mid", "")

        if not chat_id or not user_id:
            logger.debug("MAX update missing chat_id or user_id")
            return

        # Store chat_id → user_id mapping for sending replies
        self._chat_to_user[str(chat_id)] = str(user_id)

        # Build session source
        source = self.build_source(
            chat_id=str(chat_id),
            user_id=str(user_id),
            user_name=user_name,
        )

        # Build message event
        event = MessageEvent(
            text=msg_text or "",
            message_id=str(message_id),
            message_type=MessageType.TEXT,
            source=source,
        )
        await self.handle_message(event)

    # ------------------------------------------------------------------
    # API request helper
    # ------------------------------------------------------------------

    async def _api_request(
        self,
        method: str,
        path: str,
        json: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Any:
        """Make an API request to platform-api2.max.ru."""
        if not self._session:
            logger.error("No HTTP session available")
            return None

        url = urljoin(self._api_base, path)

        try:
            async with self._session.request(
                method, url, json=json, params=params
            ) as resp:
                if resp.status == 429:
                    logger.warning("Rate limited on %s %s", method, path)
                    return None
                if resp.status == 401:
                    logger.error("Authentication failed — check MAX_TOKEN")
                    return None
                if resp.status >= 500:
                    text = await resp.text()
                    logger.error("Server error %d on %s %s: %s",
                                 resp.status, method, path, text[:200])
                    return None

                if resp.status in (200, 201):
                    return await resp.json()
                if resp.status == 204:
                    return {}

                # Error
                text = await resp.text()
                logger.warning("API error %d on %s %s: %s",
                               resp.status, method, path, text[:500])
                return None

        except asyncio.TimeoutError:
            logger.warning("Timeout on %s %s", method, path)
            return None
        except aiohttp.ClientError as e:
            logger.warning("HTTP error on %s %s: %s", method, path, e)
            return None

    async def _cleanup_session(self):
        """Clean up the aiohttp session."""
        if self._session:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        """Check if the adapter is running and connected."""
        return self._running and self._session is not None and not self._session.closed


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def validate_config(config) -> bool:
    """Validate that the platform config has enough info."""
    return _validate_config(config)


# ---------------------------------------------------------------------------
# Webhook inbound handler
# ---------------------------------------------------------------------------

# When MAX sends a webhook event, the gateway's HTTP server routes
# POST requests to this handler. We use the active adapter instance
# to process the event.


async def handle_webhook_event(adapter: MAXAdapter, body: bytes):
    """Handle an incoming webhook event from MAX."""
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON in webhook body")
        return {"ok": False, "error": "invalid_json"}

    # MAX may send a single update or an array
    updates = data if isinstance(data, list) else [data]
    for update in updates:
        await adapter._process_update(update)

    return {"ok": True}


# ---------------------------------------------------------------------------
# Standalone sender for cron delivery
# ---------------------------------------------------------------------------


async def _standalone_send(
    chat_id: str,
    text: str,
    api_key: str = "",
    **kwargs,
) -> Dict[str, Any]:
    """Send a message from outside the gateway (cron jobs, send_message tool).

    Handles MEDIA:<path> tags by uploading the file and sending as attachment.
    """
    token = api_key or os.getenv("MAX_TOKEN", "")
    if not token:
        return {"error": "No MAX token configured"}

    # Extract MEDIA:<path> tags
    media_attachments = []
    media_paths = re.findall(r"MEDIA:\s*(\S+)", text)
    for mp in media_paths:
        mp = os.path.expanduser(mp.strip().strip("`\"'"))
        if os.path.isfile(mp):
            try:
                # Upload via curl subprocess (aiohttp not available at module level)
                import subprocess as _sp, json as _json
                # Step 1: get upload URL
                ssl_ctx = _create_ssl_context()
                connector = aiohttp.TCPConnector(ssl=ssl_ctx)
                async with aiohttp.ClientSession(
                    connector=connector,
                    headers=_make_headers(token),
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as session:
                    # Step 1: get upload URL
                    get_url_r = await session.post(
                        f"{API_BASE}/uploads?type=file"
                    )
                    if get_url_r.status != 200:
                        continue
                    get_url_data = await get_url_r.json()
                    upload_url = get_url_data.get("url", "")
                    if not upload_url:
                        continue

                    # Step 2: upload file with fresh session (no auth)
                    ssl_ctx2 = _create_ssl_context()
                    connector2 = aiohttp.TCPConnector(ssl=ssl_ctx2)
                    async with aiohttp.ClientSession(
                        connector=connector2,
                        timeout=aiohttp.ClientTimeout(total=60),
                    ) as up_session:
                        with open(mp, "rb") as _f:
                            file_data = _f.read()
                        filename = os.path.basename(mp)
                        boundary = "----FormBoundary7MA4YWxk"
                        body_parts = [
                            f"--{boundary}\r\n".encode(),
                            f'Content-Disposition: form-data; name="data"; filename="{filename}"\r\n'.encode(),
                            b"Content-Type: application/octet-stream\r\n\r\n",
                            file_data,
                            f"\r\n--{boundary}--\r\n".encode(),
                        ]
                        payload = b"".join(body_parts)
                        up_r = await up_session.post(
                            upload_url,
                            data=payload,
                            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                        )
                        if up_r.status in (200, 201):
                            up_data = await up_r.json()
                            fid = up_data.get("fileId")
                            tok = up_data.get("token")
                            if fid and tok:
                                media_attachments.append({
                                    "type": "file",
                                    "payload": {"file_id": fid, "token": tok},
                                })
            except Exception as e:
                logger.warning("_standalone_send MEDIA upload failed: %s", e)

    # Clean MEDIA tags from text
    text = re.sub(r"MEDIA:\s*\S+", "", text).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)

    headers = _make_headers(token)
    body: Dict[str, Any] = {
        "text": text or "",
    }
    if media_attachments:
        body["attachments"] = media_attachments
    # chat_id as query param per MAX API docs
    params = {"chat_id": int(chat_id) if chat_id.isdigit() else chat_id}

    # Add reply_to as link (MAX API: NewMessageBody.link)
    reply_to = kwargs.get("reply_to")
    if reply_to:
        body["link"] = {"message_id": reply_to}

    try:
        ssl_ctx = _create_ssl_context()
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        async with aiohttp.ClientSession(
            connector=connector,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as session:
            url = urljoin(API_BASE, "/messages")
            async with session.post(url, json=body, params=params) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return {"ok": True, "message_id": str(result.get("message", {}).get("message_id", ""))}
                else:
                    error_text = await resp.text()
                    return {"error": f"HTTP {resp.status}: {error_text[:200]}"}
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# Env-driven auto-configuration
# ---------------------------------------------------------------------------


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Seed PlatformConfig.extra from env vars for env-only setups."""
    token = os.getenv("MAX_TOKEN", "").strip()
    if not token:
        return None

    extra: Dict[str, Any] = {"token": token}

    webhook = os.getenv("MAX_WEBHOOK_URL", "").strip()
    if webhook:
        extra["webhook_url"] = webhook

    poll = os.getenv("MAX_POLL_INTERVAL", "").strip()
    if poll:
        try:
            extra["poll_interval"] = float(poll)
        except ValueError:
            pass

    # Home channel
    home = os.getenv("MAX_HOME_CHANNEL", "").strip()
    home_channel = None
    if home:
        home_channel = {"chat_id": home}

    return {"extra": extra, "home_channel": home_channel}


# ---------------------------------------------------------------------------
# Interactive setup wizard
# ---------------------------------------------------------------------------


async def interactive_setup(config) -> bool:
    """Run interactive setup for MAX."""
    # This is a minimal setup; the standard env-var flow handles most.
    token = input("MAX bot token: ").strip()
    if not token:
        print("Token is required")
        return False

    os.environ["MAX_TOKEN"] = token

    webhook = input("Webhook URL (HTTPS, press Enter to skip): ").strip()
    if webhook:
        os.environ["MAX_WEBHOOK_URL"] = webhook

    print("MAX configured. Restart the gateway to apply.")
    return True


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="max",
        label="MAX",
        adapter_factory=lambda cfg: MAXAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=lambda: False,  # checked via the adapter instance
        required_env=["MAX_TOKEN"],
        install_hint="No extra packages needed (aiohttp is a Hermes dependency)",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MAX_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="MAX_ALLOWED_USERS",
        allow_all_env="MAX_ALLOW_ALL_USERS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="💬",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "Вы общаетесь через MAX (max.ru). "
            "MAX поддерживает Markdown-форматирование: **жирный**, *курсив*, "
            "`код`, ~~зачёркнутый~~, [ссылки](url). "
            "Максимальная длина сообщения: 4000 символов. "
            "Поддерживаются inline-клавиатуры и медиавложения. "
            "Работают slash-команды: /help, /new, /reset, /stop и другие."
        ),
    )
