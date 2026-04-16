import html
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser

STATE_FILE = Path(".rss_state.json")

RSS_FEED_URL = os.getenv("RSS_FEED_URL", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

MAX_POSTS_PER_RUN = max(1, int(os.getenv("MAX_POSTS_PER_RUN", "5")))
FIRST_RUN_MODE = os.getenv("FIRST_RUN_MODE", "seed").strip().lower()
REQUEST_TIMEOUT = max(5, int(os.getenv("REQUEST_TIMEOUT", "30")))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "rss-to-discord-github-actions/2.0 (+https://github.com/actions)",
).strip()

WEBHOOK_USERNAME = os.getenv("WEBHOOK_USERNAME", "RSS Feed").strip()
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "").strip()
MENTION_TEXT = os.getenv("MENTION_TEXT", "").strip()

EMBED_COLOR = int(os.getenv("EMBED_COLOR", "3447003"))
SHOW_THUMBNAIL = os.getenv("SHOW_THUMBNAIL", "true").strip().lower() == "true"
SHOW_TIMESTAMP = os.getenv("SHOW_TIMESTAMP", "true").strip().lower() == "true"
SUMMARY_LIMIT = max(100, int(os.getenv("SUMMARY_LIMIT", "500")))
SEEN_IDS_LIMIT = max(100, int(os.getenv("SEEN_IDS_LIMIT", "500")))
POST_DELAY_SECONDS = max(0, float(os.getenv("POST_DELAY_SECONDS", "1")))


def log(message: str) -> None:
    print(message, flush=True)


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {
            "version": 2,
            "feed_url": "",
            "feed_title": "",
            "etag": "",
            "modified": "",
            "seen_ids": [],
            "last_run_at": "",
            "last_status": "",
        }

    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state file must contain an object")
        data.setdefault("version", 2)
        data.setdefault("feed_url", "")
        data.setdefault("feed_title", "")
        data.setdefault("etag", "")
        data.setdefault("modified", "")
        data.setdefault("seen_ids", [])
        data.setdefault("last_run_at", "")
        data.setdefault("last_status", "")
        return data
    except Exception as exc:
        log(f"Warning: could not read state file cleanly: {exc}")
        return {
            "version": 2,
            "feed_url": "",
            "feed_title": "",
            "etag": "",
            "modified": "",
            "seen_ids": [],
            "last_run_at": "",
            "last_status": "state_reset",
        }


def save_state(state: dict[str, Any]) -> None:
    state["version"] = 2
    state["last_run_at"] = datetime.now(timezone.utc).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def clean_text(value: Any, limit: int = 400) -> str:
    if value is None:
        return ""

    text = str(value)
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text


def entry_id(entry: dict[str, Any]) -> str:
    for key in ("id", "guid", "link", "title"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    return ""


def entry_timestamp(entry: dict[str, Any]) -> int:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        return int(time.mktime(parsed))
    return 0


def entry_timestamp_iso(entry: dict[str, Any]) -> str | None:
    ts = entry_timestamp(entry)
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def choose_thumbnail(entry: dict[str, Any]) -> str | None:
    media_thumbnail = entry.get("media_thumbnail")
    if isinstance(media_thumbnail, list) and media_thumbnail:
        url = media_thumbnail[0].get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            return url

    media_content = entry.get("media_content")
    if isinstance(media_content, list) and media_content:
        url = media_content[0].get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            return url

    image = entry.get("image")
    if isinstance(image, dict):
        href = image.get("href")
        if isinstance(href, str) and href.startswith(("http://", "https://")):
            return href

    links = entry.get("links")
    if isinstance(links, list):
        for link in links:
            href = link.get("href")
            mime_type = str(link.get("type") or "")
            if (
                isinstance(href, str)
                and href.startswith(("http://", "https://"))
                and mime_type.startswith("image/")
            ):
                return href

    return None


def fetch_feed(feed_url: str, etag: str = "", modified: str = ""):
    kwargs: dict[str, Any] = {
        "agent": USER_AGENT,
        "request_headers": {"Accept": "application/rss+xml, application/atom+xml, text/xml, application/xml"},
    }
    if etag:
        kwargs["etag"] = etag
    if modified:
        kwargs["modified"] = modified

    parsed = feedparser.parse(feed_url, **kwargs)

    status = int(getattr(parsed, "status", 200) or 200)
    if status >= 400:
        raise RuntimeError(f"Feed request failed with HTTP {status}")

    if getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", []):
        exc = getattr(parsed, "bozo_exception", None)
        raise RuntimeError(f"Could not parse feed: {exc or 'unknown parse error'}")

    return parsed


def build_embed(feed_title: str, entry: dict[str, Any]) -> dict[str, Any]:
    title = clean_text(entry.get("title") or "New post", 256)
    description = clean_text(
        entry.get("summary") or entry.get("description") or "New RSS item published.",
        SUMMARY_LIMIT,
    )

    embed: dict[str, Any] = {
        "title": title,
        "url": (entry.get("link") or None),
        "description": description or "New RSS item published.",
        "footer": {"text": clean_text(feed_title, 200)},
        "color": EMBED_COLOR,
    }

    author = clean_text(entry.get("author"), 200)
    if author:
        embed["author"] = {"name": author}

    if SHOW_TIMESTAMP:
        timestamp_iso = entry_timestamp_iso(entry)
        if timestamp_iso:
            embed["timestamp"] = timestamp_iso

    if SHOW_THUMBNAIL:
        thumbnail = choose_thumbnail(entry)
        if thumbnail:
            embed["thumbnail"] = {"url": thumbnail}

    return embed


def discord_post_json(url: str, payload: dict[str, Any], timeout: int = REQUEST_TIMEOUT) -> None:
    data = json.dumps(payload).encode("utf-8")

    for attempt in range(5):
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status not in (200, 204):
                    raise RuntimeError(f"Webhook post failed with status {resp.status}")
                return

        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")

            if exc.code == 429:
                retry_after = 5.0
                try:
                    parsed = json.loads(body)
                    retry_after = float(parsed.get("retry_after", retry_after))
                except Exception:
                    pass

                retry_after = max(retry_after, 1.0)
                log(f"Discord rate limited the webhook. Waiting {retry_after:.2f}s before retry.")
                time.sleep(retry_after)
                continue

            raise RuntimeError(f"Webhook post failed with status {exc.code}: {body}") from exc

        except urllib.error.URLError as exc:
            if attempt == 4:
                raise RuntimeError(f"Network error while posting webhook: {exc}") from exc
            wait_seconds = 2 + attempt
            log(f"Temporary network error posting webhook. Retrying in {wait_seconds}s.")
            time.sleep(wait_seconds)

    raise RuntimeError("Webhook post failed after multiple retries")


def post_to_discord(webhook_url: str, feed_title: str, entry: dict[str, Any]) -> None:
    payload: dict[str, Any] = {
        "username": WEBHOOK_USERNAME,
        "embeds": [build_embed(feed_title, entry)],
        "allowed_mentions": {"parse": []},
    }

    if WEBHOOK_AVATAR_URL:
        payload["avatar_url"] = WEBHOOK_AVATAR_URL

    if MENTION_TEXT:
        payload["content"] = MENTION_TEXT[:2000]

    discord_post_json(webhook_url, payload)


def normalize_entries(parsed_feed) -> list[tuple[str, dict[str, Any]]]:
    items: list[tuple[str, dict[str, Any]]] = []

    for raw_entry in list(getattr(parsed_feed, "entries", [])):
        eid = entry_id(raw_entry)
        if not eid:
            continue
        items.append((eid, raw_entry))

    items.sort(key=lambda pair: (entry_timestamp(pair[1]), pair[0]))
    return items


def trim_seen_ids(seen_ids: set[str]) -> list[str]:
    trimmed = list(seen_ids)[-SEEN_IDS_LIMIT:]
    return trimmed


def handle_first_run(
    *,
    feed_title: str,
    items: list[tuple[str, dict[str, Any]]],
    webhook_url: str,
    state: dict[str, Any],
) -> int:
    if FIRST_RUN_MODE not in {"seed", "latest", "all"}:
        raise RuntimeError("FIRST_RUN_MODE must be one of: seed, latest, all")

    if not items:
        state["seen_ids"] = []
        state["feed_title"] = feed_title
        state["last_status"] = "first_run_no_items"
        save_state(state)
        log("First run complete: feed has no items.")
        return 0

    if FIRST_RUN_MODE == "seed":
        state["seen_ids"] = [eid for eid, _ in items][-SEEN_IDS_LIMIT:]
        state["feed_title"] = feed_title
        state["last_status"] = "first_run_seeded"
        save_state(state)
        log("First run complete: seeded state without posting old items.")
        return 0

    if FIRST_RUN_MODE == "latest":
        latest_eid, latest_entry = items[-1]
        post_to_discord(webhook_url, feed_title, latest_entry)
        state["seen_ids"] = [eid for eid, _ in items][-SEEN_IDS_LIMIT:]
        state["feed_title"] = feed_title
        state["last_status"] = "first_run_posted_latest"
        save_state(state)
        log(f"First run complete: posted latest item: {latest_eid}")
        return 1

    # FIRST_RUN_MODE == "all"
    batch = items[-MAX_POSTS_PER_RUN:]
    posted = 0
    for eid, entry in batch:
        post_to_discord(webhook_url, feed_title, entry)
        posted += 1
        if POST_DELAY_SECONDS:
            time.sleep(POST_DELAY_SECONDS)

    state["seen_ids"] = [eid for eid, _ in items][-SEEN_IDS_LIMIT:]
    state["feed_title"] = feed_title
    state["last_status"] = "first_run_posted_batch"
    save_state(state)
    log(f"First run complete: posted {posted} item(s) and seeded state.")
    return posted


def main() -> int:
    if not RSS_FEED_URL or not DISCORD_WEBHOOK_URL:
        log("Missing RSS_FEED_URL or DISCORD_WEBHOOK_URL")
        return 1

    state = load_state()
    previous_seen_ids = set(str(x) for x in state.get("seen_ids", []))

    parsed_feed = fetch_feed(
        RSS_FEED_URL,
        etag=str(state.get("etag", "")),
        modified=str(state.get("modified", "")),
    )

    status = int(getattr(parsed_feed, "status", 200) or 200)
    feed_title = clean_text(getattr(parsed_feed.feed, "title", None) or "RSS Feed", 200)

    if status == 304:
        state["feed_url"] = RSS_FEED_URL
        state["feed_title"] = feed_title
        state["last_status"] = "not_modified"
        save_state(state)
        log("Feed not modified since last run.")
        return 0

    items = normalize_entries(parsed_feed)
    log(f"Fetched {len(items)} feed item(s) from: {feed_title}")

    state["feed_url"] = RSS_FEED_URL
    state["feed_title"] = feed_title
    state["etag"] = str(getattr(parsed_feed, "etag", "") or "")
    state["modified"] = str(getattr(parsed_feed, "modified", "") or "")

    if not previous_seen_ids:
        posted = handle_first_run(
            feed_title=feed_title,
            items=items,
            webhook_url=DISCORD_WEBHOOK_URL,
            state=state,
        )
        return 0 if posted >= 0 else 1

    new_items = [(eid, entry) for eid, entry in items if eid not in previous_seen_ids]

    if not new_items:
        state["seen_ids"] = trim_seen_ids(previous_seen_ids)
        state["last_status"] = "no_new_items"
        save_state(state)
        log("No new items found.")
        return 0

    new_items = new_items[-MAX_POSTS_PER_RUN:]
    posted = 0

    for eid, entry in new_items:
        post_to_discord(DISCORD_WEBHOOK_URL, feed_title, entry)
        previous_seen_ids.add(eid)
        posted += 1
        log(f"Posted: {clean_text(entry.get('title') or eid, 120)}")
        if POST_DELAY_SECONDS:
            time.sleep(POST_DELAY_SECONDS)

    state["seen_ids"] = trim_seen_ids(previous_seen_ids)
    state["last_status"] = f"posted_{posted}"
    save_state(state)
    log(f"Posted {posted} new item(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
