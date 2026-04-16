import hashlib
import html
import json
import os
import re
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser

STATE_FILE = Path(".rss_state.json")

RSS_FEED_URL = os.getenv("RSS_FEED_URL", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

FIRST_RUN_MODE = os.getenv("FIRST_RUN_MODE", "seed").strip().lower()
MAX_POSTS_PER_RUN = max(1, int(os.getenv("MAX_POSTS_PER_RUN", "3")))
SEEN_IDS_LIMIT = max(50, int(os.getenv("SEEN_IDS_LIMIT", "400")))
SUMMARY_LIMIT = max(120, int(os.getenv("SUMMARY_LIMIT", "280")))
REQUEST_TIMEOUT = max(5, int(os.getenv("REQUEST_TIMEOUT", "30")))
POST_DELAY_SECONDS = max(0.0, float(os.getenv("POST_DELAY_SECONDS", "1")))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "rss-to-discord-actions/3.0 (+https://github.com/actions)",
).strip()

WEBHOOK_USERNAME = os.getenv("WEBHOOK_USERNAME", "RSS Feed").strip()
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "").strip()
MENTION_TEXT = os.getenv("MENTION_TEXT", "").strip()

EMBED_COLOR = int(os.getenv("EMBED_COLOR", "10181046"))
SHOW_THUMBNAIL = os.getenv("SHOW_THUMBNAIL", "true").strip().lower() == "true"
SHOW_TIMESTAMP = os.getenv("SHOW_TIMESTAMP", "true").strip().lower() == "true"
SHOW_TAGS = os.getenv("SHOW_TAGS", "true").strip().lower() == "true"
SHOW_STATS = os.getenv("SHOW_STATS", "true").strip().lower() == "true"

INCLUDE_KEYWORDS = [
    x.strip().lower()
    for x in os.getenv("INCLUDE_KEYWORDS", "").split(",")
    if x.strip()
]
EXCLUDE_KEYWORDS = [
    x.strip().lower()
    for x in os.getenv("EXCLUDE_KEYWORDS", "").split(",")
    if x.strip()
]


def log(message: str) -> None:
    print(message, flush=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {
            "version": 3,
            "feed_url": "",
            "feed_title": "",
            "etag": "",
            "modified": "",
            "seen_ids": [],
            "last_run_at": "",
            "last_status": "",
            "last_posted_ids": [],
        }

    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state file must be an object")
        data.setdefault("version", 3)
        data.setdefault("feed_url", "")
        data.setdefault("feed_title", "")
        data.setdefault("etag", "")
        data.setdefault("modified", "")
        data.setdefault("seen_ids", [])
        data.setdefault("last_run_at", "")
        data.setdefault("last_status", "")
        data.setdefault("last_posted_ids", [])
        return data
    except Exception as exc:
        log(f"Warning: resetting unreadable state file: {exc}")
        return {
            "version": 3,
            "feed_url": "",
            "feed_title": "",
            "etag": "",
            "modified": "",
            "seen_ids": [],
            "last_run_at": "",
            "last_status": "state_reset",
            "last_posted_ids": [],
        }


def save_state(state: dict[str, Any]) -> None:
    state["version"] = 3
    state["last_run_at"] = utc_now_iso()
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def clean_text(value: Any, limit: int = 300) -> str:
    if value is None:
        return ""

    text = str(value)
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n\s+", "\n", text)
    text = text.strip()

    if len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text


def strip_boilerplate(text: str) -> str:
    patterns = [
        r"Continue reading.*$",
        r"The post .*? appeared first on .*?$",
        r"Read more.*$",
        r"Source:.*$",
    ]
    for pattern in patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
    return text


def extract_first_image_url(raw_html: str | None) -> str | None:
    if not raw_html:
        return None

    match = re.search(
        r"""<img[^>]+src=["'](https?://[^"' >]+)["']""",
        raw_html,
        flags=re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def stable_entry_id(entry: dict[str, Any]) -> str:
    candidates = [
        str(entry.get("id") or "").strip(),
        str(entry.get("guid") or "").strip(),
        str(entry.get("link") or "").strip(),
        str(entry.get("title") or "").strip(),
    ]
    for candidate in candidates:
        if candidate:
            return candidate

    digest = hashlib.sha256(
        json.dumps(entry, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"hash:{digest[:24]}"


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


def display_date(entry: dict[str, Any]) -> str:
    ts = entry_timestamp(entry)
    if ts <= 0:
        return "Unknown"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%b %d, %Y")


def choose_thumbnail(entry: dict[str, Any]) -> str | None:
    sources: list[str | None] = []

    media_thumbnail = entry.get("media_thumbnail")
    if isinstance(media_thumbnail, list) and media_thumbnail:
        sources.append(media_thumbnail[0].get("url"))

    media_content = entry.get("media_content")
    if isinstance(media_content, list) and media_content:
        sources.append(media_content[0].get("url"))

    image = entry.get("image")
    if isinstance(image, dict):
        sources.append(image.get("href"))

    summary_html = entry.get("summary") or entry.get("description")
    sources.append(extract_first_image_url(summary_html))

    links = entry.get("links")
    if isinstance(links, list):
        for link in links:
            href = link.get("href")
            mime_type = str(link.get("type") or "")
            if mime_type.startswith("image/"):
                sources.append(href)

    for source in sources:
        if isinstance(source, str) and source.startswith(("http://", "https://")):
            return source

    return None


def fetch_feed(feed_url: str, etag: str = "", modified: str = ""):
    kwargs: dict[str, Any] = {
        "agent": USER_AGENT,
        "request_headers": {
            "Accept": "application/rss+xml, application/atom+xml, text/xml, application/xml"
        },
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


def normalize_title(raw_title: str, feed_title: str) -> str:
    title = clean_text(raw_title, 256)
    title = re.sub(r"^\[?fitgirl.*?\]?\s*[-–—:|]*\s*", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*[-–—|:]\s*FitGirl.*$", "", title, flags=re.IGNORECASE)
    title = title.strip(" -–—|:")

    if title and title.lower() != feed_title.lower():
        return title

    return "New update"


def summarize_entry(entry: dict[str, Any]) -> str:
    raw = entry.get("summary") or entry.get("description") or entry.get("content") or ""
    text = clean_text(raw, SUMMARY_LIMIT * 3)
    text = strip_boilerplate(text)

    if not text:
        return "New post published on the feed."

    lines = [line.strip("•-–— ") for line in text.splitlines() if line.strip()]
    text = " ".join(lines)

    if len(text) > SUMMARY_LIMIT:
        text = text[: SUMMARY_LIMIT - 1].rstrip() + "…"

    return text


def extract_tags(entry: dict[str, Any]) -> list[str]:
    tags: list[str] = []

    raw_tags = entry.get("tags")
    if isinstance(raw_tags, list):
        for tag in raw_tags:
            term = clean_text(tag.get("term"), 40)
            if term:
                tags.append(term)

    raw_title = clean_text(entry.get("title"), 300)
    if "upcoming repacks" in raw_title.lower():
        tags.insert(0, "Upcoming")

    deduped = list(OrderedDict.fromkeys(tags))
    return deduped[:4]


def classify_entry(entry: dict[str, Any]) -> str:
    title = clean_text(entry.get("title"), 300).lower()
    summary = summarize_entry(entry).lower()
    text = f"{title} {summary}"

    if "upcoming repacks" in text:
        return "Upcoming Release"
    if "repack" in text:
        return "Repack Update"
    if "patch" in text or "hotfix" in text or "update" in text:
        return "Update"
    return "News"


def matches_filters(entry: dict[str, Any]) -> bool:
    haystack = (
        clean_text(entry.get("title"), 400)
        + " "
        + clean_text(entry.get("summary") or entry.get("description"), 800)
    ).lower()

    if INCLUDE_KEYWORDS and not any(keyword in haystack for keyword in INCLUDE_KEYWORDS):
        return False

    if EXCLUDE_KEYWORDS and any(keyword in haystack for keyword in EXCLUDE_KEYWORDS):
        return False

    return True


def build_embed(feed_title: str, entry: dict[str, Any]) -> dict[str, Any]:
    title = normalize_title(str(entry.get("title") or ""), feed_title)
    description = summarize_entry(entry)
    post_url = str(entry.get("link") or "").strip()
    tags = extract_tags(entry)
    entry_type = classify_entry(entry)

    embed: dict[str, Any] = {
        "author": {
            "name": "FitGirl RSS",
        },
        "title": title[:256],
        "url": post_url or None,
        "description": description or "New feed item published.",
        "color": EMBED_COLOR,
        "footer": {
            "text": f"{entry_type} • {feed_title}"[:2048],
        },
        "fields": [],
    }

    if SHOW_STATS:
        embed["fields"].append(
            {
                "name": "Published",
                "value": display_date(entry),
                "inline": True,
            }
        )
        embed["fields"].append(
            {
                "name": "Category",
                "value": entry_type,
                "inline": True,
            }
        )

    if SHOW_TAGS and tags:
        embed["fields"].append(
            {
                "name": "Tags",
                "value": " • ".join(tags)[:1024],
                "inline": False,
            }
        )

    if SHOW_TIMESTAMP:
        timestamp_iso = entry_timestamp_iso(entry)
        if timestamp_iso:
            embed["timestamp"] = timestamp_iso

    if SHOW_THUMBNAIL:
        thumbnail = choose_thumbnail(entry)
        if thumbnail:
            embed["thumbnail"] = {"url": thumbnail}

    return embed


def discord_post_json(url: str, payload: dict[str, Any]) -> None:
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
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
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
                log(f"Webhook rate limited. Sleeping {retry_after:.2f}s.")
                time.sleep(retry_after)
                continue

            raise RuntimeError(f"Webhook post failed with status {exc.code}: {body}") from exc

        except urllib.error.URLError as exc:
            if attempt == 4:
                raise RuntimeError(f"Network error while posting webhook: {exc}") from exc
            wait_seconds = 2 + attempt
            log(f"Temporary network error. Retrying in {wait_seconds}s.")
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
        eid = stable_entry_id(raw_entry)
        if not eid:
            continue
        if not matches_filters(raw_entry):
            continue
        items.append((eid, raw_entry))

    items.sort(key=lambda pair: (entry_timestamp(pair[1]), pair[0]))
    return items


def trim_seen_ids(seen_ids: list[str]) -> list[str]:
    deduped = list(OrderedDict.fromkeys(seen_ids))
    return deduped[-SEEN_IDS_LIMIT:]


def handle_first_run(
    *,
    feed_title: str,
    items: list[tuple[str, dict[str, Any]]],
    webhook_url: str,
    state: dict[str, Any],
) -> int:
    if FIRST_RUN_MODE not in {"seed", "latest", "all"}:
        raise RuntimeError("FIRST_RUN_MODE must be seed, latest, or all")

    if not items:
        state["seen_ids"] = []
        state["feed_title"] = feed_title
        state["last_status"] = "first_run_no_items"
        state["last_posted_ids"] = []
        save_state(state)
        log("First run: no items found.")
        return 0

    if FIRST_RUN_MODE == "seed":
        state["seen_ids"] = trim_seen_ids([eid for eid, _ in items])
        state["feed_title"] = feed_title
        state["last_status"] = "first_run_seeded"
        state["last_posted_ids"] = []
        save_state(state)
        log("First run: seeded state without posting old items.")
        return 0

    if FIRST_RUN_MODE == "latest":
        latest_eid, latest_entry = items[-1]
        post_to_discord(webhook_url, feed_title, latest_entry)
        state["seen_ids"] = trim_seen_ids([eid for eid, _ in items])
        state["feed_title"] = feed_title
        state["last_status"] = "first_run_posted_latest"
        state["last_posted_ids"] = [latest_eid]
        save_state(state)
        log(f"First run: posted latest item: {latest_eid}")
        return 1

    batch = items[-MAX_POSTS_PER_RUN:]
    posted_ids: list[str] = []
    for eid, entry in batch:
        post_to_discord(webhook_url, feed_title, entry)
        posted_ids.append(eid)
        if POST_DELAY_SECONDS:
            time.sleep(POST_DELAY_SECONDS)

    state["seen_ids"] = trim_seen_ids([eid for eid, _ in items])
    state["feed_title"] = feed_title
    state["last_status"] = f"first_run_posted_{len(posted_ids)}"
    state["last_posted_ids"] = posted_ids
    save_state(state)
    log(f"First run: posted {len(posted_ids)} item(s) and seeded state.")
    return len(posted_ids)


def main() -> int:
    if not RSS_FEED_URL or not DISCORD_WEBHOOK_URL:
        log("Missing RSS_FEED_URL or DISCORD_WEBHOOK_URL")
        return 1

    state = load_state()
    previous_seen_ids = trim_seen_ids([str(x) for x in state.get("seen_ids", [])])

    parsed_feed = fetch_feed(
        RSS_FEED_URL,
        etag=str(state.get("etag", "")),
        modified=str(state.get("modified", "")),
    )

    status = int(getattr(parsed_feed, "status", 200) or 200)
    feed_title = clean_text(getattr(parsed_feed.feed, "title", None) or "RSS Feed", 200)

    state["feed_url"] = RSS_FEED_URL
    state["feed_title"] = feed_title
    state["etag"] = str(getattr(parsed_feed, "etag", "") or "")
    state["modified"] = str(getattr(parsed_feed, "modified", "") or "")

    if status == 304:
        state["last_status"] = "not_modified"
        state["last_posted_ids"] = []
        save_state(state)
        log("Feed not modified since last run.")
        return 0

    items = normalize_entries(parsed_feed)
    log(f"Fetched {len(items)} matching feed item(s) from '{feed_title}'.")

    if not previous_seen_ids:
        handle_first_run(
            feed_title=feed_title,
            items=items,
            webhook_url=DISCORD_WEBHOOK_URL,
            state=state,
        )
        return 0

    seen_set = set(previous_seen_ids)
    new_items = [(eid, entry) for eid, entry in items if eid not in seen_set]

    if not new_items:
        state["seen_ids"] = previous_seen_ids
        state["last_status"] = "no_new_items"
        state["last_posted_ids"] = []
        save_state(state)
        log("No new items found.")
        return 0

    new_items = new_items[-MAX_POSTS_PER_RUN:]
    posted_ids: list[str] = []

    for eid, entry in new_items:
        post_to_discord(DISCORD_WEBHOOK_URL, feed_title, entry)
        previous_seen_ids.append(eid)
        posted_ids.append(eid)
        log(f"Posted: {normalize_title(str(entry.get('title') or ''), feed_title)}")
        if POST_DELAY_SECONDS:
            time.sleep(POST_DELAY_SECONDS)

    state["seen_ids"] = trim_seen_ids(previous_seen_ids)
    state["last_status"] = f"posted_{len(posted_ids)}"
    state["last_posted_ids"] = posted_ids
    save_state(state)
    log(f"Posted {len(posted_ids)} new item(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
