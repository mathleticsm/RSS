import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import feedparser

STATE_FILE = Path(".rss_state.json")
MAX_POSTS_PER_RUN = 5


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"seen_ids": []}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"seen_ids": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def fetch_feed(feed_url: str):
    feed = feedparser.parse(feed_url)
    if getattr(feed, "bozo", 0) and not getattr(feed, "entries", []):
        raise RuntimeError("Could not parse feed")
    return feed


def entry_id(entry) -> str:
    return (
        str(entry.get("id") or "").strip()
        or str(entry.get("guid") or "").strip()
        or str(entry.get("link") or "").strip()
        or str(entry.get("title") or "").strip()
    )


def clean_text(value: str | None, limit: int = 300) -> str:
    if not value:
        return ""
    text = " ".join(str(value).split())
    return text[: limit - 1] + "…" if len(text) > limit else text


def post_to_discord(webhook_url: str, feed_title: str, entry: dict) -> None:
    embed = {
        "title": clean_text(entry.get("title") or "New post", 256),
        "url": entry.get("link") or None,
        "description": clean_text(
            entry.get("summary") or entry.get("description") or "New RSS item published.",
            400,
        ),
        "footer": {"text": clean_text(feed_title, 200)},
    }

    payload = {
        "username": "RSS Feed",
        "embeds": [embed],
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=30) as resp:
        if resp.status not in (200, 204):
            raise RuntimeError(f"Webhook post failed with status {resp.status}")


def main() -> int:
    feed_url = os.getenv("RSS_FEED_URL", "").strip()
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

    if not feed_url or not webhook_url:
        print("Missing RSS_FEED_URL or DISCORD_WEBHOOK_URL")
        return 1

    state = load_state()
    seen_ids = set(state.get("seen_ids", []))

    feed = fetch_feed(feed_url)
    feed_title = getattr(feed.feed, "title", None) or "RSS Feed"
    entries = list(getattr(feed, "entries", []))

    items = []
    for entry in entries:
        eid = entry_id(entry)
        if not eid:
            continue
        items.append((eid, entry))

    new_items = [(eid, entry) for eid, entry in items if eid not in seen_ids]
    new_items = new_items[-MAX_POSTS_PER_RUN:]  # avoid flooding
    new_items.reverse()  # oldest first

    if not state.get("seen_ids"):
        # first run: seed without posting old items
        state["seen_ids"] = [eid for eid, _ in items][-100:]
        save_state(state)
        print("Seeded initial state without posting old items.")
        return 0

    for eid, entry in new_items:
        post_to_discord(webhook_url, feed_title, entry)
        seen_ids.add(eid)
        time.sleep(1)

    state["seen_ids"] = list(seen_ids)[-200:]
    save_state(state)

    print(f"Posted {len(new_items)} new item(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
