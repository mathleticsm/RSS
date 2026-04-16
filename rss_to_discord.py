import hashlib
import html
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser

STATE_FILE = Path(".rss_state.json")

# Required
RSS_FEED_URL = os.getenv("RSS_FEED_URL", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

# Runtime behavior
FIRST_RUN_MODE = os.getenv("FIRST_RUN_MODE", "seed").strip().lower()  # seed | latest | all
MAX_POSTS_PER_RUN = max(1, int(os.getenv("MAX_POSTS_PER_RUN", "2")))
MULTI_GAME_EMBEDS_LIMIT = max(1, min(25, int(os.getenv("MULTI_GAME_EMBEDS_LIMIT", "8"))))
SEEN_IDS_LIMIT = max(50, int(os.getenv("SEEN_IDS_LIMIT", "500")))

# Display
WEBHOOK_USERNAME = os.getenv("WEBHOOK_USERNAME", "FitGirl RSS").strip()
WEBHOOK_AVATAR_URL = os.getenv("WEBHOOK_AVATAR_URL", "").strip()
MENTION_TEXT = os.getenv("MENTION_TEXT", "").strip()

EMBED_COLOR = int(os.getenv("EMBED_COLOR", "10181046"))
SUMMARY_LIMIT = max(80, int(os.getenv("SUMMARY_LIMIT", "140")))
SHOW_TIMESTAMP = os.getenv("SHOW_TIMESTAMP", "true").strip().lower() == "true"
SHOW_SOURCE_LINK = os.getenv("SHOW_SOURCE_LINK", "true").strip().lower() == "true"
SHOW_STEAM_LINKS = os.getenv("SHOW_STEAM_LINKS", "true").strip().lower() == "true"

# Requests
REQUEST_TIMEOUT = max(5, int(os.getenv("REQUEST_TIMEOUT", "30")))
POST_DELAY_SECONDS = max(0.0, float(os.getenv("POST_DELAY_SECONDS", "1")))
STEAM_REQUEST_DELAY_SECONDS = max(0.0, float(os.getenv("STEAM_REQUEST_DELAY_SECONDS", "0.6")))
STEAM_LOOKUPS_LIMIT = max(1, int(os.getenv("STEAM_LOOKUPS_LIMIT", "6")))
USER_AGENT = os.getenv(
    "USER_AGENT",
    "rss-to-discord-actions/5.0 (+https://github.com/actions)",
).strip()

# Filters (optional)
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

# Discord limits
DISCORD_MAX_EMBEDS_PER_MESSAGE = 10

STEAM_SEARCH_CACHE: dict[str, dict[str, Any] | None] = {}
STEAM_LOOKUPS_USED = 0


def log(message: str) -> None:
    print(message, flush=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_state() -> dict[str, Any]:
    return {
        "version": 5,
        "feed_url": "",
        "feed_title": "",
        "etag": "",
        "modified": "",
        "seen_ids": [],
        "last_run_at": "",
        "last_status": "",
        "last_posted_ids": [],
    }


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return default_state()

    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state file must be a JSON object")

        base = default_state()
        base.update(data)
        return base
    except Exception as exc:
        log(f"Warning: could not read state file; resetting it. Reason: {exc}")
        state = default_state()
        state["last_status"] = "state_reset"
        return state


def save_state(state: dict[str, Any]) -> None:
    state["version"] = 5
    state["last_run_at"] = utc_now_iso()
    STATE_FILE.write_text(
        json.dumps(state, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def trim_seen_ids(seen_ids: list[str]) -> list[str]:
    deduped = list(OrderedDict.fromkeys(seen_ids))
    return deduped[-SEEN_IDS_LIMIT:]


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
        return html.unescape(match.group(1))
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
    text = clean_text(raw, SUMMARY_LIMIT * 4)
    text = strip_boilerplate(text)

    if not text:
        return "New post published on the feed."

    lines = [line.strip("•-–— ") for line in text.splitlines() if line.strip()]
    text = " ".join(lines)

    if len(text) > SUMMARY_LIMIT:
        text = text[: SUMMARY_LIMIT - 1].rstrip() + "…"

    return text


def choose_entry_image(entry: dict[str, Any]) -> str | None:
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

    summary_html = entry.get("summary") or entry.get("description")
    return extract_first_image_url(summary_html)


def slugify_for_compare(text: str) -> str:
    text = clean_text(text, 300).lower()
    text = re.sub(r"\[[^\]]+\]", " ", text)
    text = re.sub(r"\([^)]*(build|edition|v\d+|update)[^)]*\)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def cleanup_game_name(text: str) -> str:
    text = clean_text(text, 220)
    text = strip_boilerplate(text)
    text = re.sub(r"^\s*[•\-–—>*→]+\s*", "", text)
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"\s+\((latest build|build[^)]*|v[\d.]+|update[^)]*)\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+[–—-]\s+repack.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+[–—-]\s+will be released.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+[–—-]\s+continue reading.*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^(upcoming repacks|fitgirl repacks?)\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" -–—|:;,.")
    return text


def matches_filters(entry: dict[str, Any]) -> bool:
    haystack = (
        clean_text(entry.get("title"), 400)
        + " "
        + clean_text(entry.get("summary") or entry.get("description"), 1200)
    ).lower()

    if INCLUDE_KEYWORDS and not any(keyword in haystack for keyword in INCLUDE_KEYWORDS):
        return False

    if EXCLUDE_KEYWORDS and any(keyword in haystack for keyword in EXCLUDE_KEYWORDS):
        return False

    return True


def extract_game_candidates(entry: dict[str, Any], feed_title: str) -> list[str]:
    title = normalize_title(str(entry.get("title") or ""), feed_title)
    raw_html = str(entry.get("summary") or entry.get("description") or "")
    raw_text = clean_text(raw_html, 8000)

    normalized = raw_text
    normalized = normalized.replace("→", "\n")
    normalized = normalized.replace("►", "\n")
    normalized = normalized.replace("•", "\n")
    normalized = normalized.replace("—>", "\n")
    normalized = normalized.replace("->", "\n")

    lines = [cleanup_game_name(part) for part in normalized.splitlines()]
    lines = [line for line in lines if line]

    is_multi = "upcoming repacks" in title.lower() or len(lines) >= 3

    candidates: list[str] = []

    if is_multi:
        for line in lines:
            lowered = line.lower()
            if lowered in {"upcoming repacks", "fitgirl repacks"}:
                continue
            if "appeared first on" in lowered:
                continue
            if "continue reading" in lowered:
                continue
            candidates.append(line)
    else:
        candidates.append(cleanup_game_name(title))

    cleaned: list[str] = []
    seen: set[str] = set()

    for candidate in candidates:
        candidate = cleanup_game_name(candidate)
        if not candidate:
            continue

        key = slugify_for_compare(candidate)
        if not key or key in seen:
            continue
        if len(key) < 2:
            continue

        seen.add(key)
        cleaned.append(candidate)

    if not cleaned:
        cleaned = [normalize_title(str(entry.get("title") or ""), feed_title)]

    return cleaned[:MULTI_GAME_EMBEDS_LIMIT]


def tokenize(text: str) -> set[str]:
    return set(slugify_for_compare(text).split())


def steam_search_score(query: str, result_title: str) -> int:
    q = slugify_for_compare(query)
    r = slugify_for_compare(result_title)

    if not q or not r:
        return 0
    if q == r:
        return 1000

    q_tokens = tokenize(q)
    r_tokens = tokenize(r)
    overlap = len(q_tokens & r_tokens)

    score = overlap * 100

    if q in r:
        score += 250
    if r in q:
        score += 150
    if result_title.lower().startswith(query.lower()):
        score += 120

    return score


def steam_search_game(game_name: str) -> dict[str, Any] | None:
    global STEAM_LOOKUPS_USED

    key = slugify_for_compare(game_name)
    if key in STEAM_SEARCH_CACHE:
        return STEAM_SEARCH_CACHE[key]

    if STEAM_LOOKUPS_USED >= STEAM_LOOKUPS_LIMIT:
        STEAM_SEARCH_CACHE[key] = None
        return None

    STEAM_LOOKUPS_USED += 1
    if STEAM_REQUEST_DELAY_SECONDS:
        time.sleep(STEAM_REQUEST_DELAY_SECONDS)

    url = "https://store.steampowered.com/search/?term=" + urllib.parse.quote_plus(game_name)
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT},
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            page = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:
        log(f"Steam lookup failed for '{game_name}': {exc}")
        STEAM_SEARCH_CACHE[key] = None
        return None

    pattern = re.compile(
        r'<a[^>]+href="(?P<href>https://store\.steampowered\.com/app/(?P<appid>\d+)/[^"]+)"[^>]*class="[^"]*search_result_row[^"]*"[^>]*>(?P<body>.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )

    best_match: dict[str, Any] | None = None
    best_score = -1

    for match in pattern.finditer(page):
        href = html.unescape(match.group("href"))
        body = match.group("body")

        title_match = re.search(
            r'<span[^>]*class="title"[^>]*>(.*?)</span>',
            body,
            flags=re.IGNORECASE | re.DOTALL,
        )
        image_match = re.search(
            r'<img[^>]+src="([^"]+)"',
            body,
            flags=re.IGNORECASE,
        )

        title = clean_text(title_match.group(1) if title_match else "", 200)
        image = html.unescape(image_match.group(1)) if image_match else ""
        score = steam_search_score(game_name, title)

        if score > best_score:
            best_score = score
            best_match = {
                "title": title,
                "url": href,
                "image": image if image.startswith(("http://", "https://")) else "",
                "score": score,
            }

    if best_match and best_match["score"] >= 120:
        STEAM_SEARCH_CACHE[key] = best_match
        return best_match

    STEAM_SEARCH_CACHE[key] = None
    return None


def category_emoji(category: str) -> str:
    mapping = {
        "Upcoming Release": "🟡",
        "Repack Update": "🔥",
        "Update": "🛠️",
        "News": "📰",
    }
    return mapping.get(category, "🎮")


def category_color(category: str) -> int:
    mapping = {
        "Upcoming Release": 16766720,
        "Repack Update": 10181046,
        "Update": 5793266,
        "News": 3447003,
    }
    return mapping.get(category, EMBED_COLOR)


def infer_game_category(entry: dict[str, Any], game_name: str) -> str:
    combined = (
        clean_text(entry.get("title"), 300)
        + " "
        + clean_text(entry.get("summary") or entry.get("description"), 1000)
        + " "
        + game_name
    ).lower()

    if "upcoming repacks" in combined or "will be released" in combined:
        return "Upcoming Release"
    if "repack" in combined:
        return "Repack Update"
    if "patch" in combined or "hotfix" in combined or "update" in combined:
        return "Update"
    return "News"


def chip(label: str) -> str:
    return f"`{label}`"


def compact_summary(text: str, limit: int = 140) -> str:
    text = clean_text(text, limit * 3)
    text = strip_boilerplate(text)
    text = re.sub(r"\s+", " ", text).strip()

    if len(text) > limit:
        return text[: limit - 1].rstrip() + "…"
    return text or "Listed in the latest FitGirl feed update."


def build_meta_line(
    *,
    category: str,
    published: str,
    has_steam: bool,
    has_source: bool,
) -> str:
    parts = [
        f"{category_emoji(category)} {chip(category)}",
        f"📅 {chip(published)}",
    ]
    if has_steam:
        parts.append(f"🛒 {chip('Steam')}")
    if has_source:
        parts.append(f"📦 {chip('FitGirl')}")
    return " ".join(parts)


def build_premium_description(
    *,
    category: str,
    entry: dict[str, Any],
    steam_match: dict[str, Any] | None,
    source_url: str,
    total_games: int,
) -> str:
    lines = [
        build_meta_line(
            category=category,
            published=display_date(entry),
            has_steam=bool(steam_match and steam_match.get("url")),
            has_source=bool(source_url),
        ),
        "",
    ]

    if total_games == 1:
        lines.append(compact_summary(
            entry.get("summary") or entry.get("description") or "",
            SUMMARY_LIMIT,
        ))
    else:
        lines.append("Listed in the latest multi-game FitGirl update.")

    return "\n".join(lines)[:4096]


def build_action_fields(
    *,
    steam_match: dict[str, Any] | None,
    source_url: str,
    category: str,
    index: int,
    total: int,
) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []

    if steam_match and steam_match.get("url") and SHOW_STEAM_LINKS:
        fields.append(
            {
                "name": "🛒 Steam",
                "value": f"[Open Store Page]({steam_match['url']})",
                "inline": True,
            }
        )

    if source_url and SHOW_SOURCE_LINK:
        fields.append(
            {
                "name": "📦 Source",
                "value": f"[Open FitGirl Post]({source_url})",
                "inline": True,
            }
        )

    fields.append(
        {
            "name": "🏷️ Type",
            "value": category,
            "inline": True,
        }
    )

    if total > 1:
        fields.append(
            {
                "name": "📚 List Position",
                "value": f"{index}/{total}",
                "inline": True,
            }
        )

    return fields


def build_game_embed(
    *,
    feed_title: str,
    entry: dict[str, Any],
    game_name: str,
    steam_match: dict[str, Any] | None,
    index: int,
    total: int,
) -> dict[str, Any]:
    source_url = str(entry.get("link") or "").strip()
    category = infer_game_category(entry, game_name)

    image_url = None
    if steam_match and steam_match.get("image"):
        image_url = steam_match["image"]
    else:
        image_url = choose_entry_image(entry)

    embed_url = None
    if steam_match and steam_match.get("url"):
        embed_url = steam_match["url"]
    elif source_url:
        embed_url = source_url

    embed: dict[str, Any] = {
        "author": {
            "name": "FitGirl Premium Feed",
        },
        "title": clean_text(game_name, 256),
        "url": embed_url,
        "description": build_premium_description(
            category=category,
            entry=entry,
            steam_match=steam_match,
            source_url=source_url,
            total_games=total,
        ),
        "color": category_color(category),
        "fields": build_action_fields(
            steam_match=steam_match,
            source_url=source_url,
            category=category,
            index=index,
            total=total,
        ),
        "footer": {
            "text": f"{feed_title} • {category}"[:2048],
        },
    }

    if SHOW_TIMESTAMP:
        timestamp_iso = entry_timestamp_iso(entry)
        if timestamp_iso:
            embed["timestamp"] = timestamp_iso

    if image_url:
        embed["image"] = {"url": image_url}

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


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def post_entry_to_discord(webhook_url: str, feed_title: str, entry: dict[str, Any]) -> None:
    games = extract_game_candidates(entry, feed_title)
    source_title = normalize_title(str(entry.get("title") or ""), feed_title)

    embeds: list[dict[str, Any]] = []
    for index, game_name in enumerate(games, start=1):
        steam_match = steam_search_game(game_name)
        embeds.append(
            build_game_embed(
                feed_title=feed_title,
                entry=entry,
                game_name=game_name,
                steam_match=steam_match,
                index=index,
                total=len(games),
            )
        )

    if not embeds:
        return

    embed_chunks = chunked(embeds[:MULTI_GAME_EMBEDS_LIMIT], DISCORD_MAX_EMBEDS_PER_MESSAGE)

    for chunk_index, embed_group in enumerate(embed_chunks, start=1):
        content_lines: list[str] = []

        if chunk_index == 1 and MENTION_TEXT:
            content_lines.append(MENTION_TEXT[:2000])

        if len(games) > 1:
            if len(embed_chunks) == 1:
                content_lines.append(f"## 🎮 {source_title}")
                content_lines.append(f"Showing **{len(embeds)}** games from this update.")
            else:
                start_num = (chunk_index - 1) * DISCORD_MAX_EMBEDS_PER_MESSAGE + 1
                end_num = start_num + len(embed_group) - 1
                content_lines.append(f"## 🎮 {source_title}")
                content_lines.append(f"Showing games **{start_num}-{end_num}** of **{len(embeds)}**.")

        payload: dict[str, Any] = {
            "username": WEBHOOK_USERNAME,
            "embeds": embed_group,
            "allowed_mentions": {"parse": []},
        }

        if content_lines:
            payload["content"] = "\n".join(content_lines)[:2000]

        if WEBHOOK_AVATAR_URL:
            payload["avatar_url"] = WEBHOOK_AVATAR_URL

        discord_post_json(webhook_url, payload)

        if POST_DELAY_SECONDS and chunk_index != len(embed_chunks):
            time.sleep(POST_DELAY_SECONDS)


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
        post_entry_to_discord(webhook_url, feed_title, latest_entry)
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
        post_entry_to_discord(webhook_url, feed_title, entry)
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
        post_entry_to_discord(DISCORD_WEBHOOK_URL, feed_title, entry)
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
