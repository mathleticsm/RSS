"""
Advanced Discord RSS Bot (Render-ready)

Highlights
- Multiple RSS/Atom subscriptions per server
- Per-subscription settings: channel, interval, mention role, post format
- SQLite persistence with item dedupe across restarts
- Conditional feed fetching with ETag / Last-Modified support
- Background polling plus manual check command
- Small HTTP health server for Render web services
- Slash commands grouped under /feed

Required environment variables
- DISCORD_TOKEN: your Discord bot token

Optional environment variables
- DATABASE_PATH=data/rssbot.db
- PORT=10000
- HOST=0.0.0.0
- POLL_INTERVAL_SECONDS=30
- REQUEST_TIMEOUT_SECONDS=25
- MAX_POSTS_PER_CHECK=10
- INITIAL_SEED_ITEMS=25
- LOG_LEVEL=INFO

Python packages
    pip install discord.py aiohttp feedparser

Render notes
1) Deploy this as a Web Service.
2) Build command:
       pip install -U pip && pip install discord.py aiohttp feedparser
3) Start command:
       python advanced_rss_discord_bot.py
4) Add DISCORD_TOKEN as an environment variable.
5) For persistent history across deploys, attach a persistent disk and set DATABASE_PATH
   to something on that disk, for example: /var/data/rssbot.db
6) Set the health check path to /healthz

Discord commands
- /feed add url:<feed> [channel] [interval_minutes] [mention_role] [post_format]
- /feed list
- /feed remove subscription_id:<id>
- /feed pause subscription_id:<id>
- /feed resume subscription_id:<id>
- /feed edit subscription_id:<id> [channel] [interval_minutes] [mention_role] [post_format]
- /feed check [subscription_id]
- /feed help
"""

from __future__ import annotations

import asyncio
import html
import logging
import os
import re
import signal
import sqlite3
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

import aiohttp
import discord
import feedparser
from aiohttp import web
from discord import app_commands
from discord.ext import commands, tasks


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/rssbot.db")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "10000"))
POLL_INTERVAL_SECONDS = max(10, int(os.getenv("POLL_INTERVAL_SECONDS", "30")))
REQUEST_TIMEOUT_SECONDS = max(5, int(os.getenv("REQUEST_TIMEOUT_SECONDS", "25")))
MAX_POSTS_PER_CHECK = max(1, int(os.getenv("MAX_POSTS_PER_CHECK", "10")))
INITIAL_SEED_ITEMS = max(0, int(os.getenv("INITIAL_SEED_ITEMS", "25")))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HTTP_USER_AGENT = "AdvancedDiscordRSSBot/2.0 (+https://discord.com)"


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("advanced-rss-bot")


def utc_now_ts() -> int:
    return int(time.time())


def ensure_parent_dir(file_path: str) -> None:
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)


def valid_feed_url(value: str) -> str:
    value = value.strip()
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Feed URL must start with http:// or https://")
    return value


def clean_text(value: str | None, limit: int = 350) -> str:
    if not value:
        return ""
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    if len(value) > limit:
        return value[: limit - 1].rstrip() + "…"
    return value


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

    return None


def entry_uid(entry: dict[str, Any]) -> str:
    for key in ("id", "guid", "link", "title"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    return ""


def entry_timestamp(entry: dict[str, Any]) -> int:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        return int(time.mktime(parsed))
    return utc_now_ts()


def ts_to_display(ts: int | None) -> str:
    if not ts:
        return "Never"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


@dataclass(slots=True)
class FeedResult:
    status: int
    title: str
    entries: list[dict[str, Any]]
    etag: str | None
    modified: str | None


class Storage:
    def __init__(self, path: str) -> None:
        ensure_parent_dir(path)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                feed_url TEXT NOT NULL,
                feed_title TEXT NOT NULL DEFAULT 'RSS Feed',
                interval_minutes INTEGER NOT NULL DEFAULT 10,
                mention_role_id INTEGER,
                post_format TEXT NOT NULL DEFAULT 'embed',
                is_paused INTEGER NOT NULL DEFAULT 0,
                etag TEXT,
                modified TEXT,
                last_checked_at INTEGER,
                next_check_at INTEGER,
                last_success_at INTEGER,
                last_error TEXT,
                created_by INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_sub
                ON subscriptions (guild_id, channel_id, feed_url);

            CREATE INDEX IF NOT EXISTS idx_due_subs
                ON subscriptions (is_paused, next_check_at);

            CREATE TABLE IF NOT EXISTS posted_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id INTEGER NOT NULL,
                item_uid TEXT NOT NULL,
                posted_at INTEGER NOT NULL,
                UNIQUE(subscription_id, item_uid),
                FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
            );
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def create_subscription(
        self,
        *,
        guild_id: int,
        channel_id: int,
        feed_url: str,
        feed_title: str,
        interval_minutes: int,
        mention_role_id: int | None,
        post_format: str,
        created_by: int,
    ) -> int:
        now = utc_now_ts()
        cur = self.conn.execute(
            """
            INSERT INTO subscriptions (
                guild_id, channel_id, feed_url, feed_title, interval_minutes,
                mention_role_id, post_format, next_check_at, created_by, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                channel_id,
                feed_url,
                feed_title,
                interval_minutes,
                mention_role_id,
                post_format,
                now + (interval_minutes * 60),
                created_by,
                now,
                now,
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def seed_posted_items(self, subscription_id: int, item_uids: list[str]) -> None:
        now = utc_now_ts()
        self.conn.executemany(
            "INSERT OR IGNORE INTO posted_items (subscription_id, item_uid, posted_at) VALUES (?, ?, ?)",
            [(subscription_id, uid, now) for uid in item_uids if uid],
        )
        self.conn.commit()

    def list_subscriptions(self, guild_id: int) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            "SELECT * FROM subscriptions WHERE guild_id = ? ORDER BY id ASC",
            (guild_id,),
        )
        return list(cur.fetchall())

    def get_subscription(self, subscription_id: int, guild_id: int | None = None) -> sqlite3.Row | None:
        if guild_id is None:
            cur = self.conn.execute("SELECT * FROM subscriptions WHERE id = ?", (subscription_id,))
        else:
            cur = self.conn.execute(
                "SELECT * FROM subscriptions WHERE id = ? AND guild_id = ?",
                (subscription_id, guild_id),
            )
        return cur.fetchone()

    def remove_subscription(self, subscription_id: int, guild_id: int) -> bool:
        cur = self.conn.execute(
            "DELETE FROM subscriptions WHERE id = ? AND guild_id = ?",
            (subscription_id, guild_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def set_paused(self, subscription_id: int, guild_id: int, paused: bool) -> bool:
        now = utc_now_ts()
        cur = self.conn.execute(
            """
            UPDATE subscriptions
            SET is_paused = ?, updated_at = ?, next_check_at = CASE WHEN ? = 0 THEN ? ELSE next_check_at END
            WHERE id = ? AND guild_id = ?
            """,
            (1 if paused else 0, now, 1 if paused else 0, now + 10, subscription_id, guild_id),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def edit_subscription(
        self,
        subscription_id: int,
        guild_id: int,
        *,
        channel_id: int | None = None,
        interval_minutes: int | None = None,
        mention_role_id: int | None | Literal[False] = False,
        post_format: str | None = None,
    ) -> bool:
        current = self.get_subscription(subscription_id, guild_id)
        if not current:
            return False

        now = utc_now_ts()
        new_channel_id = channel_id if channel_id is not None else int(current["channel_id"])
        new_interval = interval_minutes if interval_minutes is not None else int(current["interval_minutes"])
        if mention_role_id is False:
            new_role_id = current["mention_role_id"]
        else:
            new_role_id = mention_role_id
        new_format = post_format if post_format is not None else str(current["post_format"])

        self.conn.execute(
            """
            UPDATE subscriptions
            SET channel_id = ?, interval_minutes = ?, mention_role_id = ?, post_format = ?,
                updated_at = ?, next_check_at = ?
            WHERE id = ? AND guild_id = ?
            """,
            (
                new_channel_id,
                new_interval,
                new_role_id,
                new_format,
                now,
                now + min(new_interval * 60, 300),
                subscription_id,
                guild_id,
            ),
        )
        self.conn.commit()
        return True

    def mark_seen(self, subscription_id: int, item_uid: str) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO posted_items (subscription_id, item_uid, posted_at) VALUES (?, ?, ?)",
            (subscription_id, item_uid, utc_now_ts()),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def is_seen(self, subscription_id: int, item_uid: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM posted_items WHERE subscription_id = ? AND item_uid = ? LIMIT 1",
            (subscription_id, item_uid),
        )
        return cur.fetchone() is not None

    def due_subscriptions(self, now: int) -> list[sqlite3.Row]:
        cur = self.conn.execute(
            """
            SELECT * FROM subscriptions
            WHERE is_paused = 0 AND COALESCE(next_check_at, 0) <= ?
            ORDER BY next_check_at ASC, id ASC
            """,
            (now,),
        )
        return list(cur.fetchall())

    def mark_checked(
        self,
        subscription_id: int,
        *,
        interval_minutes: int,
        etag: str | None,
        modified: str | None,
        feed_title: str | None = None,
        error: str | None = None,
        success: bool = False,
    ) -> None:
        now = utc_now_ts()
        next_check = now + (interval_minutes * 60)
        self.conn.execute(
            """
            UPDATE subscriptions
            SET etag = COALESCE(?, etag),
                modified = COALESCE(?, modified),
                feed_title = COALESCE(?, feed_title),
                last_checked_at = ?,
                next_check_at = ?,
                last_success_at = CASE WHEN ? = 1 THEN ? ELSE last_success_at END,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (etag, modified, feed_title, now, next_check, 1 if success else 0, now, error or "", now, subscription_id),
        )
        self.conn.commit()


class FeedFetcher:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session

    async def fetch(self, url: str, *, etag: str | None = None, modified: str | None = None) -> FeedResult:
        headers: dict[str, str] = {}
        if etag:
            headers["If-None-Match"] = etag
        if modified:
            headers["If-Modified-Since"] = modified

        async with self.session.get(url, headers=headers, allow_redirects=True) as response:
            if response.status == 304:
                return FeedResult(status=304, title="RSS Feed", entries=[], etag=etag, modified=modified)

            response.raise_for_status()
            raw = await response.read()
            new_etag = response.headers.get("ETag")
            new_modified = response.headers.get("Last-Modified")

        parsed = feedparser.parse(raw)
        feed_title = clean_text(parsed.feed.get("title") if hasattr(parsed, "feed") else None, 180) or "RSS Feed"

        entries: list[dict[str, Any]] = []
        for raw_entry in getattr(parsed, "entries", []):
            uid = entry_uid(raw_entry)
            if not uid:
                continue
            entries.append(
                {
                    "uid": uid,
                    "title": clean_text(raw_entry.get("title"), 256) or "Untitled entry",
                    "link": str(raw_entry.get("link") or "").strip(),
                    "summary": clean_text(raw_entry.get("summary") or raw_entry.get("description"), 350),
                    "timestamp": entry_timestamp(raw_entry),
                    "author": clean_text(raw_entry.get("author"), 120),
                    "image": choose_entry_image(raw_entry),
                }
            )

        entries.sort(key=lambda item: item["timestamp"])
        return FeedResult(status=200, title=feed_title, entries=entries, etag=new_etag, modified=new_modified)


class HealthServer:
    def __init__(self, bot: "RSSBot", host: str, port: int) -> None:
        self.bot = bot
        self.host = host
        self.port = port
        self.app = web.Application()
        self.app.router.add_get("/", self.handle_root)
        self.app.router.add_get("/healthz", self.handle_health)
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None

    async def start(self) -> None:
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self.host, port=self.port)
        await self.site.start()
        logger.info("Health server listening on %s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            self.site = None

    async def handle_root(self, request: web.Request) -> web.Response:
        stats = {
            "bot_ready": self.bot.is_ready(),
            "guilds": len(self.bot.guilds),
            "latency_ms": round(self.bot.latency * 1000, 1) if self.bot.is_ready() else None,
        }
        return web.json_response(stats)

    async def handle_health(self, request: web.Request) -> web.Response:
        status = 200 if self.bot.health_ok else 503
        return web.json_response({"ok": self.bot.health_ok}, status=status)


class RSSBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.db = Storage(DATABASE_PATH)
        self.session: aiohttp.ClientSession | None = None
        self.fetcher: FeedFetcher | None = None
        self.health_server = HealthServer(self, HOST, PORT)
        self.db_lock = asyncio.Lock()
        self.fetch_semaphore = asyncio.Semaphore(4)
        self.health_ok = False

    async def setup_hook(self) -> None:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": HTTP_USER_AGENT},
        )
        self.fetcher = FeedFetcher(self.session)
        await self.health_server.start()
        if not poll_due_feeds.is_running():
            poll_due_feeds.start()
        try:
            synced = await self.tree.sync()
            logger.info("Synced %s application command(s)", len(synced))
        except Exception:
            logger.exception("Failed to sync application commands")
        self.health_ok = True

    async def close(self) -> None:
        self.health_ok = False
        if poll_due_feeds.is_running():
            poll_due_feeds.cancel()
        await self.health_server.stop()
        if self.session and not self.session.closed:
            await self.session.close()
        self.db.close()
        await super().close()

    async def fetch_channel_safely(self, channel_id: int) -> discord.abc.Messageable | None:
        channel = self.get_channel(channel_id)
        if channel is not None:
            return channel
        with suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
            return await self.fetch_channel(channel_id)
        return None

    async def post_item(self, sub: sqlite3.Row, entry: dict[str, Any]) -> None:
        channel = await self.fetch_channel_safely(int(sub["channel_id"]))
        if channel is None:
            raise RuntimeError("Target channel not found or inaccessible")

        mention_role_id = sub["mention_role_id"]
        mention_text = f"<@&{mention_role_id}>\n" if mention_role_id else ""
        allowed_mentions = discord.AllowedMentions(roles=True)

        if str(sub["post_format"]) == "text":
            lines = [
                f"**{entry['title']}**",
                entry["link"] or "",
            ]
            if entry["summary"]:
                lines.append(entry["summary"])
            content = mention_text + "\n".join(line for line in lines if line)
            await channel.send(content=content[:2000], allowed_mentions=allowed_mentions)
            return

        embed = discord.Embed(
            title=entry["title"][:256],
            url=entry["link"] or None,
            description=entry["summary"] or "New feed item published.",
            timestamp=datetime.fromtimestamp(entry["timestamp"], tz=timezone.utc),
        )
        embed.set_footer(text=str(sub["feed_title"])[:2048])
        if entry["author"]:
            embed.set_author(name=entry["author"][:256])
        if entry["image"]:
            embed.set_thumbnail(url=entry["image"])
        await channel.send(content=mention_text or None, embed=embed, allowed_mentions=allowed_mentions)

    async def check_subscription(self, sub: sqlite3.Row) -> tuple[int, str]:
        assert self.fetcher is not None

        async with self.fetch_semaphore:
            sub_id = int(sub["id"])
            interval = int(sub["interval_minutes"])
            try:
                result = await self.fetcher.fetch(
                    str(sub["feed_url"]),
                    etag=sub["etag"],
                    modified=sub["modified"],
                )
            except Exception as exc:
                async with self.db_lock:
                    self.db.mark_checked(
                        sub_id,
                        interval_minutes=interval,
                        etag=None,
                        modified=None,
                        error=str(exc),
                        success=False,
                    )
                return 0, f"fetch failed: {exc}"

            if result.status == 304:
                async with self.db_lock:
                    self.db.mark_checked(
                        sub_id,
                        interval_minutes=interval,
                        etag=sub["etag"],
                        modified=sub["modified"],
                        feed_title=str(sub["feed_title"]),
                        error="",
                        success=True,
                    )
                return 0, "not modified"

            unseen = [item for item in result.entries if not self.db.is_seen(sub_id, item["uid"])]
            if len(unseen) > MAX_POSTS_PER_CHECK:
                unseen = unseen[-MAX_POSTS_PER_CHECK:]

            posted = 0
            for item in unseen:
                try:
                    await self.post_item(sub, item)
                    async with self.db_lock:
                        self.db.mark_seen(sub_id, item["uid"])
                    posted += 1
                except Exception as exc:
                    async with self.db_lock:
                        self.db.mark_checked(
                            sub_id,
                            interval_minutes=interval,
                            etag=result.etag,
                            modified=result.modified,
                            feed_title=result.title,
                            error=f"post failed: {exc}",
                            success=False,
                        )
                    return posted, f"post failed: {exc}"

            async with self.db_lock:
                self.db.mark_checked(
                    sub_id,
                    interval_minutes=interval,
                    etag=result.etag,
                    modified=result.modified,
                    feed_title=result.title,
                    error="",
                    success=True,
                )
            return posted, "ok"


bot = RSSBot()
feed_group = app_commands.Group(name="feed", description="Manage RSS and Atom feed subscriptions")
bot.tree.add_command(feed_group)


def require_guild(interaction: discord.Interaction) -> int:
    if interaction.guild_id is None:
        raise app_commands.CheckFailure("This command can only be used in a server.")
    return interaction.guild_id


def format_subscription_line(row: sqlite3.Row) -> str:
    status = "paused" if row["is_paused"] else "active"
    role = f"<@&{row['mention_role_id']}>" if row["mention_role_id"] else "none"
    return (
        f"`#{row['id']}` **{row['feed_title']}**\n"
        f"URL: {row['feed_url']}\n"
        f"Channel: <#{row['channel_id']}> | Every: {row['interval_minutes']}m | Format: {row['post_format']} | Role: {role}\n"
        f"Status: {status} | Last checked: {ts_to_display(row['last_checked_at'])}"
        + (f"\nLast error: `{row['last_error']}`" if row["last_error"] else "")
    )


@bot.event
async def on_ready() -> None:
    logger.info("Logged in as %s (%s)", bot.user, bot.user.id if bot.user else "unknown")


@tasks.loop(seconds=POLL_INTERVAL_SECONDS)
async def poll_due_feeds() -> None:
    await bot.wait_until_ready()
    now = utc_now_ts()
    async with bot.db_lock:
        due = bot.db.due_subscriptions(now)

    if not due:
        return

    logger.info("Checking %s due subscription(s)", len(due))
    await asyncio.gather(*(bot.check_subscription(sub) for sub in due), return_exceptions=True)


@poll_due_feeds.before_loop
async def before_poll_due_feeds() -> None:
    await bot.wait_until_ready()


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="help", description="Show feed bot usage help.")
async def feed_help(interaction: discord.Interaction) -> None:
    text = (
        "**Feed Bot Help**\n"
        "`/feed add` add a new feed subscription\n"
        "`/feed list` show every feed in this server\n"
        "`/feed edit` update channel, interval, role, or format\n"
        "`/feed pause` and `/feed resume` stop or restart checks\n"
        "`/feed check` run a manual check now\n"
        "`/feed remove` delete a subscription\n\n"
        "Post formats: `embed` or `text`\n"
        "The bot seeds current items on add so only future posts are sent."
    )
    await interaction.response.send_message(text, ephemeral=True)


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="list", description="List feed subscriptions for this server.")
async def feed_list(interaction: discord.Interaction) -> None:
    guild_id = require_guild(interaction)
    async with bot.db_lock:
        rows = bot.db.list_subscriptions(guild_id)

    if not rows:
        await interaction.response.send_message("No feeds are configured in this server yet.", ephemeral=True)
        return

    chunks: list[str] = []
    current = ""
    for row in rows:
        block = format_subscription_line(row) + "\n\n"
        if len(current) + len(block) > 1800:
            chunks.append(current)
            current = block
        else:
            current += block
    if current:
        chunks.append(current)

    await interaction.response.send_message(chunks[0], ephemeral=True)
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, ephemeral=True)


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="add", description="Add a new RSS/Atom feed subscription.")
@app_commands.describe(
    url="RSS or Atom feed URL",
    channel="Where updates should be posted. Defaults to this channel.",
    interval_minutes="How often to check the feed",
    mention_role="Optional role to mention for new posts",
    post_format="How feed items should be posted",
)
async def feed_add(
    interaction: discord.Interaction,
    url: str,
    channel: discord.TextChannel | None = None,
    interval_minutes: app_commands.Range[int, 1, 1440] = 10,
    mention_role: discord.Role | None = None,
    post_format: Literal["embed", "text"] = "embed",
) -> None:
    guild_id = require_guild(interaction)
    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        feed_url = valid_feed_url(url)
    except ValueError as exc:
        await interaction.followup.send(str(exc), ephemeral=True)
        return

    target_channel = channel or interaction.channel
    if not isinstance(target_channel, discord.TextChannel):
        await interaction.followup.send("Use this in a text channel or provide a text channel explicitly.", ephemeral=True)
        return

    assert bot.fetcher is not None
    try:
        result = await bot.fetcher.fetch(feed_url)
    except Exception as exc:
        await interaction.followup.send(f"I couldn't read that feed: `{exc}`", ephemeral=True)
        return

    async with bot.db_lock:
        try:
            subscription_id = bot.db.create_subscription(
                guild_id=guild_id,
                channel_id=target_channel.id,
                feed_url=feed_url,
                feed_title=result.title,
                interval_minutes=int(interval_minutes),
                mention_role_id=mention_role.id if mention_role else None,
                post_format=post_format,
                created_by=interaction.user.id,
            )
        except sqlite3.IntegrityError:
            await interaction.followup.send(
                "That same feed is already subscribed in that channel for this server.",
                ephemeral=True,
            )
            return

        seed = [item["uid"] for item in result.entries[-INITIAL_SEED_ITEMS:]]
        bot.db.seed_posted_items(subscription_id, seed)
        bot.db.mark_checked(
            subscription_id,
            interval_minutes=int(interval_minutes),
            etag=result.etag,
            modified=result.modified,
            feed_title=result.title,
            error="",
            success=True,
        )

    await interaction.followup.send(
        (
            f"Added subscription `#{subscription_id}` for **{result.title}**\n"
            f"Channel: {target_channel.mention}\n"
            f"Every: **{interval_minutes} minute(s)**\n"
            f"Format: **{post_format}**\n"
            f"Mention role: **{mention_role.mention if mention_role else 'none'}**\n"
            "Current feed items were seeded, so only new posts will be sent."
        ),
        ephemeral=True,
    )


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="remove", description="Remove a feed subscription.")
async def feed_remove(interaction: discord.Interaction, subscription_id: int) -> None:
    guild_id = require_guild(interaction)
    async with bot.db_lock:
        removed = bot.db.remove_subscription(subscription_id, guild_id)

    if removed:
        await interaction.response.send_message(f"Removed subscription `#{subscription_id}`.", ephemeral=True)
    else:
        await interaction.response.send_message("Subscription not found in this server.", ephemeral=True)


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="pause", description="Pause a feed subscription.")
async def feed_pause(interaction: discord.Interaction, subscription_id: int) -> None:
    guild_id = require_guild(interaction)
    async with bot.db_lock:
        updated = bot.db.set_paused(subscription_id, guild_id, True)

    if updated:
        await interaction.response.send_message(f"Paused subscription `#{subscription_id}`.", ephemeral=True)
    else:
        await interaction.response.send_message("Subscription not found in this server.", ephemeral=True)


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="resume", description="Resume a feed subscription.")
async def feed_resume(interaction: discord.Interaction, subscription_id: int) -> None:
    guild_id = require_guild(interaction)
    async with bot.db_lock:
        updated = bot.db.set_paused(subscription_id, guild_id, False)

    if updated:
        await interaction.response.send_message(
            f"Resumed subscription `#{subscription_id}`. It will be checked shortly.",
            ephemeral=True,
        )
    else:
        await interaction.response.send_message("Subscription not found in this server.", ephemeral=True)


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="edit", description="Edit an existing feed subscription.")
@app_commands.describe(
    channel="New text channel",
    interval_minutes="New check interval",
    mention_role="New mention role. Leave empty to keep current.",
    post_format="New post format",
    clear_mention_role="Set true to remove the current mention role",
)
async def feed_edit(
    interaction: discord.Interaction,
    subscription_id: int,
    channel: discord.TextChannel | None = None,
    interval_minutes: app_commands.Range[int, 1, 1440] | None = None,
    mention_role: discord.Role | None = None,
    post_format: Literal["embed", "text"] | None = None,
    clear_mention_role: bool = False,
) -> None:
    guild_id = require_guild(interaction)

    if mention_role is not None and clear_mention_role:
        await interaction.response.send_message("Choose a new role or clear the role, not both.", ephemeral=True)
        return

    mention_role_value: int | None | Literal[False]
    if clear_mention_role:
        mention_role_value = None
    elif mention_role is not None:
        mention_role_value = mention_role.id
    else:
        mention_role_value = False

    async with bot.db_lock:
        updated = bot.db.edit_subscription(
            subscription_id,
            guild_id,
            channel_id=channel.id if channel else None,
            interval_minutes=int(interval_minutes) if interval_minutes is not None else None,
            mention_role_id=mention_role_value,
            post_format=post_format,
        )
        row = bot.db.get_subscription(subscription_id, guild_id)

    if not updated or row is None:
        await interaction.response.send_message("Subscription not found in this server.", ephemeral=True)
        return

    await interaction.response.send_message(
        f"Updated subscription `#{subscription_id}`.\n\n{format_subscription_line(row)}",
        ephemeral=True,
    )


@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@feed_group.command(name="check", description="Check one feed or all feeds now.")
async def feed_check(interaction: discord.Interaction, subscription_id: int | None = None) -> None:
    guild_id = require_guild(interaction)
    await interaction.response.defer(ephemeral=True, thinking=True)

    async with bot.db_lock:
        if subscription_id is None:
            rows = bot.db.list_subscriptions(guild_id)
        else:
            row = bot.db.get_subscription(subscription_id, guild_id)
            rows = [row] if row is not None else []

    if not rows:
        await interaction.followup.send("No matching subscriptions found.", ephemeral=True)
        return

    results = await asyncio.gather(*(bot.check_subscription(row) for row in rows))
    total_posted = sum(posted for posted, _ in results)
    details = "\n".join(
        f"`#{row['id']}` {row['feed_title']}: posted {posted} ({status})"
        for row, (posted, status) in zip(rows, results)
    )
    await interaction.followup.send(
        f"Manual check complete. Total posted: **{total_posted}**\n\n{details}",
        ephemeral=True,
    )


@feed_add.error
@feed_list.error
@feed_remove.error
@feed_pause.error
@feed_resume.error
@feed_edit.error
@feed_check.error
@feed_help.error
async def feed_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    logger.warning("App command error: %s", error)
    message = "Something went wrong while running that command."

    if isinstance(error, app_commands.MissingPermissions):
        message = "You need the **Manage Server** permission to use this command."
    elif isinstance(error, app_commands.CheckFailure):
        message = str(error) or "This command can't be used here."
    elif isinstance(error, app_commands.CommandInvokeError) and error.original:
        message = f"Command failed: `{error.original}`"

    if interaction.response.is_done():
        await interaction.followup.send(message, ephemeral=True)
    else:
        await interaction.response.send_message(message, ephemeral=True)


def install_signal_handlers() -> None:
    loop = asyncio.get_event_loop()

    def _stop() -> None:
        logger.info("Shutdown signal received")
        asyncio.create_task(bot.close())

    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _stop)


async def main() -> None:
    if not DISCORD_TOKEN:
        raise RuntimeError("Set the DISCORD_TOKEN environment variable before starting the bot.")

    install_signal_handlers()
    async with bot:
        await bot.start(DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
