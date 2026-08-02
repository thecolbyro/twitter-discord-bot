"""
Discord bot that watches specific X/Twitter accounts and, once per hour on
the hour, pings a Discord user for every tweet/retweet from the last hour
that mentions a configured keyword -- and keeps a running score of how many
times each person has done so since the bot was added.

Setup: see README.md
"""

import json
import os
import logging
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
import discord
from discord.ext import tasks, commands
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "config.json"
STATE_PATH = BASE_DIR / "state.json"
ENV_PATH = BASE_DIR / ".env"

# Load explicitly from the script's own folder, not whatever folder the
# terminal happens to be in -- and check the file actually exists first so
# we can give a clear error instead of a raw KeyError traceback.
if not ENV_PATH.exists():
    raise SystemExit(
        f"Could not find {ENV_PATH}.\n"
        f"Run 'dir /a' (Windows) or 'ls -a' (Mac/Linux) in this folder to check "
        f"the file is actually named '.env' and not '.env.txt' or '.env.example'."
    )
load_dotenv(ENV_PATH)


def require_env(key):
    value = os.environ.get(key)
    if not value or "your-" in value.lower():
        raise SystemExit(
            f"'{key}' is missing or still set to its placeholder value in {ENV_PATH}.\n"
            f"Open .env and make sure the line looks like '{key}=<your real value>' "
            f"with no quotes and no leftover placeholder text."
        )
    return value


DISCORD_BOT_TOKEN = require_env("DISCORD_BOT_TOKEN")
DISCORD_CHANNEL_ID = int(require_env("DISCORD_CHANNEL_ID"))
X_BEARER_TOKEN = require_env("X_BEARER_TOKEN")

X_API_BASE = "https://api.x.com/2"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("twitter-watch-bot")


def load_json(path, default):
    if not path.exists():
        return default
    with open(path, "r") as f:
        return json.load(f)


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def x_api_get(path, params=None):
    """Thin wrapper around the X API v2 with auth + basic error handling."""
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    resp = requests.get(f"{X_API_BASE}{path}", headers=headers, params=params or {})
    if resp.status_code == 429:
        log.warning("Rate limited by X API on %s. Response: %s", path, resp.text)
        return None
    if not resp.ok:
        log.error("X API error %s on %s: %s", resp.status_code, path, resp.text)
        return None
    return resp.json()


def resolve_user_id(username, state):
    """Look up a Twitter user's numeric ID, caching it in state.json.

    This costs one 'user read' the first time and is free forever after,
    since we cache it.
    """
    cache = state.setdefault("user_ids", {})
    key = username.lower()
    if key in cache:
        return cache[key]

    data = x_api_get(f"/users/by/username/{username}")
    if not data or "data" not in data:
        log.error("Could not resolve X user id for @%s: %s", username, data)
        return None

    user_id = data["data"]["id"]
    cache[key] = user_id
    save_json(STATE_PATH, state)
    log.info("Resolved @%s -> user id %s", username, user_id)
    return user_id


def establish_baseline(username, user_id, state):
    """On first-ever run for an account, record its current newest tweet id
    WITHOUT scoring anything, so scoring only counts tweets from the point
    the bot was added forward -- not its whole tweet history.
    """
    since_map = state.setdefault("since_id", {})
    if username.lower() in since_map:
        return  # already have a baseline

    data = x_api_get(f"/users/{user_id}/tweets", {"max_results": 5, "exclude": "replies"})
    tweets = (data or {}).get("data", [])
    if tweets:
        since_map[username.lower()] = tweets[0]["id"]
        log.info("Established baseline for @%s at tweet %s (not scored)", username, tweets[0]["id"])
    else:
        # No tweets yet; use a placeholder so we don't re-baseline every poll.
        since_map[username.lower()] = "0"
    save_json(STATE_PATH, state)


def get_full_text(tweet, includes_by_id):
    """Return the text that should actually be keyword-matched.

    Retweets come back with truncated text like 'RT @user: partial text...'
    so if the tweet references a retweeted (or quoted) tweet, we pull the
    full text of that referenced tweet from the `includes` payload instead.
    """
    texts = [tweet.get("text", "")]
    for ref in tweet.get("referenced_tweets", []):
        ref_tweet = includes_by_id.get(ref["id"])
        if ref_tweet:
            texts.append(ref_tweet.get("text", ""))
    return "\n".join(texts)


def matches_any_keyword(text, keywords):
    """Case-insensitive, mid-word substring match against a LIST of keywords
    that all represent the same underlying topic (e.g. "hasan" and "piker"
    both mean Hasan Piker). Returns True if ANY of them appear -- used so a
    tweet containing multiple synonyms for the same topic only counts once.
    """
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def fetch_recent_tweets(user_id, since_id):
    params = {
        "max_results": 20,  # covers a busy hour; still cheap since since_id trims most polls
        "exclude": "replies",
        "tweet.fields": "created_at,referenced_tweets,author_id",
        "expansions": "referenced_tweets.id,referenced_tweets.id.author_id",
    }
    if since_id and since_id != "0":
        params["since_id"] = since_id

    data = x_api_get(f"/users/{user_id}/tweets", params)
    if not data:
        return [], {}

    tweets = data.get("data", [])
    includes_by_id = {t["id"]: t for t in data.get("includes", {}).get("tweets", [])}
    return tweets, includes_by_id


def tweet_url(username, tweet_id):
    # fxtwitter.com mirrors x.com but reliably unfurls a real Discord embed
    # (card, text, video) where plain x.com links often fail to.
    return f"https://fxtwitter.com/{username}/status/{tweet_id}"


def seconds_until_next_hour():
    now = datetime.now(timezone.utc)
    next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return (next_hour - now).total_seconds()


class TwitterWatchBot(commands.Bot):
    def __init__(self, config, state):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)
        self.config = config
        self.state = state

    async def setup_hook(self):
        self.poll_accounts.start()

    async def on_ready(self):
        log.info("Logged in as %s", self.user)

    @tasks.loop(hours=1)
    async def poll_accounts(self):
        channel = self.get_channel(DISCORD_CHANNEL_ID)
        if channel is None:
            log.error(
                "Could not find Discord channel %s. Is the bot in that server "
                "with access to the channel?",
                DISCORD_CHANNEL_ID,
            )
            return

        # Group watches by twitter account so we only poll each account once
        accounts = {}
        for watch in self.config["watches"]:
            accounts.setdefault(watch["twitter_username"], []).append(watch)

        for username, watches in accounts.items():
            user_id = resolve_user_id(username, self.state)
            if not user_id:
                continue

            establish_baseline(username, user_id, self.state)
            since_id = self.state["since_id"].get(username.lower())
            tweets, includes_by_id = fetch_recent_tweets(user_id, since_id)

            if not tweets:
                continue

            # X returns newest first; process oldest->newest so alerts land
            # in chronological order, then remember the newest id we saw.
            for tweet in reversed(tweets):
                full_text = get_full_text(tweet, includes_by_id)
                is_retweet = any(
                    r["type"] == "retweeted" for r in tweet.get("referenced_tweets", [])
                )
                kind = "retweeted" if is_retweet else "tweeted"

                for watch in watches:
                    # Collect every topic this tweet matches for this watch
                    # FIRST, so multiple matches (e.g. a tweet mentioning both
                    # "hasan" and "piker") produce one combined message
                    # instead of one message per topic.
                    hits = []
                    for topic in watch["topics"]:
                        if not matches_any_keyword(full_text, topic["keywords"]):
                            continue

                        topic_label = topic.get("label", "/".join(topic["keywords"]))
                        score_key = f"{username.lower()}::{topic_label.lower()}"
                        scores = self.state.setdefault("scores", {})
                        scores[score_key] = scores.get(score_key, 0) + 1
                        count = scores[score_key]
                        hits.append((topic_label, count))

                    if not hits:
                        continue

                    save_json(STATE_PATH, self.state)

                    mention_id = watch["discord_user_id"]
                    url = tweet_url(username, tweet["id"])
                    source_label = watch.get("source_label", username)

                    clauses = []
                    for topic_label, count in hits:
                        plural = "time" if count == 1 else "times"
                        clauses.append(f"about {topic_label} {count} {plural}")
                    if len(clauses) == 1:
                        combined = clauses[0]
                    else:
                        combined = ", ".join(clauses[:-1]) + " and " + clauses[-1]

                    await channel.send(
                        f"<@{mention_id}>\n"
                        f"{url}\n"
                        f'"{source_label} has now {kind} {combined}!"'
                    )
                    log.info(
                        "Alerted %s for @%s tweet %s (topics: %s)",
                        mention_id, username, tweet["id"],
                        [t for t, _ in hits],
                    )

            newest_id = tweets[0]["id"]  # tweets[0] is newest (API default order)
            self.state["since_id"][username.lower()] = newest_id
            save_json(STATE_PATH, self.state)

    @poll_accounts.before_loop
    async def before_poll(self):
        await self.wait_until_ready()
        wait_seconds = seconds_until_next_hour()
        log.info("Waiting %.0f seconds to align to the top of the hour", wait_seconds)
        await asyncio.sleep(wait_seconds)


def main():
    config = load_json(CONFIG_PATH, None)
    if config is None:
        raise SystemExit(f"Missing {CONFIG_PATH}. Copy config.example.json to config.json and edit it.")

    state = load_json(STATE_PATH, {})

    bot = TwitterWatchBot(config, state)
    bot.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()