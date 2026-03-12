"""Main orchestrator coordinating the entire workflow."""

import asyncio
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict
from urllib.parse import urlparse
import httpx
from rich.console import Console

from .models import Config, ContentItem
from .storage.manager import StorageManager
from .services.emailer import EmailManager
from .scrapers.github import GitHubScraper
from .scrapers.hackernews import HackerNewsScraper
from .scrapers.rss import RSSScraper
from .scrapers.reddit import RedditScraper
from .scrapers.telegram import TelegramScraper
from .ai.client import create_ai_client
from .ai.analyzer import ContentAnalyzer
from .ai.summarizer import DailySummarizer
from .ai.enricher import ContentEnricher


class HorizonOrchestrator:
    def __init__(self, config: Config, storage: StorageManager):
        self.config = config
        self.storage = storage
        self.console = Console()
        self.email_manager = EmailManager(config.email, console=self.console) if config.email else None

    async def run(self, force_hours: int = None) -> None:
        self.console.print("[bold cyan]🌅 Horizon - Starting aggregation...[/bold cyan]\n")

        if self.email_manager and self.config.email and self.config.email.enabled:
            self.console.print("📧 Checking for new email subscriptions...")
            self.email_manager.check_subscriptions(self.storage)

        try:
            since = self._determine_time_window(force_hours)
            self.console.print(f"📅 Fetching content since: {since.strftime('%Y-%m-%d %H:%M:%S')}\n")

            all_items = await self.fetch_all_sources(since)
            self.console.print(f"📥 Fetched {len(all_items)} items from all sources\n")

            if not all_items:
                self.console.print("[yellow]No new content found. Exiting.[/yellow]")
                return

            merged_items = self.merge_cross_source_duplicates(all_items)
            
            # --- БЫСТРЫЙ АНАЛИЗ ---
            analyzed_items = await self._analyze_content(merged_items)
            self.console.print(f"🤖 Analyzed {len(analyzed_items)} items with AI\n")

            threshold = self.config.filtering.ai_score_threshold
            important_items = [
                item for item in analyzed_items
                if item.ai_score and item.ai_score >= threshold
            ]
            important_items.sort(key=lambda x: x.ai_score or 0, reverse=True)

            deduped_items = self.merge_topic_duplicates(important_items)
            important_items = deduped_items

            # --- БЫСТРОЕ ОБОГАЩЕНИЕ ---
            await self._enrich_important_items(important_items)

            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            for lang in self.config.ai.languages:
                summary = await self._generate_summary(important_items, today, len(all_items), language=lang)
                self.storage.save_daily_summary(today, summary, language=lang)

                try:
                    from pathlib import Path
                    post_filename = f"{today}-summary-{lang}.md"
                    posts_dir = Path("docs/_posts")
                    posts_dir.mkdir(parents=True, exist_ok=True)
                    dest_path = posts_dir / post_filename

                    front_matter = (
                        "---\n"
                        "layout: default\n"
                        f"title: \"Horizon Summary: {today} ({lang.upper()})\"\n"
                        f"date: {today}\n"
                        f"lang: {lang}\n"
                        "---\n\n"
                    )

                    summary_content = summary
                    if summary_content.strip().startswith("# "):
                        parts = summary_content.split("\n", 1)
                        if len(parts) > 1:
                            summary_content = parts[1].strip()

                    with open(dest_path, "w", encoding="utf-8") as f:
                        f.write(front_matter + summary_content)

                except Exception as e:
                    self.console.print(f"[yellow]⚠️  Failed to copy {lang.upper()} summary to docs/: {e}[/yellow]\n")

            self.console.print("[bold green]✅ Horizon completed successfully![/bold green]")

        except Exception as e:
            self.console.print(f"[bold red]❌ Error: {e}[/bold red]")
            raise

    def _determine_time_window(self, force_hours: int = None) -> datetime:
        if force_hours:
            since = datetime.now(timezone.utc) - timedelta(hours=force_hours)
        else:
            hours = self.config.filtering.time_window_hours
            since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return since

    async def fetch_all_sources(self, since: datetime) -> List[ContentItem]:
        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = []
            if self.config.sources.github:
                tasks.append(self._fetch_with_progress("GitHub", GitHubScraper(self.config.sources.github, client), since))
            if self.config.sources.hackernews.enabled:
                tasks.append(self._fetch_with_progress("Hacker News", HackerNewsScraper(self.config.sources.hackernews, client), since))
            if self.config.sources.rss:
                tasks.append(self._fetch_with_progress("RSS Feeds", RSSScraper(self.config.sources.rss, client), since))
            if self.config.sources.reddit.enabled:
                tasks.append(self._fetch_with_progress("Reddit", RedditScraper(self.config.sources.reddit, client), since))
            if self.config.sources.telegram.enabled:
                tasks.append(self._fetch_with_progress("Telegram", TelegramScraper(self.config.sources.telegram, client), since))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            all_items = []
            for result in results:
                if isinstance(result, list):
                    all_items.extend(result)
            return all_items

    async def _fetch_with_progress(self, name: str, scraper, since: datetime) -> List[ContentItem]:
        self.console.print(f"🔍 Fetching from {name}...")
        return await scraper.fetch(since)

    def merge_cross_source_duplicates(self, items: List[ContentItem]) -> List[ContentItem]:
        def normalize_url(url: str) -> str:
            parsed = urlparse(str(url))
            host = parsed.hostname or ""
            if host.startswith("www."): host = host[4:]
            return f"{host}{parsed.path.rstrip('/')}"

        url_groups = {}
        for item in items:
            key = normalize_url(str(item.url))
            url_groups.setdefault(key, []).append(item)

        merged = []
        for key, group in url_groups.items():
            if len(group) == 1:
                merged.append(group[0])
                continue
            primary = max(group, key=lambda x: len(x.content or ""))
            merged.append(primary)
        return merged

    def merge_topic_duplicates(self, items: List[ContentItem], threshold: float = 0.33) -> List[ContentItem]:
        return items 

    async def _enrich_important_items(self, items: List[ContentItem]) -> None:
        if not items: return
        self.console.print("📚 Enriching with background knowledge...")
        ai_client = create_ai_client(self.config.ai)
        enricher = ContentEnricher(ai_client)
        await enricher.enrich_batch(items)
        self.console.print(f"   Enriched {len(items)} items\n")

    async def _analyze_content(self, items: List[ContentItem]) -> List[ContentItem]:
        self.console.print("🤖 Analyzing content with AI...")
        ai_client = create_ai_client(self.config.ai)
        analyzer = ContentAnalyzer(ai_client)
        return await analyzer.analyze_batch(items)

    async def _generate_summary(self, items: List[ContentItem], date: str, total_fetched: int, language: str = "en") -> str:
        summarizer = DailySummarizer()
        return await summarizer.generate_summary(items, date, total_fetched, language=language)
