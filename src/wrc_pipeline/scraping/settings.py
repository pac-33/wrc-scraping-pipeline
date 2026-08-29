"""Scrapy settings, derived from the application Settings (env-driven).

Politeness model: AutoThrottle adapts the request rate to observed latency
(and never speeds up after non-200s), DOWNLOAD_DELAY is its floor, and
CONCURRENT_REQUESTS_PER_DOMAIN is the hard cap — adaptive in the middle,
bounded on both ends. LOG_ENABLED=False stops Scrapy installing its own
plain-text root handler; its loggers still propagate into our JSON handler.
"""

import scrapy

from wrc_pipeline.config import get_settings
from wrc_pipeline.logging import setup_logging

_settings = get_settings()
setup_logging(_settings.log_level, _settings.log_format)

BOT_NAME = "wrc_pipeline"
SPIDER_MODULES = ["wrc_pipeline.scraping.spiders"]

USER_AGENT = (
    f"wrc-decisions-scraper/0.1 "
    f"(contact: {_settings.scraper.contact_email}) Scrapy/{scrapy.__version__}"
)

ROBOTSTXT_OBEY = True
COOKIES_ENABLED = False

CONCURRENT_REQUESTS_PER_DOMAIN = _settings.scraper.max_concurrent_requests_per_domain
DOWNLOAD_DELAY = _settings.scraper.download_delay_floor_seconds
DOWNLOAD_TIMEOUT = _settings.scraper.download_timeout_seconds

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_TARGET_CONCURRENCY = _settings.scraper.target_concurrency
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 60.0

RETRY_ENABLED = True
RETRY_TIMES = _settings.scraper.retry_times

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
    "wrc_pipeline.scraping.middlewares.RetryAfterBackoffMiddleware": 550,
}

ITEM_PIPELINES = {
    "wrc_pipeline.scraping.pipelines.HashingPipeline": 100,
    "wrc_pipeline.scraping.pipelines.PersistencePipeline": 200,
}

# Our structlog root handler renders all log records as JSON lines;
# Scrapy must not install its own plain-text handler on top.
LOG_ENABLED = False

HTTPCACHE_ENABLED = _settings.scraper.dev_http_cache
HTTPCACHE_EXPIRATION_SECS = 0
HTTPCACHE_DIR = "httpcache"

FEED_EXPORT_ENCODING = "utf-8"
