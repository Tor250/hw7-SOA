from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
import logging

import boto3
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential

from src.analytics.repository import AnalyticsRepository
from src.analytics.settings import AnalyticsSettings


LOGGER = logging.getLogger(__name__)


class S3Exporter:
    def __init__(self, settings: AnalyticsSettings, repository: AnalyticsRepository) -> None:
        self.settings = settings
        self.repository = repository
        self.s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4"),
        )

    def default_export_date(self) -> date:
        return (datetime.now(tz=timezone.utc) - timedelta(days=1)).date()

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10), reraise=True)
    def export_date(self, target_date: date) -> str:
        payload = self.repository.export_payload_for_date(target_date)
        payload["exported_at"] = datetime.now(tz=timezone.utc).isoformat()
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        key = f"daily/{target_date.isoformat()}/aggregates.json"
        self.s3.put_object(
            Bucket=self.settings.s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
        )
        LOGGER.info("Exported aggregates for %s to s3://%s/%s", target_date, self.settings.s3_bucket, key)
        return key
