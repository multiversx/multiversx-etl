from typing import Any, Dict, Iterable

import elasticsearch.helpers
from elasticsearch import Elasticsearch

SCROLL_CONSISTENCY_TIME = "10m"
SCAN_BATCH_SIZE = 9999


class Indexer:
    def __init__(self, url: str):
        self.elastic_search_client = Elasticsearch(url)

    def count_records_with_interval(self, index_name: str) -> int:
        return self.elastic_search_client.count(index=index_name)["count"]

    def count_records_without_interval(self, index_name: str) -> int:
        return self.elastic_search_client.count(index=index_name)["count"]

    def get_records_with_interval(self, index_name: str, start_timestamp: int, end_timestamp: int) -> Iterable[Dict[str, Any]]:
        query = {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": start_timestamp,
                        "lte": end_timestamp,
                    },
                }
            }
        }

        records = elasticsearch.helpers.scan(
            client=self.elastic_search_client,
            index=index_name,
            query=query,
            scroll=SCROLL_CONSISTENCY_TIME,
            raise_on_error=True,
            preserve_order=False,
            size=SCAN_BATCH_SIZE,
            request_timeout=None,
            scroll_kwargs=None,
            clear_scroll=True
        )

        return records

    def get_records_without_interval(self, index_name: str) -> Iterable[Dict[str, Any]]:
        query: Any = {
            "query": {
                "match_all": {},
            }
        }

        records = elasticsearch.helpers.scan(
            client=self.elastic_search_client,
            index=index_name,
            query=query,
            scroll=SCROLL_CONSISTENCY_TIME,
            raise_on_error=True,
            preserve_order=False,
            size=SCAN_BATCH_SIZE,
            request_timeout=None,
            scroll_kwargs=None,
            clear_scroll=True
        )

        return records
