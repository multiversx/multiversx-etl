from typing import Any, Dict, Iterable, Optional

import elasticsearch.helpers
from elasticsearch import Elasticsearch

from multiversxetl.constants import (ELASTICSEARCH_CONNECTIONS_PER_NODE,
                                     ELASTICSEARCH_MAX_RETRIES)

SCROLL_CONSISTENCY_TIME = "10m"
SCAN_BATCH_SIZE = 7500


class Indexer:
    def __init__(self, url: str, username: str = "", password: str = ""):
        basic_auth = (username, password) if username and password else None

        self.elastic_search_client = Elasticsearch(
            url,
            max_retries=ELASTICSEARCH_MAX_RETRIES,
            retry_on_timeout=True,
            connections_per_node=ELASTICSEARCH_CONNECTIONS_PER_NODE,
            basic_auth=basic_auth
        )

    def count_records(self, index_name: str, start_timestamp: int, end_timestamp: int) -> int:
        query = self._get_query_object(start_timestamp, end_timestamp)
        return self.elastic_search_client.count(index=index_name, query=query["query"])["count"]

    def get_records(
            self,
            index_name: str,
            start_timestamp: Optional[int] = None,
            end_timestamp: Optional[int] = None
    ) -> Iterable[Dict[str, Any]]:
        query = self._get_query_object(start_timestamp, end_timestamp)

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

    @staticmethod
    def _get_query_object(start_timestamp: Optional[int], end_timestamp: Optional[int]) -> Dict[str, Any]:
        if start_timestamp is None and end_timestamp is None:
            return {
                "query": {
                    "match_all": {},
                }
            }

        return {
            "query": {
                "range": {
                    "timestamp": {
                        "gte": str(start_timestamp),
                        "lt": str(end_timestamp),
                    },
                }
            }
        }
