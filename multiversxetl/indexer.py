from typing import Any, Dict, Iterable, List, Optional, Tuple

import elasticsearch.helpers
from elasticsearch import Elasticsearch

from multiversxetl.constants import (ELASTICSEARCH_CONNECTIONS_PER_NODE,
                                     ELASTICSEARCH_MAX_RETRIES)

SCROLL_CONSISTENCY_TIME = "10m"
SCAN_BATCH_SIZE = 7500
TIMESTAMP_FIELD_IN_SECONDS = "timestamp"
TIMESTAMP_FIELD_IN_MILLISECONDS = "timestampMs"


class Indexer:
    def __init__(
            self,
            url: str,
            username: str = "",
            password: str = "",
            indices_with_millisecond_timestamp: Optional[List[str]] = None
    ):
        basic_auth = (username, password) if username and password else None

        # Some indices (e.g. "executionresults") do not hold a "timestamp" field at all,
        # only "timestampMs" (mapped as a date with format "epoch_millis").
        # For those, we must range-query the millisecond field instead.
        self.indices_with_millisecond_timestamp = set(indices_with_millisecond_timestamp or [])

        self.elastic_search_client = Elasticsearch(
            url,
            max_retries=ELASTICSEARCH_MAX_RETRIES,
            retry_on_timeout=True,
            connections_per_node=ELASTICSEARCH_CONNECTIONS_PER_NODE,
            basic_auth=basic_auth
        )

    def count_records(self, index_name: str, start_timestamp: int, end_timestamp: int) -> int:
        query = self._get_query_object(index_name, start_timestamp, end_timestamp)
        return self.elastic_search_client.count(index=index_name, query=query["query"])["count"]

    def get_records(
            self,
            index_name: str,
            start_timestamp: Optional[int] = None,
            end_timestamp: Optional[int] = None
    ) -> Iterable[Dict[str, Any]]:
        query = self._get_query_object(index_name, start_timestamp, end_timestamp)

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

    def _get_query_object(self, index_name: str, start_timestamp: Optional[int], end_timestamp: Optional[int]) -> Dict[str, Any]:
        if start_timestamp is None and end_timestamp is None:
            return {
                "query": {
                    "match_all": {},
                }
            }

        field, factor = self._get_timestamp_field_and_factor(index_name)

        return {
            "query": {
                "range": {
                    field: {
                        "gte": str(start_timestamp * factor),
                        "lt": str(end_timestamp * factor),
                    },
                }
            }
        }

    def _get_timestamp_field_and_factor(self, index_name: str) -> Tuple[str, int]:
        """
        Task intervals are always expressed in seconds. Returns the field to range-query,
        along with the factor to apply to the (seconds-based) interval bounds.
        """
        if index_name in self.indices_with_millisecond_timestamp:
            return TIMESTAMP_FIELD_IN_MILLISECONDS, 1000
        return TIMESTAMP_FIELD_IN_SECONDS, 1
