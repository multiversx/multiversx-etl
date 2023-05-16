from datetime import datetime, timezone
from typing import Any, Dict, Protocol, Tuple, Union

from blockchainetl_common.graph.graph_operations import OutOfBoundsError

MAX_NUM_BLOCKS_LOOKAHEAD = 8


class INetworkProvider(Protocol):
    def get_genesis_timestamp(self) -> int: ...
    def get_round_duration(self) -> float: ...
    def get_hyperblock_by_round(self, round: int) -> Union[Dict[str, Any], None]: ...


class MultiversXBlocksService:
    def __init__(self, network_provider: INetworkProvider) -> None:
        self.provider = network_provider

    def get_hyperblock_range_for_date(self, date: datetime) -> Tuple[int, int]:
        start_datetime = datetime.combine(date, datetime.min.time().replace(tzinfo=timezone.utc))
        end_datetime = datetime.combine(date, datetime.max.time().replace(tzinfo=timezone.utc))
        start_timestamp = int(start_datetime.timestamp())
        end_timestamp = int(end_datetime.timestamp())

        return self.get_hyperblock_range_for_timestamps(start_timestamp, end_timestamp)

    def get_hyperblock_range_for_timestamps(self, start_timestamp: int, end_timestamp: int) -> Tuple[int, int]:
        if start_timestamp > end_timestamp:
            raise ValueError("'start_timestamp' must be greater or equal to 'end_timestamp'")

        start_hyperblock = self._get_hyperblock_by_timestamp(start_timestamp)
        end_hyperblock = self._get_hyperblock_by_timestamp(end_timestamp)

        if not start_hyperblock and not end_hyperblock:
            raise OutOfBoundsError("The given time range does not cover any hyperblocks")

        if not start_hyperblock or not end_hyperblock:
            raise OutOfBoundsError("The existing hyperblocks do not completely cover the given time range")

        return start_hyperblock["nonce"], end_hyperblock["nonce"]

    def _get_hyperblock_by_timestamp(self, timestamp: int):
        round = self._get_round_by_timestamp(timestamp)

        for _ in range(0, MAX_NUM_BLOCKS_LOOKAHEAD):
            block = self.provider.get_hyperblock_by_round(round)
            if block:
                return block

    def _get_round_by_timestamp(self, timestamp: int):
        genesis_timestamp = self.provider.get_genesis_timestamp()
        delta = timestamp - genesis_timestamp
        round = int(delta / self.provider.get_round_duration())
        return round
