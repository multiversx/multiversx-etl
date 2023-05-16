import concurrent.futures
from functools import lru_cache
from typing import Any, Dict, List, Optional

from multiversx_sdk_core.constants import METACHAIN_ID
from multiversx_sdk_network_providers import (GenericResponse,
                                              ProxyNetworkProvider)
from multiversx_sdk_network_providers.network_config import NetworkConfig


class MultiversXNetworkProvider(ProxyNetworkProvider):
    def __init__(self, url: str) -> None:
        super().__init__(url)

    def get_hyperblock_nonce_by_round(self, round: int) -> Optional[int]:
        response = self.do_get_generic(f"blocks/by-round/{round}")
        blocks = response.get("blocks")
        metablock = next((block for block in blocks if block.get("shard") == METACHAIN_ID), None)
        if metablock:
            return metablock.get("nonce")

    def get_hyperblocks_by_nonces(self, nonces: List[int]) -> List[Dict[str, Any]]:
        NUM_PARALLEL_REQUESTS = 8

        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_PARALLEL_REQUESTS) as executor:
            futures = [executor.submit(self.get_hyperblock_by_nonce, nonce) for nonce in nonces]
            hyperblocks = [future.result() for future in concurrent.futures.as_completed(futures)]
            return hyperblocks

    def get_hyperblock_by_nonce(self, nonce: int) -> Dict[str, Any]:
        response = self.do_get_generic(f"hyperblock/by-nonce/{nonce}")
        hyperblock = response.get("hyperblock")
        return hyperblock

    @lru_cache()
    def get_genesis_timestamp(self) -> int:
        return self.get_network_config().start_time

    @lru_cache()
    def get_round_duration(self) -> float:
        round_duration = self.get_network_config().round_duration
        return round_duration / 1000

    def get_network_config(self) -> NetworkConfig:
        return self._get_network_config_cached()

    @lru_cache()
    def _get_network_config_cached(self) -> NetworkConfig:
        return super().get_network_config()

    def do_get_generic(self, resource_url: str) -> GenericResponse:
        response = super().do_get_generic(resource_url)
        return response
