from typing import Any, Dict

from multiversxetl.domain import MultiversXHyperblock
from multiversxetl.mappers.transaction_mapper import \
    MultiversXTransactionMapper


class MultiversXHyperblockMapper:
    def __init__(self):
        self.transaction_mapper = MultiversXTransactionMapper()

    def from_json_dict(self, json_dict: Dict[str, Any]) -> MultiversXHyperblock:
        hyperblock = MultiversXHyperblock()

        hyperblock.hash = json_dict.get("hash", "")
        hyperblock.previous_block_hash = json_dict.get("prevBlockHash", "")
        hyperblock.state_root_hash = json_dict.get("stateRootHash", "")

        hyperblock.round = json_dict.get("round", 0)
        hyperblock.nonce = json_dict.get("nonce", 0)
        hyperblock.epoch = json_dict.get("epoch", 0)
        hyperblock.timestamp = json_dict.get("timestamp", 0)

        hyperblock.num_transactions = json_dict.get("num_transactions", 0)
        hyperblock.transactions = []

        for transaction_json_dict in json_dict.get("transactions", []):
            transaction = self.transaction_mapper.from_json_dict(transaction_json_dict)
            hyperblock.transactions.append(transaction)

        return hyperblock

    def to_json_dict(self, hyperblock: MultiversXHyperblock) -> Dict[str, Any]:
        return {
            "type": "hyperblock",

            "hash": hyperblock.hash,
            "previous_block_hash": hyperblock.previous_block_hash,
            "state_root_hash": hyperblock.state_root_hash,
            "round": hyperblock.round,
            "nonce": hyperblock.nonce,
            "epoch": hyperblock.epoch,
            "timestamp": hyperblock.timestamp,
            "num_transactions": hyperblock.num_transactions
        }
