

from typing import List

from multiversxetl.domain.transaction import MultiversXTransaction


class MultiversXHyperblock:
    def __init__(self) -> None:
        self.hash: str = ""
        self.previous_block_hash: str = ""
        self.state_root_hash: str = ""

        self.round: int = 0
        self.nonce: int = 0
        self.epoch: int = 0
        self.timestamp: int = 0

        self.num_transactions: int = 0
        self.transactions: List[MultiversXTransaction] = []
