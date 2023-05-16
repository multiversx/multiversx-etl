class MultiversXTransaction():
    def __init__(self) -> None:
        self.hash: str = ""
        self.previous_transaction_hash: str = ""
        self.original_transaction_hash: str = ""

        self.type: str = ""
        self.processing_type_on_source: str = ""
        self.processing_type_on_destination: str = ""

        self.nonce: int = 0
        self.round: int = 0
        self.epoch: int = 0
        self.timestamp: int = 0

        self.sender: str = ""
        self.receiver: str = ""
        self.value: int = 0
        self.gas_limit: int = 0
        self.gas_price: int = 0
        self.data: str = ""
        self.version: int = 0
        self.options: int = 0
        self.chain_id: int = 0
        self.signature: str = ""

        self.status: str = ""
        self.initially_paid_fee: int = 0

        self.source_shard: int = 0
        self.destination_shard: int = 0
        self.miniblock_type: str = ""
        self.miniblock_hash: str = ""

        self.operation: str = ""
        self.function: str = ""
        self.call_type: str = ""
