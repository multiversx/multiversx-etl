import logging
from typing import Any, Dict, Optional, Protocol

import google.cloud.logging

LOGGER_NAME = "multiversx-etl"


class SupportsToPlainDictionary(Protocol):
    def to_plain_dictionary(self) -> Dict[str, Any]:
        ...


class CloudLogger:
    def __init__(self, project_id: str, worker_id: str):
        self.logging_client = google.cloud.logging.Client(project=project_id)
        self.worker_id = worker_id

    def log_info(self, message: str, data: Optional[SupportsToPlainDictionary] = None):
        logging.info(message)

        logger = self.logging_client.logger(LOGGER_NAME)  # type: ignore
        logger.log_struct({  # type: ignore
            "worker": self.worker_id,
            "message": message,
            "data": data.to_plain_dictionary() if data else {}
        })

    def log_error(self, message: str, data: Optional[SupportsToPlainDictionary] = None):
        logging.error(message)

        logger = self.logging_client.logger(LOGGER_NAME)  # type: ignore
        logger.log_struct({  # type: ignore
            "worker": self.worker_id,
            "message": message,
            "data": data.to_plain_dictionary() if data else {}
        }, severity="ERROR")
