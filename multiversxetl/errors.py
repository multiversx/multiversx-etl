class TransientError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class UsageError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
