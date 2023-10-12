class KnownError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class SomeTasksFailedError(KnownError):
    def __init__(self):
        super().__init__("Some tasks have failed.")


class CountsMismatchError(KnownError):
    def __init__(self, message: str):
        super().__init__(message)


class UsageError(KnownError):
    def __init__(self, message: str):
        super().__init__(message)
