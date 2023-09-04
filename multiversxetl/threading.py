import logging
import threading
import time
from typing import Callable, List, Protocol

from multiversxetl.errors import TransientError


class ICounter(Protocol):
    def increment(self) -> int:
        ...


def do_until(
        callable: Callable[[], bool],
        external_event_is_work_enough_for_now: threading.Event,
        external_or_internal_event_has_error_happened: threading.Event,
        calls_counter: ICounter,
        sleep_between_calls: int,
        num_threads: int,
        thread_start_delay: int = 1,
        thread_name_prefix: str = "thread") -> bool:
    """
    Execute a callable until an external event is set or an error happens.
    :param callable: The payload function to execute.
    :param external_event_is_work_enough_for_now: An external exit (break) event; external caller decides when "work is enough".
    :param external_or_internal_event_has_error_happened: An external or internal exit (break) event; external caller or internal threads decide when an "error happened".
    :param calls_counter: Gets incremented after each call.
    :param sleep_between_calls: Time to sleep between calls of "callable".
    :param num_threads: Number of threads to use.
    :param thread_start_delay: Time to sleep between starting threads.
    :param thread_name_prefix: Prefix for thread names.
    :return: True if "do_until" exits due to "work is enough for now" or because of an non-transient error (more work might be necessary in the future). False if "do_until" exits due to "callable" returning False (actually no more work to do).
    """

    threads: List[threading.Thread] = []

    for i in range(num_threads):
        # Start threads with a small delay between them.
        time.sleep(thread_start_delay)

        thread = threading.Thread(
            name=f"{thread_name_prefix}-{i}",
            target=_do_until_in_thread,
            args=[
                callable,
                external_event_is_work_enough_for_now,
                external_or_internal_event_has_error_happened,
                sleep_between_calls,
                calls_counter
            ]
        )

        thread.start()
        threads.append(thread)

        if external_or_internal_event_has_error_happened.is_set():
            break

    for thread in threads:
        if thread.is_alive():
            thread.join()

    # True if more work might be necessary in the future.
    return external_event_is_work_enough_for_now.is_set() or external_or_internal_event_has_error_happened.is_set()


def _do_until_in_thread(
        callable: Callable[[], bool],
        external_event_is_work_enough: threading.Event,
        external_or_internal_event_has_error_happened: threading.Event,
        calls_counter: ICounter,
        sleep_between_calls: int):
    while not external_event_is_work_enough.is_set() and not external_or_internal_event_has_error_happened.is_set():
        try:
            still_work_to_do = callable()
            calls_counter.increment()

            if not still_work_to_do:
                # No more work to do.
                break

            time.sleep(sleep_between_calls)
        except TransientError as error:
            logging.info(f"Transient error, will try again later: {error}")
        except:
            logging.exception("Unexpected error, thread will stop.")
            external_or_internal_event_has_error_happened.set()
            raise
