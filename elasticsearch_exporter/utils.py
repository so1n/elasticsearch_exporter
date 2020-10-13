import functools
import logging
import time
import signal
import sys
import os
from enum import Enum

from typing import Callable, Tuple


def shutdown(shutdown_signals: Tuple[Enum, ...] = (signal.SIGINT, signal.SIGTERM)):

    def sig_handler(signum: int, _):
        logging.info(f'Received signal:{signal.Signals(signum).name}')
        sys.exit()

    def decorator(func: Callable) -> Callable:

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            old_handlers: dict = {}
            for sig in shutdown_signals:
                old_handlers[sig] = signal.signal(sig, sig_handler)

            try:
                _func: Callable = func(*args, **kwargs)
                while True:
                    try:
                        time.sleep(5)
                    except KeyboardInterrupt:
                        sig_name = '/'.join(
                            [sig.name for sig in shutdown_signals]
                        )
                        logging.warning(
                            f"You can use kill -{sig_name} {os.getpid()}"
                        )
                    except Exception:
                        return _func
            finally:
                for sig, old_handler in old_handlers.items():
                    signal.signal(sig, old_handler)
        return wrapper
    return decorator
