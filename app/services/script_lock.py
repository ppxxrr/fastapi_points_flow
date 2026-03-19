from __future__ import annotations

import os
from contextlib import AbstractContextManager
from pathlib import Path


class FileScriptLock(AbstractContextManager["FileScriptLock"]):
    def __init__(self, lock_path: str | Path):
        self.lock_path = Path(lock_path)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = None

    def acquire(self) -> bool:
        self._handle = self.lock_path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._handle.seek(0)
            self._handle.truncate()
            self._handle.write(str(os.getpid()).encode("ascii"))
            self._handle.flush()
            return True
        except OSError:
            self.release()
            return False

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            self._handle.close()
            self._handle = None

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

