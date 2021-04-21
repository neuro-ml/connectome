from pathlib import Path
from typing import Callable, Any, ContextManager, Sequence


class RemoteLocation(ContextManager):
    def fetch(self, keys: Sequence[str], store: Callable[[str, Path], Any]) -> Sequence[str]:
        raise NotImplementedError

    def download(self, key: str, file: Path):
        raise NotImplementedError
