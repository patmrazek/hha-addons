"""Logování s maskováním tajemství (access code, tokeny)."""
import logging
import sys


class RedactFilter(logging.Filter):
    """Nahradí známé tajné řetězce ve zprávách i argumentech za ***."""

    def __init__(self):
        super().__init__()
        self.secrets: set[str] = set()

    def add(self, *values):
        for v in values:
            if v and isinstance(v, str) and len(v) >= 4:
                self.secrets.add(v)

    def _clean(self, text: str) -> str:
        for s in self.secrets:
            text = text.replace(s, "***")
        return text

    def filter(self, record: logging.LogRecord) -> bool:
        if self.secrets:
            record.msg = self._clean(str(record.msg))
            if record.args:
                record.args = tuple(self._clean(a) if isinstance(a, str) else a for a in record.args) \
                    if isinstance(record.args, tuple) else record.args
        return True


REDACT = RedactFilter()


def setup(level: str = "info") -> None:
    root = logging.getLogger()
    root.handlers.clear()
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"))
    h.addFilter(REDACT)
    root.addHandler(h)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("paho").setLevel(logging.WARNING)
