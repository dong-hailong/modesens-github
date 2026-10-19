# -*- coding: utf-8 -*-
import io
import json
import logging
import os
import threading

log = logging.getLogger(__name__)

__all__ = [
    'DUMP_PATH_ENV',
    'DEFAULT_DUMP_PATH',
    'dump_path',
    'dump_key',
    'read_items',
    'dump_item',
    'close_dumps',
    'ItemDump',
    'ItemDumpMixin',
]

DUMP_PATH_ENV = 'DATAFEED_ITEM_DUMP_PATH'
DEFAULT_DUMP_PATH = '/tmp/datafeed_items/items.jsonl'


def dump_path():
    """The dump file: env override, else the fixed /tmp/datafeed_items/items.jsonl."""
    return os.environ.get(DUMP_PATH_ENV) or DEFAULT_DUMP_PATH


def dump_key(item):
    """Identity of an item for dedup: ``(merchant, sku)``, or None if unusable.

    Keyed on the pair, not on ``sku`` alone: merchants share one file, and
    skus are only unique within a merchant.
    """
    try:
        sku = item.get('sku')
    except Exception:
        return None
    if not sku:
        return None
    try:
        merchant = item.get('merchant')
    except Exception:
        merchant = None
    return (str(merchant or ''), str(sku))


def read_items(path=None, merchant=None):
    """Yield the dicts in the dump file, skipping lines that will not parse.

    Defaults to :func:`dump_path`.  Pass ``merchant`` to keep only that
    merchant's items -- filtering on the ``merchant`` field inside each line,
    since all merchants share the file.  A missing file yields nothing, and a
    corrupt line is warned about and skipped, so a truncated tail from a
    killed crawl is not fatal.
    """
    path = path or dump_path()
    if not os.path.exists(path):
        return
    with io.open(path, 'r', encoding='utf-8') as handle:
        for lineno, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError as exc:
                log.warning('item dump %s line %d unreadable: %s', path, lineno, exc)
                continue
            if not isinstance(obj, dict):
                continue
            if merchant is not None and obj.get('merchant') != merchant:
                continue
            yield obj


class ItemDump(object):
    """Append-only, deduped-on-(merchant, sku) writer for the shared dump file."""

    def __init__(self, merchant='', path=None):
        self.merchant = merchant
        self.path = path or dump_path()
        self.written = 0
        self.skipped = 0          # no sku, or already present in the file
        self._seen = None         # set((merchant, sku)), read back on first add
        self._fh = None
        self._lock = threading.Lock()

    def _load(self):
        """Read back what is already dumped, so a restart does not duplicate."""
        seen = set()
        for item in read_items(path=self.path):
            key = dump_key(item)
            if key:
                seen.add(key)
        self._seen = seen
        log.debug('item dump %s: resumed with %d item(s) on disk',
                  self.path, len(seen))

    def _open(self):
        """Open the file for appending, creating the directory if needed."""
        directory = os.path.dirname(self.path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)
        self._fh = io.open(self.path, 'a', encoding='utf-8')

    def add(self, item):
        """Append ``item`` unless its (merchant, sku) is already on disk.

        Never raises: a failed dump must not cost the crawl.  Returns True
        when a line was written, False when it was skipped (no sku, an
        earlier sighting of the same pair, or an I/O failure).
        """
        try:
            key = dump_key(item)
            if key is None:
                self.skipped += 1
                return False
            with self._lock:
                if self._seen is None:
                    self._load()
                if key in self._seen:
                    self.skipped += 1
                    return False
                if self._fh is None:
                    self._open()
                line = json.dumps(self._as_dict(item), ensure_ascii=False, default=str)
                self._fh.write(line + '\n')
                self._fh.flush()          # a killed crawl must not lose the tail
                self._seen.add(key)
            self.written += 1
            return True
        except Exception as exc:
            log.warning('item dump failed (%s): %s', self.path, exc)
            return False

    def close(self):
        handle, self._fh = self._fh, None
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
        if self.written or self.skipped:
            log.info('item dump %s: %d written, %d skipped',
                     self.path, self.written, self.skipped)

    @staticmethod
    def _as_dict(item):
        """A scrapy Item is a dict already; stay tolerant of other mappings."""
        try:
            return dict(item)
        except Exception:
            return dict((key, item[key]) for key in item.keys())


# one writer per path, so every spider in the process shares the dedup set
_dumps = {}


def dump_item(item, path=None):
    """Module-level :meth:`ItemDump.add`, sharing one writer per file."""
    merchant = ''
    try:
        merchant = item.get('merchant') or ''
    except Exception:
        pass
    target = path or dump_path()
    writer = _dumps.get(target)
    if writer is None:
        writer = _dumps[target] = ItemDump(merchant, path=target)
    return writer.add(item)


def close_dumps():
    """Close every writer opened by :func:`dump_item`, logging its tally."""
    for writer in list(_dumps.values()):
        writer.close()
    _dumps.clear()


class ItemDumpMixin(object):
    """Drop-in spider mixin exposing ``self.dump_item(item)``.

    Put it first in the bases so its ``closed`` wins and can still chain::

        class CjSftpSpider(ItemDumpMixin, BaseSpider):
            ...

    Appends the parsed item to the shared dump file, one line per
    (merchant, sku), and never raises: a failed dump must not cost the crawl.
    """

    def dump_item(self, item):
        """True when a line was appended, False when it was skipped."""
        try:
            return dump_item(item)
        except Exception as exc:
            try:
                self.logger.warning('item dump crashed (%s): %s',
                                    item.get('merchant'), exc)
            except Exception:
                pass
            return False

    def closed(self, reason):
        close_dumps()
        parent_closed = getattr(super(ItemDumpMixin, self), 'closed', None)
        if parent_closed is not None:
            parent_closed(reason)
