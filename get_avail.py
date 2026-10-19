#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""遍历读取 /tmp 下爬虫 dump 出来的 item，打印出来 —— 只读，不连数据库。

用法::

    python scripts/py/get_avail.py                          # 默认目录下全部文件
    python scripts/py/get_avail.py "D'aniello Boutique"     # 只看某个商家
    python scripts/py/get_avail.py --dir /tmp/datafeed_items --limit 20
    python scripts/py/get_avail.py --fields merchant,sku,url,listprice
    python scripts/py/get_avail.py --full                   # 每条打完整 JSON
    python scripts/py/get_avail.py --json                   # 每行一条原始 JSONL

dump 由爬虫侧产生：``storefeed/datafeed/spiders/item_dump.py`` 把解析好的 item
追加到 ``<dir>/<merchant>.jsonl``（一行一个 item，按 ``sku`` 去重）。目录默认
``/tmp/datafeed_items``，可用 ``DATAFEED_ITEM_DUMP_DIR`` 或 ``--dir`` 覆盖。

本脚本只**读取和打印**：不连数据库、不写任何文件。默认每个 item 打一行
(``merchant | sku | name | url``)，``--fields`` 换字段，``--full`` / ``--json``
打完整内容。

注意：文件名（安全化后的商家名）只是标签，**权威商家名在每行 item 的
``merchant`` 字段**；不传 merchant 时会把目录下所有 ``*.jsonl`` 都读一遍。
"""

import json
import os
import sys
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from utils.ladystyle import *
from product.models import *
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from storefeed.datafeed.spiders.item_dump import dump_path, read_items

DEFAULT_FIELDS = ('merchant', 'sku', 'name', 'url')
USEFUL_FIELDS = ('name', 'url', 'designer', 'saleprice', 'listprice', 'sku', 'merchant', 'crawler_name')
PATH = dump_path()


def parse_args(args):
    """Parse argv: merchant names, ``--dir``, ``--limit``, ``--fields``, ``--full``, ``--json``."""
    merchants = []
    directory = None
    limit = 0                     # 0 = no limit
    fields = DEFAULT_FIELDS
    mode = 'brief'                # brief | full | json
    rest = list(args or [])
    at = 0
    while at < len(rest):
        token = rest[at]
        if token in ('--dir', '-d') and at + 1 < len(rest):
            directory = rest[at + 1]
            at += 2
            continue
        if token in ('--limit', '-n') and at + 1 < len(rest):
            try:
                limit = max(0, int(rest[at + 1]))
            except ValueError:
                pass
            at += 2
            continue
        if token == '--fields' and at + 1 < len(rest):
            fields = tuple(f.strip() for f in rest[at + 1].split(',') if f.strip())
            at += 2
            continue
        if token in ('--full', '-f'):
            mode = 'full'
            at += 1
            continue
        if token == '--json':
            mode = 'json'
            at += 1
            continue
        merchants.append(token)
        at += 1
    return merchants, directory, limit, fields, mode


def dump_files(directory, merchants):
    """The dump files to read: the named merchants', or every file in the dir."""
    if merchants:
        return [dump_path(merchant, directory=directory) for merchant in merchants]
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return []
    return [os.path.join(directory, name) for name in names if name.endswith('.jsonl')]


def _flat(value):
    """One printable line for any value: lists joined, whitespace collapsed."""
    if value is None:
        return ''
    if isinstance(value, (list, tuple, set)):
        text = ', '.join(str(v) for v in value)
    elif isinstance(value, dict):
        text = json.dumps(value, ensure_ascii=False, default=str)
    else:
        text = str(value)
    return ' '.join(text.split())


def print_item(item, index, fields=DEFAULT_FIELDS, mode='brief'):
    """Print one item: a brief line, an indented full dump, or a raw JSONL line."""
    if mode == 'json':
        print(json.dumps(item, ensure_ascii=False, default=str))
        return
    if mode == 'full':
        print('[%d]' % index)
        print(json.dumps(item, ensure_ascii=False, default=str, indent=2, sort_keys=True))
        return
    print('%4d  %s' % (index, ' | '.join(_flat(item.get(field)) for field in fields)))

    




def get_avail(args):
    """command：遍历读取 ``<dir>`` 下的 item 并打印，只读。

    usage: get_avail.py [merchant ...] [--dir DIR] [--limit N] [--fields a,b,c] [--full] [--json]

    不传 merchant 就遍历目录下所有 ``*.jsonl``；传了就用 ``dump_path()`` 定位，
    找不到的打 ``[miss]``。每条 item 默认打成一行 ``merchant | sku | name | url``，
    ``--fields`` 换字段，``--full`` 打完整 JSON，``--json`` 打原始 JSONL。
    末尾给出总条数与读到的文件数。
    """
    merchants, directory, limit, fields, mode = parse_args(args)
    directory = directory or os.path.dirname(PATH)
    paths = [PATH]
    if not paths:
        print('no dump file under %s' % directory)
        return 0

    total = 0
    shown = 0
    read_files = 0
    for path in paths:
        if not os.path.exists(path):
            print('[miss] %s' % path)
            continue
        items = list(read_items(path=path))
        if not items:
            print('[skip] %s: empty' % path)
            continue
        print('[read] %s: %d items' % (path, len(items)))
        read_files += 1
        total += len(items)
        for index, item in enumerate(items, 1):
            if '7354246037548' in item['sku']:
                continue
            try:
                avail = Availability.objects.get(merchant_id=item['merchant'], code=item['sku'])
                fix_medias(avail, item['images'])
                print (item['images'])
                print (avail.id, avail.code, '==>', item['sku'])
            except Exception as e:
                print (e)
            if limit and index > limit:
                print('       ... %d more not shown (--limit %d)' % (len(items) - limit, limit))
                break
            # print_item(item, index, fields=fields, mode=mode)
            shown += 1

    print('-' * 78)
    print('total: %d items from %d file(s), %d shown' % (total, read_files, shown))
    return 0


def fix_medias(avail, images):
    medias = Media.objects.filter(availability=avail)
    # print (medias, images)

    if avail.cover != images[0]:
        print ('###### {}: [{} ===> {}]'.format(avail.id, avail.cover, images[0]))
        avail.change_cover(images[0])
        if avail.product and avail.product.availability == avail and not avail.product.saved_to_s3:
            avail.product.change_cover(images[0], force=True, save=True)

        for media in medias:
            if media.link in images:
                images.remove(media.link)
            else:
                media.delete()

        ins_medias = []
        for image in images:
            if 'http' not in image:
                continue
            media = Media(
                availability = avail,
                link = image,
                typ = 'p'
            )
            ins_medias.append(media)
        Media.objects.bulk_create(ins_medias)
        print ('###### image fixed ######', avail.id)



def main(args=None):
    return get_avail(sys.argv[1:] if args is None else args)


if __name__ == '__main__':
    sys.exit(main())
