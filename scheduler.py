#!/usr/bin/env python
# coding: utf

from sys import exit

from gevent.monkey_patch import patch_all; patch_all()
from gevent.pool import Pool
from gevent import spawn, join_all


# количество одновременно обрабатываемых профилей в каждом маркете
PROFILES_CHECK_POOL_SIZE = 10

MARKETS = {
    'google': {
        'parser': './google.py',
        'profiles': [],
        'countries': [],

    },
    'samsung': {
        'parser': './samsung.py',
        'profiles': [],
        'countries': [],
    },
}


def get_market_settings(market_name):
    settings = MARKETS.get(market_name)
    return settings


def market_loader(market_name, parser, profiles, countries, **kwargs):
    pool = Pool(size=PROFILES_CHECK_POOL_SIZE)
    pool.spawn()
    pool.join()


if __name__ == '__main__':
    jobs = [
        spawn(market_loader, market_name, **get_market_settings(market_name))
        for market_name in MARKETS.keys()
    ]
    join_all(jobs)
    exit()
