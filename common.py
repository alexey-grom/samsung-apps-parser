# coding: utf

from logging import getLogger
from time import sleep

from requests import get
from grab import Grab
from grab.spider import Spider
from pycountry import countries

import config


logger = getLogger(__file__)


def get_proxy_list(country_code=None, count=100):
    u"""Получение проксилиста"""
    response = get(config.PROXIES_API_URL % {
        'country_code': country_code,
        'count': count,
    })
    proxies = response.json()
    proxies = [
        dict(zip(['proxy', 'proxy_type'],
                 [proxy['address'], proxy['type']]))
        for proxy in proxies
    ]
    return proxies


def patch_doc():
    u"""Патчинг интерфейса для включения структурного парсинга"""

    from grab.tools.structured import TreeInterface, x
    from grab.ext import doc

    def __structure(self, *args, **kwargs):
        return TreeInterface(self.grab.tree).structured_xpath(*args, **kwargs)

    doc.DocInterface.structure = __structure


patch_doc()


class ParserWithProxy(Spider):
    u"""Базовый класс парсера для работы с прокси"""

    USE_PROXY = True

    def __init__(self, country_code, *args, **kwargs):
        super(ParserWithProxy, self).__init__(*args, **kwargs)

        self.country = countries.get(alpha2=country_code)
        self.proxies = []
        self.used_proxies = set()

        self.grab = None
        self.grab_use_count = None

        self.reinit_grab()

        self.setup_queue(getattr(config, 'QUEUE_BACKEND', 'memory'))
        if getattr(config, 'CACHE_ENABLED', False):
            self.setup_cache('mongo', getattr(config, 'CACHE_DATABASE', 'cache'))

    def check_grab(self, grab):
        return True

    def reinit_grab(self):
        if not self.grab:
            self.grab = Grab()

        self.grab_use_count = 0

        while True:
            self.grab.clear_cookies()
            self.grab.setup(**self.get_next_proxy())
            if self.check_grab(self.grab):
                break
            logger.info(u'Плохая прокси. Смена...')

    def get_grab(self):
        self.grab_use_count += 1

        if self.grab_use_count > config.PROXY_USE_LIMIT:
            self.reinit_grab()

        return self.grab.clone()

    def get_next_proxy(self):
        u"""Получение следующей неиспользованной прокси"""

        if not self.USE_PROXY:
            return {}

        while not self.proxies:
            # получение проксей и фильтрация неспользованных
            self.proxies = get_proxy_list(self.country.alpha2, 100)
            self.proxies = filter(
                lambda proxy: tuple(proxy.values()) not in self.used_proxies,
                self.proxies
            )
            if not self.proxies:
                logger.info(u'Кончились прокси, ожидание новых')
                sleep(10)
            else:
                break
        # возврат первой прокси
        proxy = self.proxies[0]
        self.used_proxies.add(tuple(proxy.values()))
        del self.proxies[0]
        return proxy
