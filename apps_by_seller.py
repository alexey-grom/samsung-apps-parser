#!/usr/bin/env python
# coding: utf

from sys import exit
from argparse import ArgumentParser
from re import match, search
from logging import getLogger, basicConfig, DEBUG, INFO
from json import dumps
from time import sleep
from pprint import pprint

from requests import get
from grab import Grab
# from grab.base import default_config
from grab.spider import Spider, Task
from grab.tools.structured import TreeInterface, x
from grab.ext import doc

import config


def __structure(self, *args, **kwargs):
    return TreeInterface(self.grab.tree).structured_xpath(*args, **kwargs)
doc.DocInterface.structure = __structure


logger = getLogger('info')


def get_proxy_list(country_code=None, count=100):
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


class Parser(Spider):
    PER_PAGE = 20

    def __init__(self, seller_id, country_code, *args, **kwargs):
        self.seller_id = seller_id
        self.country_code = country_code
        self.proxies_use_count = {}
        self.proxies = []
        self.apps = []
        super(Parser, self).__init__(*args, **kwargs)

    def get_next_proxy(self):
        proxy_address = None

        while True:
            while not self.proxies:
                self.proxies = get_proxy_list(self.country_code, 100)
                self.proxies = filter(
                    lambda proxy: proxy['proxy'] not in self.proxies_use_count,
                    self.proxies
                )
                if not self.proxies:
                    logger.info(u'Кончились прокси, ожидание новых')
                    sleep(10)
                else:
                    break

            proxy_address = self.proxies[0]['proxy']

            if proxy_address not in self.proxies_use_count:
                self.proxies_use_count[proxy_address] = 0

            if self.proxies_use_count[proxy_address] >= config.PROXY_USE_LIMIT:
                del self.proxies[0]
            else:
                break

        self.proxies_use_count[proxy_address] += 1
        return self.proxies[0]

    def task_generator(self):
        logger.info(u'Поиск приложений...')
        yield self.make_apps_task()

    def make_apps_task(self, page=0):
        post = {
            'sellerId': self.seller_id,
            'categoryID': '',
        }
        if page:
            post.update({
                'cmd': 'last',
                'pageNo': page + 1,
                'perPage': self.PER_PAGE,
                'lastIndex': page * self.PER_PAGE,
            })
        grab = Grab()
        grab.setup(**self.get_next_proxy())
        grab.setup(
            url='http://apps.samsung.com/mars/topApps/getSellerList.as',
            method='POST',
            post=post
        )
        return Task(
            name='seller_list',
            grab=grab,
            page=page
        )

    def make_app_task(self, app_url):
        grab = Grab()
        grab.setup(
            url=app_url,
            **self.get_next_proxy()
        )
        self.add_task(Task(
            'app_detail',
            grab=grab
        ))

    @staticmethod
    def app_data_prepare(app):
        if app.cost is None:
            app.cost = 'free'
        if match(r'\d+.\d+/5', app.rating):
            app.rating = float(app.rating.split('/')[0])
        else:
            app.rating = 0.0
        app.market_id = search(r'productId=(\d+)(&|$)', app.url).group(1)
        #app.title = unicode(app.title)
        #app.cost = unicode(app.cost)
        return app

    def store_apps_data(self, apps):
        self.apps.extend(apps)

    def task_seller_list(self, grab, task):
        apps = grab.doc.structure(
            '//*[@class="apps-thumb-list-p"]',
            x(
                './/*[@class="apps-con"]',
                cost='./em/text()',
                rating='.//*[starts-with(@class, "star")]/text()'
            ),
            url='.//a/@href',
            title='.//*[@class="apps-title"]/strong/text()',
            image='.//*[@class="apps-img-size03"]/img/@src',
        )
        apps = map(self.app_data_prepare, apps)
        self.store_apps_data(apps)

        for app in apps:
            self.make_app_task(app['url'])

        if len(apps) == self.PER_PAGE:
            yield self.make_apps_task(task.page + 1)

    def task_seller_list_fallback(self, task):
        new_task = task.clone()
        proxy = self.get_next_proxy()
        pprint(proxy)
        new_task.grab_config.update(proxy)
        self.add_task(new_task)

    def task_app_detail(self, grab, task):
        print [grab, task]

    def task_app_detail_fallback(self, task):
        new_task = task.clone()
        proxy = self.get_next_proxy()
        new_task.grab_config.update(proxy)
        self.add_task(new_task)

    def shutdown(self):
        result = dumps(
            self.apps,
            indent=2,
            ensure_ascii=False
        )
        print result.encode('utf-8')
        logger.info(u'Найдено %d приложений' % len(self.apps))


if __name__ == '__main__':
    basicConfig(level=DEBUG)

    argument_parser = ArgumentParser()
    argument_parser.add_argument('seller_id')
    argument_parser.add_argument('country_code')
    args = argument_parser.parse_args()

    #seller_id = 'arsdalt7cj'
    #seller_id = 'w7ouf3k0em'

    try:
        parser = Parser(args.seller_id,
                        args.country_code,
                        network_try_limit=1,
                        task_try_limit=1)
        parser.run()
    except KeyboardInterrupt:
        logger.error(u' Прервано')

    exit()
