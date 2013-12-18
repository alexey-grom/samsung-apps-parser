#!/usr/bin/env python
# coding: utf

from sys import exit
from argparse import ArgumentParser
from re import match, search
from logging import getLogger, basicConfig, DEBUG, INFO
from json import dumps
from time import sleep
from pprint import pprint
from urlparse import parse_qs, urlparse
from re import sub

from requests import get
from grab import Grab
from grab.error import GrabTimeoutError, GrabAuthError, GrabMisuseError, GrabConnectionError, GrabNetworkError
from grab.spider import Spider, Task
from grab.tools.structured import TreeInterface, x
from grab.ext import doc
from pycountry import countries

import config


#
# vibor strani
# http://apps.samsung.com/mars/main/getCountry.as
#
# smena strani
# http://apps.samsung.com/mars/common/selectCountry.as?countryCode=PHL&rmbCnry=Y&countrySetOption=


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
    # pprint(proxies)
    return proxies


class Parser(Spider):
    PER_PAGE = 20
    USE_PROXY = True

    def __init__(self, seller_id, country_code, *args, **kwargs):
        self.seller_id = seller_id
        self.country_code = country_code
        self.proxies = []
        self.used_proxies = set()
        self.apps = {}
        self.country = countries.get(alpha2=country_code)
        self.grab = Grab()
        self.reinit_grab()
        super(Parser, self).__init__(*args, **kwargs)

    def get_app_id_by_url(self, url):
        query = parse_qs(urlparse(url).query, keep_blank_values=True)
        return query.get('productId', [''])[0]

    def get_next_proxy(self):
        while not self.proxies:
            self.proxies = get_proxy_list(self.country_code, 100)
            self.proxies = filter(
                lambda proxy: tuple(proxy.values()) not in self.used_proxies,
                self.proxies
            )
            if not self.proxies:
                logger.info(u'Кончились прокси, ожидание новых')
                sleep(10)
            else:
                break
        proxy = self.proxies[0]
        self.used_proxies.add(tuple(proxy.values()))
        del self.proxies[0]
        return proxy

    def reinit_grab(self):
        self.grab_used_count = 1
        while True:
            logger.info(u'Proxy change...')
            self.grab.clear_cookies()
            self.grab.setup(**self.get_next_proxy())
            try:
                self.grab.go('http://apps.samsung.com/mars/common/selectCountry.as?countryCode=%s&rmbCnry=Y&countrySetOption=' % (
                    self.country.alpha3
                ))
                if self.grab.response.code == 200:
                    break
            except (GrabTimeoutError, GrabAuthError, GrabMisuseError, GrabConnectionError, GrabNetworkError):
                pass
            logger.info(u'Bad proxy, try next...')

    def get_grab(self):
        if self.grab_used_count < config.PROXY_USE_LIMIT:
            self.grab_used_count += 1
            return self.grab
        self.reinit_grab()
        return self.grab.clone()

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

        grab = self.get_grab()
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
        grab = self.get_grab()
        grab.setup(url=app_url)
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
        return app

    def store_apps_data(self, apps):
        for app in apps:
            productId = self.get_app_id_by_url(app['url'])
            # pprint(productId)
            self.apps[productId] = app

    def task_seller_list(self, grab, task):
        # print grab.response.body

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

    def task_app_detail(self, grab, task):
        productId = self.get_app_id_by_url(grab.response.url)

        rows = grab.xpath_list('//*[@class="detail-spec"]//*[@class="spec-wrap"]//dl//dd')
        rows = map(
            lambda node: sub(r'\s+', ' ', node.text_content().strip()),
            rows
        )
        rows = rows[1:5] + rows[7:10]
        values = dict(zip(
            ['category', 'type', 'first registration date',
             'version (update date)', 'age', 'required OS',
             'size', 'supported languages', ],
            rows
        ))
        # pprint(values)
        self.apps[productId].update(values)

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
                        args.country_code)
        parser.run()
    except KeyboardInterrupt:
        logger.error(u' Прервано')

    exit()
