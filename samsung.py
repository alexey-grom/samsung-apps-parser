#!/usr/bin/env python
# coding: utf

from sys import exit
from argparse import ArgumentParser
from re import match, search
from logging import getLogger, basicConfig, DEBUG, INFO
from json import dumps
# from pprint import pprint
from urlparse import parse_qs, urlparse
from re import sub

# from grab import Grab
from grab.error import GrabTimeoutError, GrabAuthError, GrabMisuseError, GrabConnectionError, GrabNetworkError
from grab.spider import Task
from grab.tools.structured import x

from common import ParserWithProxy
# import config


u"""
Открытие формы выбора страны
http://apps.samsung.com/mars/main/getCountry.as
Смена страны
http://apps.samsung.com/mars/common/selectCountry.as?countryCode=PHL&rmbCnry=Y&countrySetOption=
"""


logger = getLogger('info')


class Parser(ParserWithProxy):
    PER_PAGE = 20
    USE_PROXY = True

    def __init__(self, seller_id, *args, **kwargs):
        super(Parser, self).__init__(*args, **kwargs)

        self.seller_id = seller_id
        self.apps = {}
        # self.grab = Grab()
        # self.reinit_grab()

        self.add_task(self.make_apps_task())

    def get_app_id_by_url(self, url):
        u"""Извлечение ID приложения из его детального URL"""
        query = parse_qs(urlparse(url).query, keep_blank_values=True)
        return query.get('productId', [''])[0]

    def check_grab(self, grab):
        try:
            grab.go('http://apps.samsung.com/mars/common/selectCountry.as?countryCode=%s&rmbCnry=Y&countrySetOption=' % (
                self.country.alpha3
            ))
            if grab.response.code == 200:
                return True
        except (GrabTimeoutError, GrabAuthError, GrabMisuseError, GrabConnectionError, GrabNetworkError):
            pass
        return False

    def make_apps_task(self, page=0):
        u"""Создание задачи пагинации"""
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
        u"""Создание задачи детальной приложения"""
        grab = self.get_grab()
        grab.setup(url=app_url)
        self.add_task(Task(
            'app_detail',
            grab=grab
        ))

    @staticmethod
    def app_data_prepare(app):
        u"""Обработка данных о приложении"""
        if app.cost is None:
            app.cost = 'free'
        if match(r'\d+.\d+/5', app.rating):
            app.rating = float(app.rating.split('/')[0])
        else:
            app.rating = 0.0
        app.market_id = search(r'productId=(\d+)(&|$)', app.url).group(1)
        return app

    def store_apps_data(self, apps):
        u"""Сохранение набора приложений"""
        for app in apps:
            productId = self.get_app_id_by_url(app['url'])
            # pprint(productId)
            self.apps[productId] = app

    def task_seller_list(self, grab, task):
        u"""Страница пагинации продавца"""

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
        u"""Детальная страница продавца"""

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
        self.apps[productId].update(values)

    def shutdown(self):
        u"""Событие завершения работы"""
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

    parser = Parser(args.seller_id,
                    args.country_code)
    try:
        parser.run()
    except KeyboardInterrupt:
        logger.error(u' Прервано')

    print parser.render_stats()

    exit()
