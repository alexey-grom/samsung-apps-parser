#!/usr/bin/env python
# coding: utf

from sys import exit
from argparse import ArgumentParser
from re import match, search
from logging import getLogger, basicConfig, DEBUG, INFO
from json import dumps

from grab import Grab
from grab.spider import Spider, Task
from grab.tools.structured import TreeInterface, x
from grab.ext import doc


def __structure(self, *args, **kwargs):
    return TreeInterface(self.grab.tree).structured_xpath(*args, **kwargs)
doc.DocInterface.structure = __structure


logger = getLogger('info')


class Parser(Spider):
    PER_PAGE = 20

    def __init__(self, seller_id, *args, **kwargs):
        self.seller_id = seller_id
        self.apps = []
        super(Parser, self).__init__(*args, **kwargs)

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

        if len(apps) == self.PER_PAGE and task.page < 10:
            yield self.make_apps_task(task.page + 1)

    def shutdown(self):
        result = dumps(
            self.apps,
            indent=2,
            ensure_ascii=False
        )
        print result.encode('utf-8')
        logger.info(u'Найдено %d приложений' % len(self.apps))


if __name__ == '__main__':
    basicConfig(level=INFO)

    argument_parser = ArgumentParser()
    argument_parser.add_argument('seller_id')
    args = argument_parser.parse_args()

    #seller_id = 'arsdalt7cj'
    #seller_id = 'w7ouf3k0em'

    try:
        parser = Parser(args.seller_id)
        parser.run()
    except KeyboardInterrupt:
        logger.error(u' Прервано')

    exit()
