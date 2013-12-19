#!/usr/bin/env python
# coding: utf

from sys import exit, argv
from argparse import ArgumentParser
from logging import getLogger, basicConfig, DEBUG, INFO
from pprint import pprint
from urllib import urlencode
from json import dumps

from grab import Grab
from grab.spider import Spider, Task
from grab.tools.structured import TreeInterface, x
from grab.ext import doc
from grab.error import GrabTimeoutError, GrabAuthError, GrabMisuseError, GrabConnectionError, GrabNetworkError

from common import ParserWithProxy


class Parser(ParserWithProxy):
    PAGE_SIZE = 24
    USE_PROXY = True

    def __init__(self, developer_name, *args, **kwargs):
        super(Parser, self).__init__(*args, **kwargs)

        self.developer_name = developer_name
        self.apps = []

        self.add_task(self.get_next_page_task())

    def check_grab(self, grab):
        try:
            grab.go('https://play.google.com/')
            if grab.response.code == 200 and grab.response.url == 'https://play.google.com/store':
                return True
        except (GrabTimeoutError, GrabAuthError, GrabMisuseError, GrabConnectionError, GrabNetworkError):
            pass
        return False

    def get_paging_grab(self):
        return self.get_grab()

    def get_detail_grab(self):
        return self.get_grab()

    def get_next_page_task(self, page=0):
        grab = self.get_paging_grab()
        url = 'https://play.google.com/store/apps/developer?' + urlencode({
            'id': self.developer_name,
        })
        data = {
            'start': page * self.PAGE_SIZE,
            'num': self.PAGE_SIZE,
            'numChildren': 0,
            'ipf': 1,
            'xhr': 1,
        }
        # pprint(data)
        grab.setup(
            url=url,
            post=data,
            referer=url,
        )
        return Task(
            'developer',
            grab=grab,
            page=page
        )

    def get_detail_page_task(self, app_url):
        grab = self.get_detail_grab()
        grab.setup(url=app_url)
        return Task(
            'detail',
            grab=grab
        )

    def task_developer(self, grab, task):
        grab.tree.make_links_absolute(grab.response.url)

        apps = grab.xpath_list('//*[@class="card-list"]//*[@class="details"]/a[@class="card-click-target"]/@href')

        if len(apps) >= self.PAGE_SIZE:
            yield self.get_next_page_task(task.page + 1)

        for app_url in apps:
            yield self.get_detail_page_task(app_url)

    def task_detail(self, grab, task):
        remove_space = lambda text: text.replace(u'\xa0', '')
        get_text = lambda node: node.text_content().strip()

        def get_number(text):
            try:
                return int(remove_space(text))
            except ValueError:
                pass
            return text

        # get_number = lambda text: int(remove_space(text))

        # def remove_space(text):
        #     try:
        #         return text.replace(u'\xa0', '')
        #     except:
        #         pass
        #     return text

        # def split_range(text):
        #     text = remove_space(text)
        #     for splitter in [u'–', u'-']:
        #         if splitter in text:
        #             parts = text.split(u'–')
        #         break
        #     try:
        #         parts = map(int, parts)
        #     except ValueError:
        #         pass
        #     return parts

        data = grab.doc.structure(
            '//body',
            #info=
            x(
                './/*[@class="details-info"]',
                image='.//*[@class="cover-image"]/@src',
                name=('.//*[@class="document-title"]', get_text),
            ),
            rating=x(
                './/*[@class="rating-box"]',
                score='.//*[@class="score"]/text()',
                reviews_num=('.//*[@class="reviews-num"]/text()', get_number),
                grades=x(
                    './/*[@class="rating-histogram"]//*[starts-with(@class, "rating-bar-container ")]',
                    grade=('.//*[@class="bar-label"]', get_text),
                    count=('.//*[@class="bar-number"]/text()', get_number)
                )
            ),
            detail=x(
                './/*[@class="details-section-contents"]//*[@class="content"]',
                type='./@itemprop',
                value='./text()',
            )
        )

        #
        data = data[0]
        # data['info'] = data['info'][0]
        data['rating'] = data['rating'][0]

        # преобразование детальной информации в словарь и разбиение диапазона загрузок
        detail = data['detail']
        detail = {
            item['type']: item['value']
            for item in detail
        }
        # detail['numDownloads'] = split_range(detail['numDownloads']) # разделители и форматирование чисел
                                                                       # разное в разных странах
        data['detail'] = detail

        #
        self.apps.append(data)

    def shutdown(self):
        print dumps(self.apps,
                    indent=2,
                    ensure_ascii=False)


if __name__ == '__main__':
    basicConfig(level=DEBUG)

    argument_parser = ArgumentParser()
    argument_parser.add_argument('country_code')
    argument_parser.add_argument('developer_name',
                                 nargs='+')
    args = argument_parser.parse_args()

    try:
        parser = Parser(u' '.join(args.developer_name),
                        args.country_code)
        parser.run()
    except KeyboardInterrupt:
        logger.error(u' Прервано')

    exit()
