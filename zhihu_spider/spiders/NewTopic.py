# -*- coding: utf-8 -*-
import scrapy
import json
from zhihu_spider.items import TopicItem
import re


class NewtopicSpider(scrapy.Spider):
    name = 'new_topic'
    allowed_domains = ['www.zhihu.com']
    start_urls = ['https://www.zhihu.com/api/v3/topics/19580349/children?limit=10&offset=0']
    url_model = 'https://www.zhihu.com/api/v3/topics/{}/children?limit=10&offset={}'

    def parse(self, response):
        response_json = json.loads(response.body)
        is_end = response_json['paging']['is_end']
        if is_end is True:
            return
        if 'data' not in response_json:
            return
        data_json = response_json['data']
        meta = response.meta
        if 'parent_id' in meta:
            parent_id = meta['parent_id']
        else:
            parent_id = re.findall('https://www.zhihu.com/api/v3/topics/(\\d+)/children', response.url)[0]
        if 'offset' in meta:
            offset = meta['offset'] + 10
        else:
            offset = int(re.findall('limit=10&offset=(\\d+)', response.url)[0]) + 10
        for data in data_json:
            item = TopicItem(data)
            topic_id = item['id']
            item['parent_id'] = parent_id
            if 'category' not in item:
                item['category'] = ''
            if 'type' not in item:
                item['type'] = ''
            yield item
            yield scrapy.Request(url=self.url_model.format(str(topic_id), str(0)),
                                 meta={'offset': 0, 'parent_id': topic_id},
                                 callback=self.parse)
        yield scrapy.Request(url=self.url_model.format(parent_id, offset),
                             meta={'offset': offset, 'parent_id': parent_id},
                             callback=self.parse)
