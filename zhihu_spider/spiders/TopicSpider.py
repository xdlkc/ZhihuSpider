# -*- coding: utf-8 -*-
import scrapy
import pymongo
import re
import json
from zhihu_spider.items import TopicItem


class TopicidSpider(scrapy.Spider):
    name = 'topic_id'
    allowed_domains = []
    start_urls = ['https://www.zhihu.com/topics']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = pymongo.MongoClient()
        self.db = self.client["zhihu"]
        self.topic_list_url = 'https://www.zhihu.com/node/TopicsPlazzaListV2'

    def parse(self, response):
        topics = response.xpath('//ul')
        topic_ids = topics.xpath('./li[@class="zm-topic-cat-item"]/@data-id').extract()
        for topic_id in topic_ids:
            yield scrapy.FormRequest(url=self.topic_list_url,
                                     formdata={'method': 'next',
                                               'params': '{"topic_id":' + topic_id
                                                         + ',"offset":0,"hash_id":"82b70f2bdf9765304cd843f94f233a74"}'},
                                     meta={'offset': 0, 'parent_id': topic_id},
                                     callback=self.parse_topic)

    def parse_topic(self, response):
        data_json = json.loads(response.body)
        if 'msg' not in data_json or len(data_json['msg']) <= 1:
            return
        meta = response.meta
        data = data_json['msg']
        items = []
        parent_id = meta['parent_id']
        for topic in data:
            item = TopicItem()
            topic_id = re.findall('/topic/(\d+)', topic)[0]
            c = self.db.spider_topic.find({'topic_id': topic_id}).count()
            if c != 0:
                continue
            topic_name = re.findall('<strong>(.*)</strong>', topic)[0]
            items.append({'topic_id': topic_id, 'topic_name': topic_name, 'parent_topic_id': 99})
            item['topic_id'] = topic_id
            item['topic_name'] = topic_name
            item['parent_id'] = parent_id
            yield item
            topic_next = '{"topic_id":' + topic_id + ',"offset":0,"hash_id":"82b70f2bdf9765304cd843f94f233a74"}'
            yield scrapy.FormRequest(url=self.topic_list_url,
                                     formdata={'method': 'next', 'params': topic_next},
                                     meta={'offset': 0, 'parent_id': topic_id},
                                     callback=self.parse_topic)
        if len(items) != 0:
            self.db.spider_topic.insert_many(items)
        offset = int(meta['offset']) + 40
        s = '{"topic_id":' + parent_id + ',"offset":' + str(offset) + ',"hash_id":"82b70f2bdf9765304cd843f94f233a74"}'
        yield scrapy.FormRequest(url=self.topic_list_url,
                                 formdata={'method': 'next', 'params': s},
                                 meta={'offset': offset, 'parent_id': parent_id},
                                 callback=self.parse_topic)
