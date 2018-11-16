# -*- coding: utf-8 -*-
import scrapy
import pymongo
import re
import json


class TopicidSpider(scrapy.Spider):
    name = 'TopicIdSpider'
    allowed_domains = []
    start_urls = ['https://www.zhihu.com/topics#投资']
    index_url = 'https://www.zhihu.com/topics'

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
                                               'params': '{"topic_id":' + topic_id + ',"offset":0,"hash_id":"82b70f2bdf9765304cd843f94f233a74"}'},
                                     callback=self.parse_topic)

    def parse_topic(self, response):
        data = json.loads(response.body)['msg']
        items = []
        for topic in data:
            topic_id = re.findall('/topic/(\d+)', topic)[0]
            c = self.db.spider_topic.find({'topic_id': topic_id}).count()
            if c != 0:
                continue
            topic_name = re.findall('<strong>(.*)</strong>', topic)[0]
            items.append({'topic_id': topic_id, 'topic_name': topic_name})
        if len(items) != 0:
            self.db.spider_topic.insert_many(items)
