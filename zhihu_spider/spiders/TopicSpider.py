# -*- coding: utf-8 -*-
import scrapy
import json
import pymongo
import re
import logging


class TopicSpider(scrapy.Spider):
    name = 'TopicSpider'
    allowed_domains = []
    start_urls = []

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = pymongo.MongoClient()
        self.topic_id_str = 'topic_id'
        self.page_no_str = 'page_no'
        self.db = self.client["zhihu"]
        self.url = "https://www.zhihu.com/api/v4/topics/{0}/feeds/essence?include=data%5B%3F(" \
                   "target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(" \
                   "target.type%3Danswer)%5D.target.content%2Crelationship.is_authorized%2Cis_author%2Cvoting" \
                   "%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F(target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(" \
                   "target.type%3Danswer)%5D.target.is_normal%2Ccomment_count%2Cvoteup_count%2Ccontent" \
                   "%2Crelevant_info%2Cexcerpt.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(" \
                   "target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(" \
                   "target.type%3Darticle)%5D.target.content%2Cvoteup_count%2Ccomment_count%2Cvoting%2Cauthor.badge" \
                   "%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(" \
                   "target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(" \
                   "target.type%3Dpeople)%5D.target.answer_count%2Carticles_count%2Cgender%2Cfollower_count" \
                   "%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(" \
                   "target.type%3Danswer)%5D.target.annotation_detail%2Ccontent%2Crelationship.is_authorized" \
                   "%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F(" \
                   "target.type%3Danswer)%5D.target.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(" \
                   "target.type%3Darticle)%5D.target.annotation_detail%2Ccontent%2Cauthor.badge%5B%3F(" \
                   "type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(" \
                   "target.type%3Dquestion)%5D.target.annotation_detail%2Ccomment_count&limit=10&offset={1}"
        # 话题列表
        topic_ids = []
        # 已经爬取的话题id
        over_topics = [d[self.topic_id_str]
                       for d in self.db.over_topics.find()]
        count = 0
        for topic in self.db.spider_topic.find():
            spider_id = topic[self.topic_id_str]
            if spider_id not in over_topics:
                topic_ids.append(int(spider_id))
                count += 1
                if count == 10:
                    break
        logging.log(msg="***********{}".format(topic_ids), level=logging.INFO)
        self.start_page = {}
        for topic_id in topic_ids:
            last_page = self.db.last_page.find_one(
                {self.topic_id_str: topic_id})
            if last_page:
                self.start_page[topic_id] = last_page['page_no']
            else:
                self.start_page[topic_id] = 1
            url = self.url.format(topic_id, self.start_page[topic_id])
            self.start_urls.append(url)

    def parse(self, response):

        if self.topic_id_str in response.meta:
            topic_id = response.meta[self.topic_id_str]
        else:
            topic_id = int(re.findall(
                r"https://www.zhihu.com/api/v4/topics/(.+?)/feeds/essence", response.url)[0])

        if self.page_no_str in response.meta:
            page_no = response.meta[self.page_no_str]
        else:
            page_no = self.start_page[topic_id]

        next_url = self.url.format(topic_id, page_no + 1)
        meta = response.meta
        meta[self.page_no_str] = page_no + 1

        # 查看目前爬取的最后一页，并与当前页比较后更新
        last_page = self.db.last_page.find_one({self.topic_id_str: topic_id})
        if last_page:
            if self.page_no_str in last_page:
                if page_no > int(last_page[self.page_no_str]):
                    self.db.last_page.update(
                        last_page, {'$set': {self.topic_id_str: topic_id, self.page_no_str: page_no}})
        else:
            self.db.last_page.insert(
                {self.topic_id_str: topic_id, self.page_no_str: page_no})

        # 查看是否已经爬取过当前页的数据
        is_saved = self.db.saved_topics.find(
            {self.topic_id_str: topic_id, self.page_no_str: page_no}).count()
        if is_saved:
            logging.log(msg="topic_id:{},page:{} exists".format(topic_id, page_no), level=logging.INFO)
            yield scrapy.Request(url=next_url, meta=meta, callback=self.parse)
        else:
            js = json.loads(response.body)
            items = js['data']
            # 重组返回的数据，将重要的字段，如id，url,content等提取出来
            for item in items:
                # 利用字典的移除操作
                target = item.pop('target')
                item['answer_id'] = target.pop('id')
                is_answered = self.db.saved_answers.find(
                    {self.topic_id_str: topic_id, 'answer_id': item['answer_id']}).count()
                if is_answered:
                    items.remove(item)
                    continue
                else:
                    self.db.saved_answers.insert(
                        {self.topic_id_str: topic_id, 'answer_id': item['answer_id']})
                item['answer_url'] = target.pop('url')
                # 有的类别如专栏，不存在问题这项
                if 'answers' in item['answer_url']:
                    item['question'] = target.pop('question')
                item['content'] = target.pop('content')
                if 'excerpt' in target:
                    item['excerpt'] = target.pop('excerpt')
                item['target'] = target
                item[self.topic_id_str] = topic_id

            # 当前页是否是最后一页
            is_end = js["paging"]["is_end"]
            if is_end:
                self.db.over_topics.insert({self.topic_id_str: topic_id})
                return

            if len(items) > 0:
                self.db.answers.insert_many(items)
                self.db.saved_topics.insert(
                    {self.topic_id_str: topic_id, self.page_no_str: page_no})
            logging.log(msg="topic_id:{} ,page:{} over".format(topic_id, page_no), level=logging.INFO)
            yield scrapy.Request(url=next_url, meta=meta, callback=self.parse)


if __name__ == '__main__':
    client = pymongo.MongoClient()
    db = client["zhihu"]