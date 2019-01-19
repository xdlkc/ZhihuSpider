# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
from zhihu_spider.items import TopicItem
from zhihu_spider.utils import MysqlManager, RedisManager
from zhihu_spider.enums import TopicEnum


class ZhihuSpiderPipeline(object):
    def __init__(self):
        self.db_manager = MysqlManager()
        self.redis_manager = RedisManager()

    def process_item(self, item, spider):
        print("process item:{}".format(item))
        try:
            if isinstance(item, TopicItem):
                self.process_topic_item(item)
        except Exception as e:
            msg = "crawl err,item:{}, e:{}".format(item, e)
            print(msg)
            logging.error(msg)
            # 抓取出现异常，发送停止信号
            spider.crawler.engine.close_spider(spider, 'process_movie_item err:{}'.format(e))

    def process_topic_item(self, item):
        id = item['id']
        if self.redis_manager.rdc.sismember(TopicEnum.over_topic_id_set.value, id):
            logging.info("topic {} already crawled...".format(id))
            return
        ins_sql = "insert into topic (topic_id, name, url, excerpt, introduction, avatar_url, type, category, parent_id,is_black,is_vote) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        self.db_manager.execute_dml(ins_sql, item['id'], item['name'], item['url'], item['excerpt'],
                                    item['introduction'], item['avatar_url'], item['type'], item['category'],
                                    item['parent_id'], item['is_black'], item['is_vote'])
        self.redis_manager.rdc.sadd(TopicEnum.over_topic_id_set.value, id)
