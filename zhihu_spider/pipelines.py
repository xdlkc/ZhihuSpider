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
        rdc = self.redis_manager.rdc
        children_id = item['id']
        parent_id = item['parent_id']

        children_db_id = rdc.hget(TopicEnum.topic_id_to_db_id_hash.value, children_id)

        parent_db_id = rdc.hget(TopicEnum.topic_id_to_db_id_hash.value, parent_id)
        query_id_sql = 'select id from topic where topic_id = %s;'
        if parent_db_id is None:
            id_set = self.db_manager.execute_query(query_id_sql, parent_id)
            if len(id_set) <= 0:
                logging.error("parent topic {} not existed...".format(parent_id))
                raise Exception
            parent_db_id = id_set[0][0]
            rdc.hset(TopicEnum.topic_id_to_db_id_hash.value, parent_id, parent_db_id)

        if children_db_id is None:
            id_set = self.db_manager.execute_query(query_id_sql, children_id)
            if len(id_set) <= 0:
                logging.info("insert new topic {}...".format(children_id))
                ins_sql = "insert into topic (topic_id, name, url, excerpt, introduction, avatar_url, type, category,is_black,is_vote) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                self.db_manager.execute_dml(ins_sql, item['id'], item['name'], item['url'], item['excerpt'],
                                            item['introduction'], item['avatar_url'], item['type'], item['category'],
                                            item['is_black'], item['is_vote'])
                id_set = self.db_manager.execute_query(query_id_sql, children_id)
                if len(id_set) <= 0:
                    logging.error("children topic {} insert failed...".format(children_id))
                    raise Exception
            children_db_id = id_set[0][0]
            rdc.hset(TopicEnum.topic_id_to_db_id_hash.value, children_id, children_db_id)

        relate_redis_str = "{}_{}".format(TopicEnum.over_children_topic_id_set.value, parent_db_id)

        if rdc.sismember(relate_redis_str, children_db_id):
            logging.info("topic {} and children topic {} already crawled...".format(parent_id, children_id))
            return
        insert_relate_topic = 'insert into topic_related (topic_id, children_id) values (%s,%s);'
        self.db_manager.execute_dml(insert_relate_topic, parent_db_id, children_db_id)
        rdc.sadd(relate_redis_str, children_db_id)
