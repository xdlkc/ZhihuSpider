import sys

from zhihu_spider.enums import TopicEnum

sys.path.append("..")
import logging
import redis
from mysql.connector import pooling
import queue
from zhihu_spider.db_config import MYSQL_CONFIG, REDIS_CONFIG


class MysqlManager(object):
    def __init__(self):
        self.mcp = pooling.MySQLConnectionPool(**MYSQL_CONFIG)

    def execute_dml(self, sql_str, *args):
        cnx = None
        try:
            cnx = self.mcp.get_connection()
            cursor = cnx.cursor()
            cursor.execute(sql_str, args)
            cursor.close()
            cnx.commit()
        except Exception as e:
            logging.log(logging.ERROR, e)
            raise e
        finally:
            if cnx:
                cnx.close()

    def execute_query(self, sql_str, *args):
        cnx = None
        try:
            cnx = self.mcp.get_connection()
            cursor = cnx.cursor()
            cursor.execute(sql_str, args)
            result_set = cursor.fetchall()
            cursor.close()
            return result_set
        except Exception as e:
            logging.log(logging.ERROR, "args:{},err:{}".format(args, e))
        finally:
            if cnx:
                cnx.close()

    def test(self):
        conn = self.mcp.get_connection()
        cur = conn.cursor()
        sql = "select count(*) from movie"
        r = cur.execute(sql)
        r = cur.fetchall()
        print(r)
        cur.close()
        conn.close()


class RedisManager(object):
    def __init__(self):
        self.rdp = redis.ConnectionPool(**REDIS_CONFIG)
        self.rdc = redis.StrictRedis(connection_pool=self.rdp)


def create_topic_tree(root_id):
    db = MysqlManager()
    rdc = RedisManager().rdc
    # 后进先出队列实现阶梯结构
    topic_queue = queue.LifoQueue()
    query_sql = 'select id,name from topic where topic_id=%s'
    root_topic = db.execute_query(query_sql, root_id)[0]
    topic_queue.put({'id': root_topic[0], 'name': root_topic[1], 'level': 0})
    parent_set = set()
    with open('{}.txt'.format(root_topic[1]), 'w', encoding='utf-8') as f:
        while topic_queue.qsize() > 0:
            # 获取父话题
            parent_topic = topic_queue.get()
            print(parent_topic)
            topic_id = parent_topic['id']
            if topic_id in parent_set:
                continue
            f.write(" " * (parent_topic['level'] * 2))
            f.write("{}\n".format(parent_topic['name']))
            # 查找子话题id
            relate_redis_str = "{}_{}".format(TopicEnum.over_children_topic_id_set.value, topic_id)
            children_set = rdc.smembers(relate_redis_str)
            for children in children_set:
                # 查找子话题详情
                sql = 'select id,name from topic where id=%s'
                result = db.execute_query(sql, children)
                if len(result) > 0:
                    for r in result:
                        # 将子话题放入队列
                        topic_queue.put({'id': r[0], 'name': r[1], 'level': parent_topic['level'] + 1})
            parent_set.add(topic_id)


if __name__ == '__main__':
    create_topic_tree(19550517)
