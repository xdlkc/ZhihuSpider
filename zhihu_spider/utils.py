import logging
import redis
from mysql.connector import pooling

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
