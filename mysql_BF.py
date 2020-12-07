import pymysql
import threading
import random
import string
import time
from queue import Queue
from dbutils.pooled_db import PooledDB

class ThreadInsert(object):
    "多线程并发MySQL插入数据"

    def __init__(self):
        self.pool = self.mysql_connection()
        self.data = self.getData()
        self.mysql_delete()
        self.task()

    def mysql_connection(self):
        pool = PooledDB(
            pymysql,
            host='127.0.0.1',
            user='root',
            port=3306,
            passwd='000606',
            db='short_video_platform',
            use_unicode=True)
        return pool

    def getData(self):
        begin_time = time.process_time()
        data = []
        for i in range(1000000):
            # a=''.join(str(random.choice(range(10))) for _ in range(8))
            a = ''.join(random.sample(string.ascii_letters, 4))
            b = random.choice(['F', 'M'])
            c = random.randint(10, 50)
            d = random.randint(0, 1000)
            e = random.randint(0, 1000)
            f = random.randint(0, 100)
            g = random.randint(0, 300)
            tup = (a, b, c, d, e, f, g)
            data.append(tup)

        n = 1000  # 按每n行数据为最小单位拆分成嵌套列表
        result = [data[i:i + n] for i in range(0, len(data), n)]
        end_time = time.process_time()
        print("共获取{}组数据,每组{}个元素.==>> 耗时:{}'s".format(len(result), n, round(end_time - begin_time, 4)))
        return result

    def mysql_delete(self):
        begin_time = time.process_time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "SET FOREIGN_KEY_CHECKS=0"
        cur.execute(sql)
        sql = "TRUNCATE TABLE users"
        cur.execute(sql)
        con.commit()
        cur.close()  # 关闭游标
        con.close()  # 关闭mysql连接
        end_time = time.process_time()
        print("清空原数据.==>> 耗时:{}'s".format(round(end_time - begin_time, 3)))

    def mysql_select(self):
        begin_time = time.process_time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "SELECT * FROM users WHERE 100< FANS < 200"
        try:
            cur.execute(sql)
            end_time = time.process_time()
            file = open("selectTime.txt", "a")
            # file.write("查询一组数据.==>> 耗时:{}'s\n".format(round(end_time-begin_time, 3)))
            file.write("{}\n".format(round(end_time - begin_time, 4)))
            print("查询成功")
        except Exception as e:
            con.rollback()
            print("SQL查询有误，原因:", e)
        finally:
            cur.close()
            con.close()
            file.close()

    def mysql_insert(self, *args):
        begin_time = time.process_time()
        con = self.pool.connection()
        cur = con.cursor()
        sql = "INSERT INTO users(NAME, SEX, AGE, FANS, FOLLOWS, PUBLISHED, FAVORITES) " \
              "VALUES(%s, %s, %s, %s, %s, %s, %s)"
        try:
            cur.executemany(sql, *args)
            con.commit()
            print("SQL插入成功")
            end_time = time.process_time()
            file = open("insertTime.txt", "a")
            # file.write("插入一组数据.==>> 耗时:{}'s\n".format(round(end_time-begin_time, 3)))
            file.write("{}\n".format(round(end_time - begin_time, 4)))
        except Exception as e:
            con.rollback()  # 事务回滚
            print('SQL插入有误,原因:', e)
        finally:
            con.close()
            cur.close()
            file.close()

    def task(self):
        q = Queue(maxsize=20)  # 设定最大队列数和线程数
        begin_time = time.process_time()
        while self.data:
            content = self.data.pop()
            t1 = threading.Thread(target=self.mysql_insert, args=(content,))
            t2 = threading.Thread(target=self.mysql_select)
            q.put(t1)
            q.put(t2)
            if (q.full() == True) or (len(self.data)) == 0:
                threads = []
                while q.empty() == False:
                    t1 = q.get()
                    t2 = q.get()
                    threads.append(t1)
                    threads.append(t2)
                    t1.start()
                    t2.start()
                for t1 in threads:
                    t1.join()
                for t2 in threads:
                    t2.join()
        end_time = time.process_time()
        print("数据插入和查询完成.==>> 耗时:{}'s".format(round(end_time - begin_time, 4)))


if __name__ == '__main__':
    ThreadInsert()

