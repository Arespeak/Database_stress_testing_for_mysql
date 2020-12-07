import pymysql
import time
import random
import string
connect = pymysql.connect(
    host='127.0.0.1',
    user='root',
    port=3306,
    passwd='000606',
    db='short_video_platform',
    use_unicode=True
)
def SelectTime():
    begintime=time.time()
    cur=connect.cursor()
    sql="SELECT ID,NAME FROM users " \
        "WHERE FANS<200"
    try:
        cur.execute(sql)
        print("查询结果为：{}".format(cur.fetchall()))
    except:
        connect.rollback()
        print("查询失败")
    finally:
        connect.close()
        cur.close()
    endtime=time.time()
    file=open("One_Seltime.txt","w")
    file.write("查询时间为：{}".format(endtime-begintime))
    file.close()


def InsertTime():

    cur = connect.cursor()
    sql = "SELECT NAME FROM users"
    cur.execute(sql)
    name = cur.fetchall() #获取users表中NAME
    name_data = []
    for mid in name:
        name_data.append(mid[0])
    sj = random.choice(name_data)
    v_author = sj
    v_intro = ''.join(random.sample(string.ascii_letters + ' ', 10))
    print("要插入的数据如下：")
    print("作者：{}, 简介：{}".format(v_author, v_intro))
    # idClient,Name,Age,Sex,Balance=input().split(' ')
    begintime = time.time()
    sql = "INSERT INTO videos(AUTHOR,INTRO) VALUES (%s,%s)"
    args=(v_author, v_intro)
    try:
        cur.execute(sql,args)
        connect.commit()
        print("插入成功")
    except:
        connect.rollback()
        print("插入失败")
    finally:
        connect.close()
        cur.close()
    endtime = time.time()
    file = open("One_IneTime.txt", "w")
    file.write("插入时间为：{}".format(endtime - begintime))
    file.close()

if __name__=="__main__":
    # InsertTime()
    SelectTime()