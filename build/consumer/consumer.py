from concurrent.futures._base import as_completed, wait
from concurrent.futures.thread import ThreadPoolExecutor
from clickhouse_driver import Client
from threading import Thread
import redis
import json
import time
import requests
import datetime
import firebase_admin
from firebase_admin import messaging
from firebase_admin import credentials
from ClickHousePool import ClickHousePool
import logging
import Config
import pytz
import uuid
import Util
from PipeMessage import *


# redis 组初始化
# should provide unique group name
def redis_init_group(redis_stream_name, redis_group_name):
    if rd.exists(redis_stream_name):
        logging.debug(rd.xinfo_groups(redis_stream_name))
        if redis_group_name in [row["name"] for row in rd.xinfo_groups(redis_stream_name)]:
            logging.error(redis_stream_name + "group exist. exiting.")
            exit(-1)
        # stream exist, may have race condition
        rd.xgroup_create(redis_stream_name, redis_group_name,
                         id='0-0', mkstream=False)
    else:
        rd.xgroup_create(redis_stream_name, redis_group_name,
                         id='0-0', mkstream=True)


# 发送企业微信消息
def send_wxwork_handler(params, send_data, d=0):
    """
        Handle wxwork message.
        ``params``(dict): for API request.
        ``send_data``: construct message.
        ``d``(dict): original message.
    """
    req = requests.post(url="https://qyapi.weixin.qq.com/cgi-bin/message/send",
                        params=params, json=send_data.to_api_data(), timeout=(5, 5))
    resp = req.json()
    logging.debug("微信send接口返回："+str(resp))
    if resp["errcode"] == 0:
        logging.info("发送成功，耗时："+str(time.time() -
                     float(send_data.get_data()["time"])))
        return True
    else:
        logging.info("发送失败")
        return False


# 发送telegram消息
def send_telegram_handler(send_data):
    req = requests.post(url="https://api.telegram.org/bot{}/sendMessage".format(
        send_data.get_config()['token']), json=send_data.to_api_data(), timeout=(5, 5))
    resp = req.json()
    logging.debug("telegram接口返回："+str(resp))
    if resp["ok"]:
        logging.info("发送成功，耗时："+str(time.time() -
                     float(send_data.get_data()["time"])))
        return True
    else:
        logging.info("发送失败")
        return False


# 发送FCM消息
def send_fcm_handler(send_data):
    try:
        response = messaging.send(send_data.to_api_data())
        logging.info('Successfully sent fcm message:', response)
        return True
    except:
        logging.info("发送失败")
        return False


# 发送Bark消息
def send_bark_handler(send_data):
    req = requests.get(url="https://api.day.app/{}/{}?url={}".format(send_data.get_config()
                       ['token'], send_data.to_api_data()['title'], send_data.get_msg_url("encode")), timeout=(5, 5))
    resp = req.json()
    logging.debug("bark接口返回："+str(resp))
    if resp["code"] == 200:
        logging.info("发送成功，耗时："+str(time.time() -
                     float(send_data.get_data()["time"])))
        return True
    else:
        logging.info("发送失败")
        return False


# 发送Server酱消息
def send_server_chan_handler(send_data):
    req = requests.post(url="https://sctapi.ftqq.com/{}.send".format(
        send_data.get_config()['token']), data=send_data.to_api_data(), timeout=(5, 5))
    resp = req.json()
    logging.debug("Server酱接口返回："+str(resp))
    if resp["code"] == 200:
        logging.info("发送成功，耗时："+str(time.time() -
                     float(send_data.get_data()["time"])))
        return True
    else:
        logging.info("发送失败")
        return False


# 异步插入数据库
def send_insert_handler(message):
    logging.debug("插入数据库"+str(message))
    Thread(target=rd.xadd, args=(Config.REDIS_INSERT_STREAM, message)).start()


# 转json
def to_json_dict(data):
    if type(data) == dict:
        return data
    elif type(data) == bytes:
        return json.loads(str(data, "utf-8"))
    elif type(data) == str:
        return json.loads(data)


# 处理、分发消息
def data_row_handler(row):
    ck = pool.get_connection()
    d = to_json_dict(row)
    result = ck.execute(
        "SELECT config FROM pipe.pipe_config where uuid = %(uuid)s and name = %(name)s",
        {"uuid": d['uuid'], "name": d["name"]}
    )
    if result:
        # 准备好要保存的情况
        logging.debug("收到消息："+str(d))

        config = json.loads(result[0][0])

        # 分发出错标志位
        is_dispatch = False

        if config['type'] == 'wxwork':
            # 处理access_token，可能需要加上secret才行，唯一化
            logging.info("进入wxwork分支")
            res = ck.execute(
                "SELECT access_token FROM pipe.pipe_wxwork where corpid = %(corpid)s and agent_id = %(agentid)s",
                {"corpid": config["corpid"], "agentid": config["agentid"]}
            )

            if res:
                access_token = res[0][0]
            else:
                req = requests.get(url="https://qyapi.weixin.qq.com/cgi-bin/gettoken", params={
                                   "corpid": config["corpid"], "corpsecret": config["corpsecret"]}, timeout=(5, 5))
                resp = req.json()
                logging.debug("微信access_token接口返回："+str(resp))
                if resp["errcode"] == 0:
                    access_token = resp["access_token"]
                    ck.execute("INSERT INTO pipe.pipe_wxwork (corpid, agent_id, access_token) VALUES", ((
                        config["corpid"], config["agentid"], resp["access_token"]),))
                else:
                    logging.error("access_token获取失败")
                    return

            # 发送消息
            send_data = Message_wxwork(config, d)
            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            is_dispatch = send_wxwork_handler(
                {"access_token": access_token}, send_data)

        elif config['type'] == 'telegram':
            # 直接发送，无需获取access_token
            # 发送消息
            logging.info("进入telegram分支")
            send_data = Message_telegram(config, d)

            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            is_dispatch = send_telegram_handler(send_data)

        elif config['type'] == 'fcm':
            # See documentation on defining a message payload.
            logging.info("进入telegram分支")
            send_data = Message_fcm(config, d)

            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            is_dispatch = send_fcm_handler(send_data)

        elif config['type'] == 'mqtt':
            # TODO
            pass

        elif config['type'] == 'apn':
            # TODO
            pass

        elif config['type'] == 'bark':
            logging.info("进入bark分支")
            send_data = Message_bark(config, d)
            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            is_dispatch = send_bark_handler(send_data)

        elif config['type'] == 'server_chan':
            logging.info("进入server_chan分支")
            send_data = Message_serverchan(config, d)
            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            is_dispatch = send_server_chan_handler(send_data)

        elif config['type'] == 'mail':
            # TODO
            pass

        if is_dispatch:
            ck.execute(
                "ALTER TABLE pipe.pipe_config UPDATE error_times = 0 WHERE uuid = %(uuid)s and name = %(name)s",
                {"uuid": d['uuid'], "name": d["name"]}
            )
        else:
            ck.execute(
                "ALTER TABLE pipe.pipe_config UPDATE error_times = error_times + 1 WHERE uuid = %(uuid)s and name = %(name)s",
                {"uuid": d['uuid'], "name": d["name"]}
            )

    pool.release_connection(ck)


# 处理插入数据
def data_insert_handler(insert_msg):
    client = pool.get_connection()
    client.execute(
        "INSERT INTO pipe.pipe_message (uuid, message_id, title, message) VALUES", insert_msg)
    pool.release_connection(client)


# 主处理消费线程
def redis_handler_stream():
    with ThreadPoolExecutor(50) as executor:
        consumer_name = uuid.uuid4().hex
        try:
            while True:
                t1 = time.time()

                data = rd.xreadgroup(
                    Config.REDIS_GROUP_NAME,
                    consumer_name,
                    {Config.REDIS_MSG_STREAM: ">", Config.REDIS_INSERT_STREAM: ">"},
                    count=50,
                    block=0)

                logging.info(
                    (rd.xpending(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME)))
                logging.info(
                    (rd.xpending(Config.REDIS_INSERT_STREAM, Config.REDIS_GROUP_NAME)))
                for stream in data:
                    # print(stream)
                    if stream[0] == Config.REDIS_MSG_STREAM:
                        for msg_id, msg in stream[1]:
                            executor.submit(data_row_handler, msg)
                            # data_row_handler(msg)
                            # 处理xack无法进行消除的bug
                            rd.xack(Config.REDIS_MSG_STREAM,
                                    Config.REDIS_GROUP_NAME, msg_id)
                            rd.xdel(Config.REDIS_MSG_STREAM, msg_id)
                    elif stream[0] == Config.REDIS_INSERT_STREAM:
                        insert_msg = []
                        for msg_id, msg in stream[1]:
                            logging.debug("收到插入数据库"+str(msg))
                            try:
                                d = to_json_dict(msg)
                                insert_msg.append(
                                    [d['uuid'], d['message_id'], d['title'], d['message']])
                            except Exception as e:
                                logging.error(e)
                                pass
                            rd.xack(Config.REDIS_INSERT_STREAM,
                                    Config.REDIS_GROUP_NAME, msg_id)
                            rd.xdel(Config.REDIS_INSERT_STREAM, msg_id)

                        # 线程池插入数据库
                        if len(insert_msg) != 0:
                            executor.submit(data_insert_handler, insert_msg)
        except Exception as e:
            logging.error(e)

        # logging.info("time: %s, handle data: %s" % (time.time()-t1, 100))


# 打印debug信息
def debug_info():
    while True:
        time.sleep(10)
        logging.debug(rd.xinfo_groups(Config.REDIS_INSERT_STREAM))
        logging.debug(rd.xinfo_groups(Config.REDIS_MSG_STREAM))


logging.basicConfig(
    level=logging.DEBUG,
    # format="%(asctime)s [*] %(processName)s  %(threadName)s  %(message)s"
    format="%(asctime)s %(message)s"
)

logging.info("Consumer started.")

try:
    cred = credentials.Certificate("fcm_token.json")
    default_app = firebase_admin.initialize_app(cred)
except Exception as e:
    logging.error(e)

# 初始化redis
redis_pool = redis.ConnectionPool(
    host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)
rd = redis.Redis(connection_pool=redis_pool)


# 初始化流
redis_init_group(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME)
redis_init_group(Config.REDIS_INSERT_STREAM, Config.REDIS_GROUP_NAME)

pool = ClickHousePool()


# 启动消费线程
thread = []

for i in range(Config.CONSUMER_THREAD):
    t = Thread(target=redis_handler_stream)
    t.start()
    thread.append(t)


# t = Thread(target=debug_info)
# t.start()
# thread.append(t)

for i in thread:
    i.join()
