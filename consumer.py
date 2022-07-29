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
redis_pool = redis.ConnectionPool(host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)
rd = redis.Redis(connection_pool=redis_pool)

try:
    rd.xgroup_destroy(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME)
except: pass
try:
    rd.xgroup_destroy(Config.REDIS_INSERT_STREAM, Config.REDIS_GROUP_NAME)
except: pass
rd.xgroup_create(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME, id='0-0', mkstream=True)
rd.xgroup_create(Config.REDIS_INSERT_STREAM, Config.REDIS_GROUP_NAME, id='0-0', mkstream=True)
pool = ClickHousePool()


# 发送企业微信消息
def send_wxwork_handler(params, send_data, d=0):
    """
        Handle wxwork message.
        ``params``(dict): for API request.
        ``send_data``: construct message.
        ``d``(dict): original message.
    """
    req = requests.post(url="https://qyapi.weixin.qq.com/cgi-bin/message/send",params=params,json=send_data.to_api_data(),timeout=(5, 5))
    resp = req.json()
    logging.debug("微信send接口返回："+str(resp))
    if resp["errcode"] == 0:
        logging.info("发送成功，耗时："+str(time.time()-float(send_data.get_data()["time"])))
    else:
        logging.info("发送失败")


# 发送telegram消息
def send_telegram_handler(send_data):
    req = requests.post(url="https://api.telegram.org/bot{}/sendMessage".format(send_data.get_config()['token']),json=send_data.to_api_data(),timeout=(5, 5))
    resp = req.json()
    logging.debug("telegram接口返回："+str(resp))
    if resp["ok"]:
        logging.info("发送成功，耗时："+str(time.time()-float(send_data.get_data()["time"])))
    else:
        logging.info("发送失败")


# 发送FCM消息
def send_fcm_handler(send_data):
    response = messaging.send(send_data.to_api_data())
    logging.info('Successfully sent fcm message:', response)


# 发送Bark消息
def send_bark_handler(send_data):
    req = requests.get(url="https://api.day.app/{}/{}?url={}".format(send_data.get_config()['token'], send_data.to_api_data()['title'], send_data.get_msg_url("encode")),timeout=(5, 5))
    resp = req.json()
    logging.debug("bark接口返回："+str(resp))
    if resp["code"] == 200:
        logging.info("发送成功，耗时："+str(time.time()-float(send_data.get_data()["time"])))
    else:
        logging.info("发送失败")

# 异步插入数据库
def send_insert_handler(message):
    logging.debug("插入数据库"+str(message))
    Thread(target=rd.xadd, args=(Config.REDIS_INSERT_STREAM, message)).start()


def to_json_dict(data):
    if type(data) == dict:
        return data
    elif type(data) == bytes:
        return json.loads(str(data ,"utf-8"))
    elif type(data) == str:
        return json.loads(data)


def data_row_handler(row):
    ck = pool.get_connection()
    d = to_json_dict(row)
    result = ck.execute("SELECT config FROM pipe.pipe_config where uuid = '{}' and name = '{}'".format(d['uuid'], d["name"]))
    if result:
        # 准备好要保存的情况
        logging.debug("收到消息："+str(d))

        config = json.loads(result[0][0])
        if config['type'] == 'wxwork':
            # 处理access_token，可能需要加上secret才行，唯一化
            logging.info("进入wxwork分支")
            res = ck.execute("SELECT access_token FROM pipe.pipe_wxwork where corpid = '{}' and agent_id = {}".format(config["corpid"], config["agentid"]))

            if res:
                access_token = res[0][0]
            else:
                req = requests.get(url="https://qyapi.weixin.qq.com/cgi-bin/gettoken",params={"corpid": config["corpid"],"corpsecret": config["corpsecret"]},timeout=(5, 5))
                resp = req.json()
                logging.debug("微信access_token接口返回："+str(resp))
                if resp["errcode"] == 0:
                    access_token = resp["access_token"]
                    ck.execute("INSERT INTO pipe.pipe_wxwork (corpid, agent_id, access_token) VALUES", ((config["corpid"], config["agentid"], resp["access_token"]),))
                else:
                    logging.error("access_token获取失败")
                    return
            
            # 发送消息
            send_data = Message_wxwork(config, d)
            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            send_wxwork_handler({"access_token": access_token}, send_data)
        
        elif config['type'] == 'telegram':
            # 直接发送，无需获取access_token
            # 发送消息
            logging.info("进入telegram分支")
            send_data = Message_telegram(config, d)

            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            send_telegram_handler(send_data)
        
        elif config['type'] == 'fcm':
            # See documentation on defining a message payload.
            logging.info("进入telegram分支")
            send_data = Message_fcm(config, d)

            if send_data.need_save():
                send_insert_handler(send_data.to_insert_data())
            send_fcm_handler(send_data)
        
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
            send_bark_handler(send_data)
        elif config['type'] == 'mail':
            # TODO
            pass
    
    pool.release_connection(ck)


def data_insert_handler(insert_msg):
    client = pool.get_connection()
    client.execute("INSERT INTO pipe.pipe_message (uuid, message_id, title, message) VALUES"
        ,insert_msg)
    pool.release_connection(client)


def redis_handler_stream():
    with ThreadPoolExecutor(50) as executor:
        consumer_name = uuid.uuid4().hex
        try:
            while True:
                t1 = time.time()
                
                data = rd.xreadgroup(
                    Config.REDIS_GROUP_NAME,
                    consumer_name,
                    {Config.REDIS_MSG_STREAM : ">", Config.REDIS_INSERT_STREAM: ">"},
                    count=50,
                    block=0)
                
                logging.info((rd.xpending(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME)))
                logging.info((rd.xpending(Config.REDIS_INSERT_STREAM, Config.REDIS_GROUP_NAME)))
                for stream in data:
                    # print(stream)
                    if stream[0] == Config.REDIS_MSG_STREAM:
                        for msg_id, msg in stream[1]:
                            executor.submit(data_row_handler, msg)
                            # data_row_handler(msg)
                            # 处理xack无法进行消除的bug
                            rd.xack(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME, msg_id)
                            rd.xdel(Config.REDIS_MSG_STREAM, msg_id)
                    elif stream[0] == Config.REDIS_INSERT_STREAM:
                        insert_msg = []
                        for msg_id, msg in stream[1]:
                            logging.debug("收到插入数据库"+str(msg))
                            try:
                                d = to_json_dict(msg)
                                insert_msg.append([d['uuid'],d['message_id'],d['title'],d['message']])
                            except Exception as e:
                                logging.error(e)
                                pass
                            rd.xack(Config.REDIS_INSERT_STREAM, Config.REDIS_GROUP_NAME, msg_id)
                            rd.xdel(Config.REDIS_INSERT_STREAM, msg_id)
                        
                        if len(insert_msg) != 0:
                            executor.submit(data_insert_handler, insert_msg)
        except Exception as e:
            logging.error(e)




        # logging.info("time: %s, handle data: %s" % (time.time()-t1, 100))

# def c():
#     while True:
#         previous = rd.llen(Config.REDIS_MSG_QUEUE)
#         previous2 = rd.llen(Config.REDIS_MSG_QUEUE)
#         time.sleep(10)
#         logging.info("pending job: %s msg" 
#         % (rd.xpending(Config.REDIS_MSG_STREAM, Config.REDIS_GROUP_NAME)["pending"]))
#         logging.info("inserted %s msg/10s" 
#         % (previous2 - rd.llen(Config.REDIS_INSERT_QUEUE)))
#         logging.info("connection idle: %s, active: %s." 
#         % (pool.len_available(), pool.len_active()))

thread = []

for i in range(5):
    t = Thread(target=redis_handler_stream)
    t.start()
    thread.append(t)

for i in thread:
    i.join()
