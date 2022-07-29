from flask import *
from clickhouse_driver import Client
import time
import redis
import json
import random
import markdown
import uuid
from ClickHousePool import ClickHousePool
import Config
from threading import Thread


app = Flask(__name__)

redis_pool = redis.ConnectionPool(host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)
rd = redis.Redis(connection_pool=redis_pool)
ck_pool = ClickHousePool()

@app.route('/')
def hello_world():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        return str(ck.execute("select 'Hello World';")[0][0]) + "\nTime:" + str(time.time() - t1)
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)

# Deprecated
# @app.route('/produce',methods=["GET"])
# def produce():
#     t1 = time.time()
#     msg = request.args.get("msg")
#     n = request.args.get("n", 1, int)
#     data = {"msg": msg, "time": time.time()}
#     pipeline = rd.pipeline()
#     for i in range(n):
#         pipeline.rpush(Config.REDIS_MSG_QUEUE, json.dumps(data))
#     pipeline.execute()
#     return "OK, msg: %s, time: %s" % (msg, time.time()-t1)



@app.route('/add_user',methods=["GET"])
def add_user():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        n = request.args.get("n", 1, int)
        ck.execute("INSERT INTO pipe.pipe_user (github_id, wechat_id) VALUES",
        ((x, x * random.randint(1, 10)) for x in range(n)))
        return "OK, time: %s" % (time.time()-t1)
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)



@app.route('/add_one_user',methods=["GET"])
def add_one_user():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        x = random.randint(100000000, 10000000000)
        ck.execute("INSERT INTO pipe.pipe_user (github_id, wechat_id) VALUES", ((x, x),))
        result = ck.execute("SELECT uuid FROM pipe.pipe_user where github_id = {}".format(x))
        return "OK, time: %s, uuid: %s" % (time.time()-t1, result[0][0])
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)



@app.route('/add_one_config',methods=["POST"])
def add_one_config():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        uuid = request.args.get("uuid", "", str)

        name = request.form.get("name", "", str)
        config = request.form.get("config", "{}", str)
        ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES", ((uuid, name, config),))
        return "OK, time: %s" % (time.time()-t1)
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)


@app.route('/get_config',methods=["GET"])
def get_config():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        uuid = request.args.get("uuid", "", str)
        name = request.args.get("name", "", str)
        result = ck.execute("SELECT config FROM pipe.pipe_config where uuid = '{}' and name = '{}'".format(uuid, name))
        return "OK, time: %s, data: %s" % (time.time()-t1, result[0][0])
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)


# Deprecated
# @app.route('/send_msg',methods=["POST"])
# def send_msg():
#     t1 = time.time()
#     uid = request.args.get("uuid", "", str)
#     name = request.args.get("name", "", str)
#     save = request.args.get("save", "", str)
#     content = request.form.get("content", "", str)
#     title = request.form.get("title", "", str)

#     data = {"uuid": uid, "name": name, "save": save, "title": title, "content": content, "time": time.time()}
#     pipeline = rd.pipeline()
#     pipeline.rpush(Config.REDIS_MSG_QUEUE, json.dumps(data))
#     pipeline.execute()

#     return "OK, time: %s" % (time.time()-t1)


@app.route('/send_msg_stream',methods=["POST"])
def send_msg_stream():
    t1 = time.time()
    uid = request.args.get("uuid", "", str)
    name = request.args.get("name", "", str)
    save = request.args.get("save", "", str)
    content = request.form.get("content", "", str)
    title = request.form.get("title", "", str)

    data = {"uuid": uid, "name": name, "save": save, "title": title, "content": content, "time": time.time()}

    Thread(target=rd.xadd, args=(Config.REDIS_MSG_STREAM, data)).start()
    return "OK, time: %s" % (time.time()-t1)


@app.route('/add_token',methods=["GET"])
def add_token():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        n = request.args.get("n", 1, int)
        ck = ck_pool.get_connection()
        result = ck.execute("SELECT uuid FROM pipe.pipe_user WHERE wechat_id > {} ORDER BY wechat_id LIMIT {}".format(random.randint(0, 1000000), n))
        
        # for row in result:
        #     ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES",
        #     )
        ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES"
            ,((row[0], "wechat"+str(random.randint(0, n)), "{}") for row in result))
        return "OK, time: %s" % (time.time()-t1)
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)




@app.route('/get_message',methods=["GET"])
def get_message():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        uuid = request.args.get("uuid", "", str)
        message_id = request.args.get("id", "", str)
        
        result = ck.execute("SELECT * FROM pipe.pipe_message WHERE uuid = '{}' and message_id = '{}'".format(uuid, message_id))

        if len(result) == 0:
            return Response("HTTP 403 FORBIDDEN", 403)
        # for row in result:
        #     ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES",
        #     )
        exts = ['markdown.extensions.extra', 'markdown.extensions.codehilite','markdown.extensions.tables','markdown.extensions.toc']

        body = markdown.markdown(result[0][3],extensions=exts)

        return render_template("message_template.html", title=result[0][2], body=body, time="OK, time: %s" % (time.time()-t1))
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)


@app.route('/get_notice',methods=["GET"])
def get_notice():
    return jsonify(status=200, data="This is a notice.")

if __name__ == '__main__':
    app.run(host="0.0.0.0")
