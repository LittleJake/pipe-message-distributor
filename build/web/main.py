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
import requests
import logging

logging.basicConfig(
    level=logging.DEBUG,
    # format="%(asctime)s [*] %(processName)s  %(threadName)s  %(message)s"
    format="%(asctime)s %(message)s"
)

app = Flask(__name__)

redis_pool = redis.ConnectionPool(
    host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)
rd = redis.Redis(connection_pool=redis_pool)
ck_pool = ClickHousePool()


# 测试页面
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


# Test function
@app.route('/add_user', methods=["GET"])
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


# @app.route('/get_uid', methods=["GET"])
# def get_uid():
#     # read uid from redis TTL 5 mins.


@app.route('/github', methods=["GET"])
def github():
    # fetch github_id and save uid into redis for TTL 5 mins.
    # GET: github login button
    # POST: webhook callback from github and insert into redis redirect to get_uid.
    code = request.args.get("code", "", str)
    state = request.args.get("state", "", str)
    if code != "" and state != "":
        if not rd.exists("uuid:"+state):
            return Response("", 403)
        req = requests.post(url="https://github.com/login/oauth/access_token", data={
            "client_id": Config.GITHUB_CLIENT_ID,
            "client_secret": Config.GITHUB_CLIENT_SECRET,
            "code": code
        }, headers={"Accept": "application/json"})
        resp = req.json()
        req = requests.get(url="https://api.github.com/user", headers={
                           "Accept": "application/json", "Authorization": "token "+resp.get("access_token")})
        resp = req.json()
        if req.status_code == 200:
            return Response(
                _add_user(
                    github_id=resp["id"],
                    data={
                        "avatar": resp["avatar_url"],
                        "ip": request.remote_addr
                    }), 200)

        return Response("Unauthorized", 403)
        # data.id, data.avatar_url, data
    else:
        state = uuid.uuid4().hex
        rd.setex("uuid:"+state, 300, "")
        return Response("https://github.com/login/oauth/authorize?client_id=b3d54f810818acf20b3f&scope=user&state={}".format(state), 200)


def _add_user(github_id, data={}):
    ck = ck_pool.get_connection()
    try:
        q = ck.execute(
            "SELECT * FROM pipe.pipe_user WHERE github_id = %(github_id)s",
            {"github_id": github_id}
        )
        # Race condition
        if len(q) < 1:
            ck.execute(
                "INSERT INTO pipe.pipe_user (github_id) VALUES", ((github_id,),))

            _update_user(github_id, data)
            q = ck.execute(
                "SELECT uuid FROM pipe.pipe_user WHERE github_id = %(github_id)s",
                {"github_id": github_id}
            )
        return q[0][0]
    except Exception as e:
        logging.error(e)
        return None
    finally:
        ck_pool.release_connection(ck)


def _update_user(github_id, data={}):
    ck = ck_pool.get_connection()
    try:
        q = ck.execute(
            "SELECT uuid FROM pipe.pipe_user WHERE github_id = %(github_id)s",
            {"github_id": github_id}
        )
        if len(q) < 1 or len(data.keys()) == 0:
            return None
        else:
            sql = "ALTER TABLE pipe.pipe_user UPDATE {} WHERE github_id=%(github_id)s".format(
                ",".join(["{}=%({})s".format(k, k) for k in data.keys()])
            )
            data["github_id"] = github_id
            ck.execute(
                sql,
                data
            )

        return q[0][0]
    except Exception as e:
        logging.error(e)
        return None
    finally:
        ck_pool.release_connection(ck)

# Test


@app.route('/add_one_user', methods=["GET"])
def add_one_user():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        x = random.randint(100000000, 10000000000)
        ck.execute(
            "INSERT INTO pipe.pipe_user (github_id, wechat_id) VALUES", ((x, x),))
        result = ck.execute(
            "SELECT uuid FROM pipe.pipe_user where github_id=%(github_id)",
            {"github_id": x}
        )
        return "OK, time: %s, uuid: %s" % (time.time()-t1, result[0][0])
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)


# Test
@app.route('/add_one_config', methods=["POST"])
def add_one_config():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        uuid = request.args.get("uuid", "", str)
        name = request.form.get("name", "", str)
        config = request.form.get("config", "{}", str)

        ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES", ((
            uuid, name, config),))

        return "OK, time: %s" % (time.time()-t1)
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)


# Test
@app.route('/get_config', methods=["GET"])
def get_config():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        uuid = request.args.get("uuid", "", str)
        name = request.args.get("name", "", str)
        result = ck.execute(
            "SELECT config FROM pipe.pipe_config where uuid = %(uuid)s and name = %(name)s",
            {"uuid": uuid, "name": name}
        )
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


@app.route('/send_msg_stream', methods=["POST"])
def send_msg_stream():
    t1 = time.time()
    uid = request.args.get("uuid", "", str)
    name = request.args.get("name", "", str)
    save = request.args.get("save", "", str)
    content = request.form.get("content", "", str)
    title = request.form.get("title", "", str)

    data = {"uuid": uid, "name": name, "save": save,
            "title": title, "content": content, "time": time.time()}

    Thread(target=rd.xadd, args=(Config.REDIS_MSG_STREAM, data)).start()
    return "OK, time: %s" % (time.time()-t1)


# Test
# @app.route('/add_token',methods=["GET"])
# def add_token():
#     ck = ck_pool.get_connection()
#     try:
#         t1 = time.time()
#         n = request.args.get("n", 1, int)
#         ck = ck_pool.get_connection()
#         result = ck.execute(
#             "SELECT uuid FROM pipe.pipe_user WHERE wechat_id > {} ORDER BY wechat_id LIMIT {}"
#             .format(random.randint(0, 1000000), n))

#         # for row in result:
#         #     ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES",
#         #     )
#         ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES"
#             ,((row[0], "wechat"+str(random.randint(0, n)), "{}") for row in result))
#         return "OK, time: %s" % (time.time()-t1)
#     except Exception as e:
#         logging.error(e)
#         return Response("HTTP 500 INTERNAL ERROR", 500)
#     finally:
#         ck_pool.release_connection(ck)


@app.route('/get_message', methods=["GET"])
def get_message():
    ck = ck_pool.get_connection()
    try:
        t1 = time.time()
        uuid = request.args.get("uuid", "", str)
        message_id = request.args.get("id", "", str)

        result = ck.execute(
            "SELECT title, message FROM pipe.pipe_message WHERE uuid = %(uuid)s and message_id = %(message_id)s",
            {"uuid": uuid, "message_id": message_id}
        )

        if len(result) == 0:
            return Response("403 FORBIDDEN", 403)
        # for row in result:
        #     ck.execute("INSERT INTO pipe.pipe_config (uuid, name, config) VALUES",
        #     )
        exts = [
            'markdown.extensions.extra',
            'markdown.extensions.codehilite',
            'markdown.extensions.tables',
            'markdown.extensions.toc'
        ]

        body = markdown.markdown(result[0][1], extensions=exts)
        
        if request.is_xhr:
            return jsonify({"code": 200, "data": {"title": result[0][0], "body": body}})
        else:
            return render_template("message_template.html", title=result[0][0], body=body, time="OK, time: %s" % (time.time()-t1))
    except Exception as e:
        logging.error(e)
        return Response("HTTP 500 INTERNAL ERROR", 500)
    finally:
        ck_pool.release_connection(ck)


@app.route('/get_notice', methods=["GET"])
def get_notice():
    return jsonify(status=200, data="This is a notice.")


if __name__ == '__main__':
    app.run(host="0.0.0.0")
