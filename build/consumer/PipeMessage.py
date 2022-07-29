import uuid
import Util
import Config

import firebase_admin
from firebase_admin import messaging
import urllib.parse

class Message():
    """
        基础Message类
    """
    def __init__(self, data):
        self._data = data
        self._message_id = uuid.uuid4().hex
    

    def need_save(self):
        return self._data['save'] != ''

    def get_data(self):
        return self._data

    def get_config(self):
        return self._config

    def get_content(self):
        content = "{}\n\n{}".format(Util.get_time(), self._data["content"], self.get_msg_url())
        if self.need_save():
            content += self.get_msg_url()
        return content

    def to_insert_data(self):
            return { 
                "uuid": self._data['uuid'],
                "message_id": self._message_id,
                "message": "{}\n\n{}".format(Util.get_time(), self._data["content"]),
                "title": self._data["title"]
            }
    
    
    def get_msg_url(self, url_type="markdown"):
        if url_type == "markdown":
            return "\n\n[查看消息](%s?uuid=%s&id=%s)" % (Config.PLATFORM_MSG_URL, self._data['uuid'], self._message_id)
        elif url_type == "encode":
            return urllib.parse.quote(self.get_msg_url("plain"))
        else:
            return "%s?uuid=%s&id=%s" % (Config.PLATFORM_MSG_URL, self._data['uuid'], self._message_id)
    
    def __str__(self):
        return ""


class Message_wxwork(Message):
    """
        企业微信
    """
    def __init__(self, config, data, msg_type="markdown", enable_duplicate_check=0, duplicate_check_interval=1800):
        Message.__init__(self, data)
        self._config = config
        self._msg_type = msg_type
        self._duplicate_check_interval = duplicate_check_interval
        self._enable_duplicate_check = enable_duplicate_check


    def to_api_data(self):
        return {
                "touser": self._config["touser"],
                "msgtype": self._msg_type,
                "agentid": self._config["agentid"],
                self._msg_type: {
                    "content": self.get_content()
                },
                "enable_duplicate_check": self._enable_duplicate_check,
                "duplicate_check_interval": self._duplicate_check_interval,
            }

class Message_telegram(Message):
    '''
    Telegram消息
    '''
    def __init__(self, config, data, msg_type="Markdown"):
        Message.__init__(self, data)
        self._config = config
        self._msg_type = msg_type

    def to_api_data(self):
        return {
            "chat_id": self._config["chat_id"],
            "text": self.get_content(),
            "parse_mode": self._msg_type
        }


class Message_bark(Message):
    '''
    Bark消息
    '''
    def __init__(self, config, data):
        Message.__init__(self, data)
        self._config = config
        self._data['save'] = '1'
    
    def to_api_data(self):
        return {"title": self._data["title"],}

class Message_fcm(Message):
    '''
    FCM消息
    '''
    def __init__(self, config, data):
        Message.__init__(self, data)
        self._config = config
    
    def to_api_data(self):
        return messaging.Message(
                message = {
                    "title": self._data["title"],
                    "body": self.get_content(),
                },
                token = self._config['token']
            )