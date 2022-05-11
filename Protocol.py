import sys
import datetime
import json
import threading

#协议类型
class Protocol:
    Login = 1
    Regist = 2
    addFriend = 3
    searchFriend = 4
    SENDMESSAGE = 6
    broadcastLogin = 10
    HEADSCUL = 11
    DELETEFRIEND = 13
    SETGROUP = 14
    GETGROUPS = 15
    DELETE_GROUP = 16
    SENDGROUPMESSAGE = 17
    GET_GROUP_MESSAGE_RECORD = 18
    GET_NEW_GROUP_MESSAGE = 20
    GET_GROUP_MEMBERS = 21
    DISMISS_GROUP = 22
    ADD_GROUP = 23
    SEND_GROUP_FILE = 26
    GET_GROUP_FILE = 27
    DOWNLOAD_GROUP_FILE = 28
    BROADCAST_GROUP_LOGIN = 29
    ADMIN_USER_LOGIN_MSG = 30
    SAVE_PROFILE = 31
    ADMIN_LOGIN_MSG = 32
    ADMIN_USER_LOGOUT_MSG = 33

#消息类型
class MessageFormat:
    NORMAL = 1
    IMAGE = 2
    FILE = 3

#解析带日期的json格式
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

#可停止线程，继承与threading.Thread


class KThread(threading.Thread):
    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.killed = False

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, why, arg):
        if why == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, why, arg):
        if self.killed:
            if why == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True
