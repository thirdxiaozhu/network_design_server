import trace
import time
import sys
import datetime
import json
import threading


class Protocol:
    Login = 1
    Regist = 2
    addFriend = 3
    searchFriend = 4

    broadcastLogin = 10
    HEADSCUL=11

    def __init__(self) -> None:
        pass
      
class MessageFormat:
    NORMAL = 1
    IMAGE = 2
    FILE = 3


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
