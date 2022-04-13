import socket
import json
import select
import errno
import time
import pymysql
import Protocol
import threading
from FileServer import FileServer
from sql import Sql
from datetime import datetime


def main():
    sql = Sql()
    server = Server(sql)
    fileserver = FileServer()
    server.initiateServer()


class Server:
    def __init__(self, sql):
        ip_port = ('0.0.0.0', 8080)
        back_log = 10
        buffer_size = 1024

        self.sql = sql
        # 套接字类型AF_INET, socket.SOCK_STREAM   tcp协议，基于流式的协议
        self.ser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 对socket的配置重用ip和端口号
        self.ser.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ser.bind(ip_port)
        self.ser.listen(back_log)  # 最多可以连接多少个客户端
        self.ser.setblocking(0)
        try:
            # 创建 epoll 句柄
            self.epoll_fd = select.epoll()
            # 向 epoll 句柄中注册 监听 socket 的 可读 事件
            self.epoll_fd.register(
                self.ser.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
            print("listen_fd: %s" % self.ser.fileno())
        except Exception as e:
            pass

        self.connections = {}
        self.addresses = {}
        self.datalist = {}
        self.threads = {}
        self.fddict = {}

    def initiateServer(self):
        while True:
            # epoll 进行 fd 扫描的地方 -- 未指定超时时间则为阻塞等待
            epoll_list = self.epoll_fd.poll()
            for fd, events in epoll_list:
                print(fd, "===>", events)
                # 若为监听 fd 被激活
                if fd == self.ser.fileno():
                    self.connectEvent(fd)
                elif select.EPOLLIN & events:
                    self.receiveEvent(fd)
                elif select.EPOLLOUT & events:
                    self.writeEvent(fd)
                elif select.EPOLLHUP & events:
                    self.hupEvent(fd)
                else:
                    # 其他 epoll 事件不进行处理
                    continue

    def connectEvent(self, fd):
        conn, addr = self.ser.accept()
        # conn.send(("连接成功，服务端端口：%s, 本机客户端端口：%s" % (8832, addr[1])).encode())
        conn.setblocking(0)
        print(conn, addr)
        self.epoll_fd.register(
            conn.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
        self.connections[conn.fileno()] = conn
        self.addresses[conn.fileno()] = addr
        self.threads[conn.fileno()] = {}

    def receiveEvent(self, fd):
        datas = ''
        while True:
            try:
                data = self.connections[fd].recv(8192)
                print(len(data))
                if not data and not datas:
                    self.epoll_fd.unregister(fd)
                    self.connections[fd].close()
                    break
                else:
                    datas += data.decode()

            except socket.error as msg:
                print("读穿了===>>>", msg)
                # 在非阻塞socket上进行recv需要处理读穿的情况。这里实际上是利用读穿出异常的方式跳到这里进行后续处理
                if msg.errno == errno.EAGAIN:
                    self.handleReceived(fd, datas)
                    break
                else:
                    # 出错处理
                    self.epoll_fd.unregister(fd)
                    self.connections[fd].close()
                    break

    def writeEvent(self, fd):
        sendLen = 0
        while True:
            print("bbbb" + self.datalist[fd])
            sendLen += self.connections[fd].send(
                (self.datalist[fd][sendLen:]).encode())
            if sendLen == len(self.datalist[fd].encode()):
                break
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLET | select.EPOLLERR | select.EPOLLHUP)

    def hupEvent(self, fd):
        # 有 HUP 事件激活
        self.epoll_fd.unregister(fd)
        self.connections[fd].close()

    def handleReceived(self, fd, datas):
        print(datas, len(datas))
        dict = json.loads(datas)
        numbers = {
            1: self.LoginEvent,
            2: self.RegistEvent,
            3: self.addFriend,
            4: self.searchFriend,
            5: self.getMessageRecord,
            6: self.sendMessage,
            7: self.closeChatWindow,
            8: self.sendFile,
            9: self.setLogout,
        }

        method = numbers.get(dict.get("msgType"))
        if method:
            method(fd, dict)

    def LoginEvent(self, fd, dict):
        account = dict.get("account")
        userinfo = self.sql.searchUser(dict)

        if userinfo:
            userinfo['code'] = 1000
            self.fddict[account] = fd
            self.broadcastLogin(account, 1)
        else:
            userinfo = {'code': 1001}

        userinfo['msgType'] = 1

        self.datalist[fd] = json.dumps(userinfo)
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def RegistEvent(self, fd, dict):
        result = self.sql.addUser(dict)
        ret = {'code': result}
        self.datalist[fd] = json.dumps(ret)

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def addFriend(self, fd, dict):
        result = self.sql.addFriend(dict)
        print(result)
        ret = {'code': result}
        self.datalist[fd] = json.dumps(ret)

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def searchFriend(self, fd, dict):
        result = self.sql.searchFriend(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = 4
        self.datalist[fd] = json.dumps(result)

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def getMessageRecord(self, fd, dict):
        thread = Protocol.KThread(
            target=self.messageThreadTarget, args=(dict, fd))
        self.threads[fd][dict.get("target")] = thread
        thread.start()

        result = self.sql.getMessageRecord(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = 5
        self.datalist[fd] = json.dumps(result, cls=Protocol.DateEncoder)

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def messageThreadTarget(self, dict, fd):
        lastTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        thread_sql = Sql()
        while True:
            result = self.sql.getNewMessage(thread_sql, dict, lastTime)
            lastTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            #没有新消息就不返回
            if result:
                result['code'] = 1000
                result['msgType'] = 6
                self.datalist[fd] = json.dumps(
                    result, cls=Protocol.DateEncoder)

                self.epoll_fd.modify(
                    fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

            time.sleep(1)

    def sendMessage(self, fd, dict):
        if dict.get("form") == Protocol.MessageFormat.IMAGE:
            #recThread = threading.thread(target=reviceFile, args=())
            pass
        self.sql.sendMessage(dict)

    def closeChatWindow(self, fd, dict):
        target = dict.get("target")
        self.threads[fd].get(target).kill()
        self.threads[fd].pop(target)

    def setLogout(self, fd, dict):
        result = self.sql.Logout(dict)
        if result == 1000:
            account = dict.get("account")
            self.broadcastLogin(account, 0)
            self.fddict.pop(account)
            print(self.fddict)

    def sendFile(self, fd, dict):
        pass

    #广播登录信息或登出信息
    def broadcastLogin(self, account, flag):
        #找到登录登出人员对应的好友
        result = self.sql.searchFriend({"account": account})
        #向在线的好友广播状态
        for data in result.get("friends"):
            friend_account = data['account_2'] if data.__contains__(
                'account_2') else data['account_1']
            clientFd = self.fddict.get(friend_account)
            if clientFd:
                result = dict(msgType=Protocol.Protocol.broadcastLogin, account=account, flag=flag)
                self.datalist[clientFd] = json.dumps(
                    result, cls=Protocol.DateEncoder)

                self.epoll_fd.modify(
                    clientFd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)
                


if __name__ == "__main__":
    main()
