import socket
import json
import select
import errno
import time
import pymysql
from sql import Sql


def main():
    sql = Sql()
    server = Server(sql)
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
        self.epoll_fd.register(
            conn.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
        self.connections[conn.fileno()] = conn
        self.addresses[conn.fileno()] = addr

    def receiveEvent(self, fd):
        datas = ''
        while True:
            try:
                data = self.connections[fd].recv(1024)
                print(data.decode())
                if not data and not datas:
                    self.epoll_fd.unregister(fd)
                    self.connections[fd].close()
                    break
                else:
                    datas += data.decode()
            except socket.error as msg:
                print("读穿了===>>>")
                # 在 非阻塞 socket 上进行 recv 需要处理 读穿 的情况。这里实际上是利用 读穿 出 异常 的方式跳到这里进行后续处理
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
            print(self.datalist[fd])
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
        dict = json.loads(datas)
        numbers = {
            1: self.LoginEvent,
            2: self.RegistEvent,
            3: self.addFriend,
            4: self.searchFriend,
        }

        method = numbers.get(dict.get("msgType"))
        if method:
            method(fd, dict)

    def LoginEvent(self, fd, dict):
        userinfo = self.sql.searchUser(dict)

        if userinfo:
            userinfo['code'] = 1000
        else:
            userinfo = {'code': 1001}

        self.datalist[fd] = json.dumps(userinfo)

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def RegistEvent(self, fd, dict):
        result  = self.sql.addUser(dict)
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
        print(result)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}
        self.datalist[fd] = json.dumps(result)

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)


if __name__ == "__main__":
    main()
