import socket
import json
import select
import errno
import time
from unittest import result
import pymysql
import Protocol
import threading
from queue import Queue
from FileServer import FileServer
from sql import Sql
from datetime import datetime
import os


def main():
    sql = Sql()
    fileserver = FileServer()
    server = Server(sql, fileserver)
    server.initiateServer()


class Server:
    def __init__(self, sql, fileserver):
        ip_port = ('0.0.0.0', 8080)
        back_log = 10
        buffer_size = 1024

        self.sql = sql
        self.fileserver = fileserver
        self.fd_fileToTrans = dict()
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
        self.groupThreads = {}
        self.fddict = {}   #绑定已连接的客户ID和对应的fd
        self.fdfiletrans = {}
        self.adminfd = {}

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
        self.threads[conn.fileno()] = {}
        self.groupThreads[conn.fileno()] = {}
        self.datalist[conn.fileno()] = Queue()

    def receiveEvent(self, fd):
        datas = ''
        while True:
            try:
                data = self.connections[fd].recv(8192)
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
        while self.datalist[fd].qsize() > 0:
            sendLen = 0
            while True:
                msg = self.datalist[fd].get()
                print("bbbb" + msg)
                sendLen += self.connections[fd].send(
                    (msg[sendLen:]).encode())
                if sendLen == len(msg.encode()):
                    break
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLET | select.EPOLLERR | select.EPOLLHUP)

    def hupEvent(self, fd):
        # 有 HUP 事件激活
        self.epoll_fd.unregister(fd)
        self.connections[fd].close()

    def handleReceived(self, fd, datas):
        print(datas, type(datas))
        #防止粘包（多个json串在一个字符串里）
        dec = json.JSONDecoder()
        pos = 0
        while not pos == len(str(datas)):
            dict, json_len = dec.raw_decode(str(datas)[pos:])
            pos += json_len
            #cdict = json.loads(j)
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
                11: self.updateHead,
                12: self.getFileRequest,
                13: self.deleteFriend,
                14: self.setGroup,
                15: self.getGroups,
                16: self.deleteGroup,
                17: self.sendGroupMessage,
                18: self.getGroupMessageRecord,
                19: self.closeGroupChatWindow,
                22: self.dismissGroup,
                23: self.addGroup,
                25: self.changeStatus,
                26: self.sendGroupFile,
                27: self.getGroupFile,
                28: self.downloadGroupFile,
            }

            method = numbers.get(dict.get("msgType"))
            if method:
                method(fd, dict)

    def LoginEvent(self, fd, dict):
        account = dict.get("account")
        userinfo = self.sql.searchUser(dict)

        userinfo['msgType'] = 1
        userinfo['fd'] = fd

        self.datalist[fd].put(json.dumps(userinfo))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)


        if userinfo.get("code") == 1000 and userinfo.get("type") == 0:
            #绑定登录用户和客户端fd
            self.fddict[account] = fd
            self.fd_fileToTrans[fd] = Queue()
            self.broadcastLogin(account, 1) #广播
        elif userinfo.get("code") == 1000 and userinfo.get("type") == 1:
            self.adminfd[account] = fd


    def RegistEvent(self, fd, dict):
        result = self.sql.addUser(dict)
        ret = {'code': result}
        self.datalist[fd].put(json.dumps(ret))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def addFriend(self, fd, dict):
        result = self.sql.addFriend(dict)
        ret = {'code': result, "msgType": 3}
        self.datalist[fd].put(json.dumps(ret))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def addGroup(self, fd, dict):
        result = self.sql.addGroup(dict)
        ret = {'code': result, "msgType": Protocol.Protocol.ADD_GROUP}

        self.datalist[fd].put(json.dumps(ret))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)


    def deleteFriend(self, fd, dataDict):
        result = self.sql.deleteFriend(dataDict)
        resultDict = dict(msgType=Protocol.Protocol.DELETEFRIEND, code = result)
        self.datalist[fd].put(json.dumps(resultDict))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def deleteGroup(self, fd, dataDict):
        result = self.sql.deleteGroup(dataDict)
        resultDict = dict(msgType=Protocol.Protocol.DELETE_GROUP, code = result)
        self.datalist[fd].put(json.dumps(resultDict))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def dismissGroup(self, fd, dataDict):
        result = self.sql.dismissGroup(dataDict)
        resultDict = dict(msgType=Protocol.Protocol.DISMISS_GROUP, code = result)
        self.datalist[fd].put(json.dumps(resultDict))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def searchFriend(self, fd, dict):
        result = self.sql.searchFriend(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = 4
        self.datalist[fd].put(json.dumps(result))

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
        self.datalist[fd].put(json.dumps(result, cls=Protocol.DateEncoder))

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
                self.datalist[fd].put(json.dumps(
                    result, cls=Protocol.DateEncoder))

                self.epoll_fd.modify(
                    fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

            time.sleep(1)


    def getGroupMessageRecord(self, fd, dict):
        thread = Protocol.KThread(
            target=self.groupMessageThreadTarget, args=(dict, fd))
        self.groupThreads[fd][dict.get("target")] = thread
        thread.start()

        result = self.sql.getGroupMessageRecord(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = Protocol.Protocol.GET_GROUP_MESSAGE_RECORD
        self.datalist[fd].put(json.dumps(result, cls=Protocol.DateEncoder))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

        result = self.sql.getGroupMembers(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = Protocol.Protocol.GET_GROUP_MEMBERS
        self.datalist[fd].put(json.dumps(result, cls=Protocol.DateEncoder))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)


    def groupMessageThreadTarget(self, dict, fd):
        lastTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        thread_sql = Sql()
        while True:
            result = self.sql.getNewGroupMessage(thread_sql, dict, lastTime)
            lastTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            #没有新消息就不返回
            if result:
                result['code'] = 1000
                result['msgType'] = Protocol.Protocol.GET_NEW_GROUP_MESSAGE
                self.datalist[fd].put(json.dumps(
                    result, cls=Protocol.DateEncoder))

                self.epoll_fd.modify(
                    fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

            time.sleep(1)

    def sendMessage(self, fd, dict):
        if dict.get("form") == Protocol.MessageFormat.IMAGE:
            #recThread = threading.thread(target=reviceFile, args=())
            pass
        self.sql.sendMessage(dict)

    def sendGroupMessage(self, fd, dict):
        self.sql.sendMessage(dict)

    def closeChatWindow(self, fd, dict):
        target = dict.get("target")
        self.threads[fd].get(target).kill()
        self.threads[fd].pop(target)

    def closeGroupChatWindow(self, fd, dict):
        target = dict.get("target")
        self.groupThreads[fd].get(target).kill()
        self.groupThreads[fd].pop(target)

    def setLogout(self, fd, dict):
        result = self.sql.Logout(dict)
        if result == 1000 and dict.get("type") == 0:
            account = dict.get("account")
            self.broadcastLogin(account, 0)
            self.fileserver.closeEvent(fd)
            self.fddict.pop(account)
            print("setlogout", self.fddict)
        elif result == 1000 and dict.get("type") == 1:
            account = dict.get("account")
            self.adminfd.pop(account)

    def changeStatus(self, fd, dict):
        result = self.sql.changeStatus(dict)
        if result == 1000:
            account = dict.get("account")
            self.broadcastLogin(account, dict.get("status"))


    def sendFile(self, fd, dict):
        pass

    #广播登录信息或登出信息
    def broadcastLogin(self, account, flag):
        #找到登录登出人员对应的好友
        friend_info = self.sql.searchFriend({"account": account})
        group_info = self.sql.getGroups({"account":account})

        if friend_info:
            #向在线的好友广播状态
            for data in friend_info.get("friends"):
                friend_account = data['account_2'] if data.__contains__(
                    'account_2') else data['account_1']
                clientFd = self.fddict.get(friend_account)
                if clientFd:
                    result = dict(msgType=Protocol.Protocol.broadcastLogin, account=account, flag=flag)
                    self.datalist[clientFd].put(json.dumps(
                        result, cls=Protocol.DateEncoder))
    
                    self.epoll_fd.modify(
                        clientFd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)


        if group_info:
            for user in self.fddict:
                clientFd = self.fddict.get(user)
                for group in group_info.get("groups"):
                    result = dict(msgType=Protocol.Protocol.BROADCAST_GROUP_LOGIN, groupid = group.get("groupid"), account=account, flag=flag)
                    self.datalist[clientFd].put(json.dumps(
                        result, cls=Protocol.DateEncoder))
    
                self.epoll_fd.modify(
                    clientFd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)



    def updateHead(self, fd, dictData):
        result = self.sql.updateHead(dictData)
        
        if result == 1000:
            msg = dict(msgType=Protocol.Protocol.HEADSCUL, code=1000, filepath = dictData.get("filepath"))
        else:
            msg = dict(msgType=Protocol.Protocol.HEADSCUL, code=1001)

                
        self.datalist[fd].put(json.dumps(msg, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def addFdFileTrans(self, client):
        pass

    def getFileRequest(self, fd, data):
        def waitFileTransReady(fd):
            while True:
                if self.fileserver.fdIsReady(fd):
                    self.fileserver.putFilePath(fd, data.get("filepath"))
                    break

        thread = Protocol.KThread(
            target=waitFileTransReady, args=(fd,))
        thread.start()

    def setGroup(self, fd, dictData):
        result = self.sql.setGroup(dictData)
        if result == 1000:
            msg = dict(msgType=Protocol.Protocol.SETGROUP, code=1000)
        else:
            msg = dict(msgType=Protocol.Protocol.SETGROUP, code=1001)

        self.datalist[fd].put(json.dumps(msg, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def getGroups(self, fd, dict):
        result = self.sql.getGroups(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = Protocol.Protocol.GETGROUPS
        self.datalist[fd].put(json.dumps(result))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def sendGroupFile(self, fd, data):
        result = self.sql.sendGroupFile(data)
        result['msgType'] = Protocol.Protocol.SEND_GROUP_FILE
        self.datalist[fd].put(json.dumps(result, cls = Protocol.DateEncoder))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    def getGroupFile(self, fd, data):
        result = self.sql.getGroupFile(data)
        print(result)
        if result:
            result['msgType'] = Protocol.Protocol.GET_GROUP_FILE
            result['groupid'] = data.get("groupid")
            result['code'] = 1000
        else:
            result ={'code': 1001 }

        self.datalist[fd].put(json.dumps(result, cls = Protocol.DateEncoder))

        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

            
    def downloadGroupFile(self, fd, data):
        groupid = data.get("groupid")
        for file in data.get("files"):
            result = self.sql.downloadGroupFile(file, groupid)
            
            dataDict = dict(mgType = Protocol.Protocol.DOWNLOAD_GROUP_FILE, code = result, path = file, groupid = groupid)
            self.datalist[fd].put(json.dumps(dataDict, cls = Protocol.DateEncoder))

            self.epoll_fd.modify(
                fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)


if __name__ == "__main__":
    main()
