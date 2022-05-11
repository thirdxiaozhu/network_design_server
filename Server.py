import socket
import json
import select
import errno
import time
import Protocol
from queue import Queue
from FileServer import FileServer
from sql import Sql
from datetime import datetime

#入口方法
def main():
    sql = Sql() #数据库实例
    fileserver = FileServer() #文件处理实体
    server = Server(sql, fileserver) #构造服务端，
    server.initiateServer()


"""
    @author: jiaxv
    @Description: 服务端业务处理 
"""
class Server:
    def __init__(self, sql, fileserver):
        ip_port = ('0.0.0.0', 8080)
        back_log = 10

        self.sql = sql
        self.fileserver = fileserver
        self.fd_fileToTrans = dict()
        # 套接字类型AF_INET, socket.SOCK_STREAM   tcp协议，基于流式的协议
        self.ser = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 对socket的配置重用ip和端口号
        self.ser.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ser.bind(ip_port)  #绑定端口
        self.ser.listen(back_log)  # 最多可以连接多少个客户端
        self.ser.setblocking(0)  #非阻塞
        try:
            # 创建 epoll 句柄
            self.epoll_fd = select.epoll()
            # 向epoll句柄中注册监听socket事件
            self.epoll_fd.register(
                self.ser.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)
            print("listen_fd: %s" % self.ser.fileno())
        except Exception as e:
            pass

        self.connections = {}   #当前处于通信中的客户端字典  { 客户socket的fileno() : 连接 }
        self.datalist = {}  #每一客户端尚未处理信息字典 { 客户fileno() : Queue() }
        self.threads = {}   #每一客户端聊天信息循环监听线程 { 客户fileno() : { 该客户好友account : 线程} }
        self.groupThreads = {}   #每一客户端群聊信息循环监听线程 { 客户fileno() : { 该客户好友account : 线程} }
        self.fddict = {}  # 绑定已连接的客户ID和对应的fd
        self.fdfiletrans = {} #每一客户端文件传输线程 { 客户fileno() : Queue() }
        self.adminfd = {}  #管理员管理字典 { 管理员fileno() : 连接 }
        self.adminClass = self.Admin(self) #管理员内部类


    #初始化服务端业务,开始监听事件
    def initiateServer(self):
        while True:
            # epoll进行fd扫描 -- 未指定超时时间则为阻塞等待
            epoll_list = self.epoll_fd.poll()
            for fd, events in epoll_list:
                print(fd, "===>", events)
                # 若为监听fd被激活
                if fd == self.ser.fileno():
                    self.connectEvent()
                elif select.EPOLLIN & events:
                    self.receiveEvent(fd)
                elif select.EPOLLOUT & events:
                    self.writeEvent(fd)
                elif select.EPOLLHUP & events:
                    self.hupEvent(fd)
                else:
                    # 其他epoll事件不进行处理
                    continue

    #连接事件
    def connectEvent(self):
        conn, addr = self.ser.accept()
        conn.setblocking(0)     #连接非阻塞
        self.epoll_fd.register(
            conn.fileno(), select.EPOLLIN | select.EPOLLERR | select.EPOLLHUP)   #注册该连接至epoll

        self.connections[conn.fileno()] = conn
        #self.addresses[conn.fileno()] = addr
        self.threads[conn.fileno()] = {}
        self.groupThreads[conn.fileno()] = {}
        self.datalist[conn.fileno()] = Queue()

    #处理socket接收
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

    #向socket中写入
    def writeEvent(self, fd):
        while self.datalist[fd].qsize() > 0:
            sendLen = 0
            while True:
                msg = self.datalist[fd].get()
                print(fd, msg)
                sendLen += self.connections[fd].send(
                    (msg[sendLen:]).encode())
                if sendLen == len(msg.encode()):
                    break
        #修改epoll中fd对应的状态
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLET | select.EPOLLERR | select.EPOLLHUP)

    def hupEvent(self, fd):
        # 有 HUP 事件激活
        self.epoll_fd.unregister(fd)
        self.connections[fd].close()

    def handleReceived(self, fd, datas):
        #防止粘包（多个json串在一个字符串里）
        pos = 0
        try:
            #如果有粘包，分离该报文中每一个json串
            while not pos == len(str(datas)):
                dict, json_len = json.JSONDecoder().raw_decode(str(datas)[pos:])
                pos += json_len
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
                    21: self.getGroupMembers,
                    22: self.dismissGroup,
                    23: self.addGroup,
                    25: self.changeStatus,
                    26: self.sendGroupFile,
                    27: self.getGroupFile,
                    28: self.downloadGroupFile,
                    31: self.saveProfile,
                }

                #根据msgType进行业务处理
                method = numbers.get(dict.get("msgType"))
                if method:
                    method(fd, dict)
        except:
            return

    #登录业务
    def LoginEvent(self, fd, dict):
        account = dict.get("account")
        userinfo = self.sql.searchUser(dict)

        userinfo['msgType'] = 1
        userinfo['fd'] = fd

        self.datalist[fd].put(json.dumps(userinfo))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

        #如果登录的是普通用户
        if userinfo.get("code") == 1000 and userinfo.get("type") == 0:
            #绑定登录用户和客户端fd
            self.fddict[account] = fd
            self.fd_fileToTrans[fd] = Queue()
            #向所有已连接的用户广播该用户登录状态
            self.broadcastLogin(account, 1)  # 广播
            self.adminClass.userLoginMessage(userinfo)
        
        #如果登录的是管理员
        elif userinfo.get("code") == 1000 and userinfo.get("type") == 1:
            self.adminfd[account] = fd
            self.adminClass.adminLoginEvent(fd)

    #注册业务
    def RegistEvent(self, fd, dict):
        result = self.sql.addUser(dict)
        ret = {'code': result}

        self.datalist[fd].put(json.dumps(ret))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #添加好友
    def addFriend(self, fd, dict):
        result = self.sql.addFriend(dict)
        ret = {'code': result, "msgType": 3}

        self.datalist[fd].put(json.dumps(ret))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #添加群
    def addGroup(self, fd, dict):
        result = self.sql.addGroup(dict)
        ret = {'code': result, "msgType": Protocol.Protocol.ADD_GROUP}

        self.datalist[fd].put(json.dumps(ret))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #删除好友
    def deleteFriend(self, fd, dataDict):
        result = self.sql.deleteFriend(dataDict)
        resultDict = dict(msgType=Protocol.Protocol.DELETEFRIEND, code=result)

        self.datalist[fd].put(json.dumps(resultDict))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #退出群
    def deleteGroup(self, fd, dataDict):
        result = self.sql.deleteGroup(dataDict)
        resultDict = dict(msgType=Protocol.Protocol.DELETE_GROUP, code=result,
                          account=dataDict.get("account"), groupid=dataDict.get("target"))

        self.datalist[fd].put(json.dumps(resultDict))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #解散群
    def dismissGroup(self, fd, dataDict):
        result = self.sql.dismissGroup(dataDict)
        resultDict = dict(msgType=Protocol.Protocol.DISMISS_GROUP, code=result)

        self.datalist[fd].put(json.dumps(resultDict))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #搜索用户好友
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

    #首次取得聊天记录
    def getMessageRecord(self, fd, dict):
        #开启循环监听聊天记录交换线程
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

    #循环监听聊天记录
    def messageThreadTarget(self, dict, fd):
        lastTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")      #上一次查询时间（当前时间）,精确到毫秒
        thread_sql = Sql()      #创建新数据库事务实体
        while True:
            result = self.sql.getNewMessage(thread_sql, dict, lastTime) 
            lastTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") #更新上次查询时间

            #没有新消息就不返回
            if result:
                result['code'] = 1000
                result['msgType'] = 6

                self.datalist[fd].put(json.dumps(
                    result, cls=Protocol.DateEncoder))
                self.epoll_fd.modify(
                    fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

            #每一秒查询一次
            time.sleep(1)

    #首次获取群聊内容
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

    #获取群成员
    def getGroupMembers(self, fd, dict):
        result = self.sql.getGroupMembers(dict)
        if result:
            result['code'] = 1000
        else:
            result = {"code": 1001}

        result['msgType'] = Protocol.Protocol.GET_GROUP_MEMBERS

        self.datalist[fd].put(json.dumps(result, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #用户发送好友聊天消息
    def sendMessage(self, fd, dict):
        self.sql.sendMessage(dict)

    #用户发送群聊信息
    def sendGroupMessage(self, fd, dict):
        self.sql.sendMessage(dict)

    #用户关闭和某人窗口事件
    def closeChatWindow(self, fd, dict):
        target = dict.get("target")
        self.threads[fd].get(target).kill()
        self.threads[fd].pop(target)

    #用户关闭某群聊窗口事件
    def closeGroupChatWindow(self, fd, dict):
        target = dict.get("target")
        self.groupThreads[fd].get(target).kill()
        self.groupThreads[fd].pop(target)

    #用户退出
    def setLogout(self, fd, dict):
        result = self.sql.Logout(dict)
        #普通用户下线
        if result == 1000 and dict.get("type") == 0:
            account = dict.get("account")

            self.broadcastLogin(account, 0)    #向在线好友广播状态
            self.fileserver.closeEvent(fd)
            self.fddict.pop(account)
            print("setlogout", self.fddict)
            self.adminClass.userLogoutMessage(account)
        #管理员下线
        elif result == 1000 and dict.get("type") == 1:
            account = dict.get("account")
            self.adminfd.pop(account)

    #用户在线状态切换
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
        group_info = self.sql.getGroups({"account": account})

        if friend_info:
            #向在线的好友广播状态
            for data in friend_info.get("friends"):
                friend_account = data['account_2'] if data.__contains__(
                    'account_2') else data['account_1']
                clientFd = self.fddict.get(friend_account)
                if clientFd:
                    result = dict(
                        msgType=Protocol.Protocol.broadcastLogin, account=account, flag=flag)

                    self.datalist[clientFd].put(json.dumps(
                        result, cls=Protocol.DateEncoder))
                    self.epoll_fd.modify(
                        clientFd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

        if group_info:
            for user in self.fddict:
                clientFd = self.fddict.get(user)
                for group in group_info.get("groups"):
                    result = dict(msgType=Protocol.Protocol.BROADCAST_GROUP_LOGIN, groupid=group.get(
                        "groupid"), account=account, flag=flag)
                    self.datalist[clientFd].put(json.dumps(
                        result, cls=Protocol.DateEncoder))

                self.epoll_fd.modify(
                    clientFd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #更新头像
    def updateHead(self, fd, dictData):
        result = self.sql.updateHead(dictData)

        if result == 1000:
            msg = dict(msgType=Protocol.Protocol.HEADSCUL,
                       code=1000, filepath=dictData.get("filepath"))
        else:
            msg = dict(msgType=Protocol.Protocol.HEADSCUL, code=1001)

        self.datalist[fd].put(json.dumps(msg, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #获取文件请求
    def getFileRequest(self, fd, data):
        def waitFileTransReady(fd):
            #不断查看线程状态，若准备好，则退出循环关闭线程
            while True:
                if self.fileserver.fdIsReady(fd):
                    self.fileserver.putFilePath(fd, data.get("filepath"))
                    break

        #等待文件线程准备好
        thread = Protocol.KThread(
            target=waitFileTransReady, args=(fd,))
        thread.start()

    #新建群
    def setGroup(self, fd, dictData):
        result = self.sql.setGroup(dictData)
        if result == 1000:
            msg = dict(msgType=Protocol.Protocol.SETGROUP, code=1000)
        else:
            msg = dict(msgType=Protocol.Protocol.SETGROUP, code=1001)

        self.datalist[fd].put(json.dumps(msg, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #获得用户所处群信息
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

    #上传群文件
    def sendGroupFile(self, fd, data):
        result = self.sql.sendGroupFile(data)
        result['msgType'] = Protocol.Protocol.SEND_GROUP_FILE

        self.datalist[fd].put(json.dumps(result, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #获取群文件列表
    def getGroupFile(self, fd, data):
        result = self.sql.getGroupFile(data)
        print(result)
        if result:
            result['msgType'] = Protocol.Protocol.GET_GROUP_FILE
            result['groupid'] = data.get("groupid")
            result['code'] = 1000
        else:
            result = {'code': 1001}

        self.datalist[fd].put(json.dumps(result, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #下载群文件
    def downloadGroupFile(self, fd, data):
        groupid = data.get("groupid")
        for file in data.get("files"):
            result = self.sql.downloadGroupFile(file, groupid)

            dataDict = dict(mgType=Protocol.Protocol.DOWNLOAD_GROUP_FILE,
                            code=result, path=file, groupid=groupid)

            self.datalist[fd].put(json.dumps(
                dataDict, cls=Protocol.DateEncoder))
            self.epoll_fd.modify(
                fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #保存个人信息修改
    def saveProfile(self, fd, data):
        result = self.sql.saveProfile(data)
        resultDict = dict(msgType=Protocol.Protocol.SAVE_PROFILE, code=result,
                          nickname=data.get("nickname"), signature=data.get("signature"))

        self.datalist[fd].put(json.dumps(resultDict, cls=Protocol.DateEncoder))
        self.epoll_fd.modify(
            fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

    #管理员业务内部类
    class Admin:
        def __init__(self, server) -> None:
            self.server = server

        #普通用户登录信息向全体在线管理员广播
        def userLoginMessage(self, dict):
            dict['msgType'] = Protocol.Protocol.ADMIN_USER_LOGIN_MSG
            for fd in self.server.adminfd.values():
                self.server.datalist[fd].put(
                    json.dumps(dict, cls=Protocol.DateEncoder))
                self.server.epoll_fd.modify(
                    fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

        #普通用户登出信息向全体在线管理员广播
        def userLogoutMessage(self, account):
            dataDict = dict(msgType = Protocol.Protocol.ADMIN_USER_LOGOUT_MSG, account = account)
            for fd in self.server.adminfd.values():
                self.server.datalist[fd].put(
                    json.dumps(dataDict, cls=Protocol.DateEncoder))
                self.server.epoll_fd.modify(
                    fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

        #管理员上线
        def adminLoginEvent(self ,fd):
            account_list = []
            for key in self.server.fddict.keys():
                account_list.append(key)
            dataDict = dict(msgType = Protocol.Protocol.ADMIN_LOGIN_MSG, accounts = account_list)
            print("dict", dataDict)
            self.server.datalist[fd].put(
                json.dumps(dataDict, cls=Protocol.DateEncoder))

            self.server.epoll_fd.modify(
                fd, select.EPOLLIN | select.EPOLLOUT | select.EPOLLERR | select.EPOLLHUP)

#程序入口
if __name__ == "__main__":
    main()
