import socket
import base64
import os
import struct
import threading
import Protocol
import time
from queue import Queue
from concurrent.futures import ProcessPoolExecutor

import setproctitle
import multiprocessing.managers # 这两次引用都很重要
from multiprocessing import Manager # 这两次引用都很重要

#def _rename_manager():
#    setproctitle.setproctitle("network") # 这里可以自定义Manager进程的名称
#
#def start(self, initializer=_rename_manager, initargs=()):
#    if self._state.value != multiprocessing.managers.State.INITIAL:
#        if self._state.value == multiprocessing.managers.State.STARTED:
#            raise ProcessError("Already started server") # type: ignore
#        elif self._state.value == multiprocessing.managers.State.SHUTDOWN:
#            raise ProcessError("Manager has shut down") # type: ignore
#        else:
#            raise ProcessError( # type: ignore
#                "Unknown state {!r}".format(self._state.value))
#
#    if initializer is not None and not callable(initializer):
#        raise TypeError('initializer must be a callable')
#
#    # pipe over which we will retrieve address of server
#    reader, writer = multiprocessing.managers.connection.Pipe(duplex=False)
#
#    # spawn process which runs a server
#    self._process = self._ctx.Process(
#        target=type(self)._run_server,
#        args=(self._registry, self._address, self._authkey,
#                self._serializer, writer, initializer, initargs),
#        )
#    ident = ':'.join(str(i) for i in self._process._identity)
#    self._process.name = type(self).__name__  + '-' + ident
#    self._process.start()
#
#    # get address of server
#    writer.close()
#    self._address = reader.recv()
#    reader.close()
#
#    # register a finalizer
#    self._state.value = multiprocessing.managers.State.STARTED
#    self.shutdown = multiprocessing.managers.util.Finalize(
#        self, type(self)._finalize_manager,
#        args=(self._process, self._address, self._authkey,
#                self._state, self._Client),
#        exitpriority=0
#        )
#
#def AutoProxy(token, serializer, manager=None, authkey=None,
#              exposed=None, incref=True, manager_owned=False):
#    _Client = multiprocessing.managers.listener_client[serializer][1]
#
#    if exposed is None:
#        conn = _Client(token.address, authkey=authkey)
#        try:
#            exposed = dispatch(conn, None, 'get_methods', (token,)) # type: ignore
#        finally:
#            conn.close()
#
#    if authkey is None and manager is not None:
#        authkey = manager._authkey
#    if authkey is None:
#        authkey = multiprocessing.process.current_process().authkey
#
#    ProxyType = multiprocessing.managers.MakeProxyType('AutoProxy[%s]' % token.typeid, exposed)
#    proxy = ProxyType(token, serializer, manager=manager, authkey=authkey,
#                      incref=incref, manager_owned=manager_owned)
#    proxy._isauto = True
#    return proxy
#
#multiprocessing.managers.AutoProxy = AutoProxy
#multiprocessing.managers.BaseManager.start = start
#### multiprocessing Manager 修复及二次开发补丁        ###

class FileServer:
    fd_dict = dict()
    fd_filequeue = dict()
    fd_ready = dict()
    def __init__(self):
        self.fileserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.fileserver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.fileserver.bind(("0.0.0.0", 8081))
        self.fileserver.listen(10)
        thread = Protocol.KThread(target=self.initTask)
        thread.start()

    def initTask(self):
        pool = ProcessPoolExecutor(30)    # 5个进程一直服务

        while 1:
            print("开始")
            conn, addr = self.fileserver.accept()
            fd = conn.recv(10).decode()
            print("fd", fd)
            self.fd_filequeue[fd] = Queue()
            print(self.fd_filequeue[fd].qsize())
            print(self.fd_filequeue.keys())
            sendTask = Protocol.KThread(target=self.handleSend, args=(conn, addr, fd,))
            recvTask = Protocol.KThread(target=self.handleReceive, args=(conn, addr, fd,))
            taskdict = dict(sendTask = sendTask, recvTask = recvTask, conn = conn)
            self.fd_dict[fd] = taskdict

            sendTask.start()
            recvTask.start()

            self.fd_ready[fd] = True
            

    def putFilePath(self, fd, path):
        print(self.fd_filequeue.keys())
        print(type(fd), type(path))
        print(self.fd_filequeue[str(fd)].qsize())
        self.fd_filequeue[str(fd)].put(path)

    def handleSend(self, conn, addr, fd):
        while True:
            print("阻塞_send")
            path = self.fd_filequeue[fd].get()
            if path == "close":
                conn.send(path.encode())
                break
            if os.path.isfile(path):
                try:
                    fp = open(path, "rb")
                    bytes = fp.read()

                    fhead = struct.pack('128sl', path.encode('utf-8'), len(bytes))
                    conn.send(fhead)

                    chunks, chunk_size = len(bytes), 1024
                    list = [bytes[i:i+chunk_size] for i in range(0, chunks, chunk_size) ]
                    for i in list:
                        print(len(i))
                        conn.send(i)
                except Exception as e:
                    print(e)
                    break


    def handleReceive(self, conn, addr, fd):
        while True:
            fileinfo_size = struct.calcsize('128sl')
            # 接收文件名与文件大小信息
            print("阻塞_recv")
            buf = conn.recv(fileinfo_size)
            print(buf)
            if buf == "close".encode():
                break
            
            if buf:
                filename, filesize = struct.unpack('128sl', buf)
                print(filesize)
                fn = filename.strip(b'\00')
                fn = fn.decode()
                print ('file new name is {0}, filesize if {1}'.format(str(fn),filesize))
                recvd_size = 0  # 定义已接收文件的大小
                # 存储在该脚本所在目录下面 
                fp = open('./' + str(fn), 'wb')
                print ('start receiving...')
                # 将分批次传输的二进制流依次写入到文件
                while not recvd_size == filesize:
                    if filesize - recvd_size > 1024:
                        data = conn.recv(1024)
                        recvd_size += len(data)
                        print("aa",recvd_size)
                    else:
                        data = conn.recv(filesize - recvd_size)
                        print(filesize - recvd_size)
                        print(len(data))
                        recvd_size = filesize
                    fp.write(data)
                fp.close()
                print("传输完成")
        print("客户端%s连接断开" % (addr,))
        conn.close()

    def fdIsReady(self, fd):
        #return self.fd_ready[str(fd)]
        return self.fd_dict.__contains__(str(fd)) and self.fd_ready.__contains__(str(fd))

    def closeEvent(self, fd):
        print(len(self.fd_filequeue), len(self.fd_dict), len(self.fd_ready))
        self.fd_filequeue.pop(str(fd))
        self.fd_dict[str(fd)].get("conn").close()
        self.fd_dict[str(fd)].get("sendTask").kill()
        self.fd_dict[str(fd)].get("recvTask").kill()
        self.fd_dict.pop(str(fd))
        self.fd_ready.pop(str(fd))
        print(len(self.fd_filequeue), len(self.fd_dict), len(self.fd_ready))

if __name__ == "__main__":
    f = FileServer()
    f.initTask()