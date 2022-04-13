import socket
import base64
import os
import struct
import threading
import Protocol
from concurrent.futures import ProcessPoolExecutor

class FileServer:
    def __init__(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(("0.0.0.0", 8081))
        self.server.listen(10)
        thread = Protocol.KThread(target=self.initTask)
        thread.start()

    def initTask(self):
        pool = ProcessPoolExecutor(10)    # 5个进程一直服务

        while 1:
            conn, addr = self.wait_accept()
            pool.submit(self.handle_request, conn, addr)    # 异步提交任务

    def wait_accept(self):
        conn, addr = self.server.accept()
        return conn, addr

    def handle_request(self, conn, addr):
        print("yeah")
        while True:
            fileinfo_size = struct.calcsize('128sl')
            print("hiiiiiiiii")
            # 接收文件名与文件大小信息
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
                        print(recvd_size)
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

if __name__ == "__main__":
    f = FileServer()
    f.initTask()