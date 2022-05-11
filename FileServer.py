import socket
import os
import struct
import Protocol
from queue import Queue
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager 


#文件传输业务
class FileServer:
    fd_dict = dict()
    fd_filequeue = dict()
    fd_ready = dict()

    
    def __init__(self):
        #文件传输开放端口为8081
        self.fileserver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.fileserver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.fileserver.bind(("0.0.0.0", 8081))
        self.fileserver.listen(10)
        #开始接受连接请求
        thread = Protocol.KThread(target=self.initTask)
        thread.start()

    def initTask(self):
        while True:
            conn, addr = self.fileserver.accept() #获取连接
            fd = conn.recv(10).decode() #客户端传来其在服务端的fileno
            self.fd_filequeue[fd] = Queue() #待传输队列初始化

            sendTask = Protocol.KThread(target=self.handleSend, args=(conn, addr, fd,)) #发送线程
            recvTask = Protocol.KThread(target=self.handleReceive, args=(conn, addr, fd,)) #接受线程

            #将两个线程存入二级字典便于管理
            taskdict = dict(sendTask = sendTask, recvTask = recvTask, conn = conn)
            self.fd_dict[fd] = taskdict

            sendTask.start()
            recvTask.start()

            #线程初始化完成完成,改变状态
            self.fd_ready[fd] = True

    def handleSend(self, conn, addr, fd):
        while True:
            print("阻塞_send")
            path = self.fd_filequeue[fd].get()
            #如果列表中的文件路径为“close“，则关闭线程
            if path == "close":
                conn.send(path.encode())
                break

            #如果文件存在
            if os.path.isfile(path):
                try:
                    fp = open(path, "rb")
                    bytes = fp.read() #读取文件存入bytes列表
                    fhead = struct.pack('128sl', path.encode('utf-8'), len(bytes))

                    conn.send(fhead) #首先发送文件名及长度

                    chunks, chunk_size = len(bytes), 8192
                    #按行切片
                    list = [bytes[i:i+chunk_size] for i in range(0, chunks, chunk_size) ]
                    #逐行发送
                    for i in list:
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
                dir = str(fn).split("/")
                path = '/'.join(dir[:-1])
                if not os.path.exists(path):
                    os.mkdir(path)                       # 创建路径

                fp = open('./' + str(fn), 'wb')
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

    def fdIsReady(self, fd):
        return self.fd_dict.__contains__(str(fd)) and self.fd_ready.__contains__(str(fd))
    
    #向队列中添加待发送文件
    def putFilePath(self, fd, path):
        self.fd_filequeue[str(fd)].put(path)

    def closeEvent(self, fd):
        self.fd_filequeue.pop(str(fd))
        self.fd_dict[str(fd)].get("conn").close()
        self.fd_dict[str(fd)].get("sendTask").kill()
        self.fd_dict[str(fd)].get("recvTask").kill()
        self.fd_dict.pop(str(fd))
        self.fd_ready.pop(str(fd))