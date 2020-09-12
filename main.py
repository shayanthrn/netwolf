import threading
import time
import socket
import json
import socketserver


class tcpserverthread(threading.Thread):
   def __init__(self):
      threading.Thread.__init__(self)
   def run(self):
       with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
           s.bind(('127.0.0.1',free_tcpport))
           s.listen()
           while(True):
               global currusers
               if(currusers>maxserverusers):
                   pass
               else:
                   conn, addr = s.accept()
                   with conn:
                       currusers+=1
                       print('connected by',addr)
                       data = conn.recv(1024)
                       filename=data.decode('utf-8')
                       myfile= open(clusterdir+filename,"rb")
                       filecontent=myfile.read()
                       conn.sendall(filecontent)
                       currusers-=1



class tcpget(threading.Thread):
   def __init__(self,ip,port,filename,udpport):
      threading.Thread.__init__(self)
      self.port=port
      self.ip=ip
      self.filename=filename
      self.udpport=udpport
      self.udpadd=self.ip+":"+self.udpport
   def run(self):
       if(self.udpadd in recievedfrom.keys()):
           recievedfrom[self.udpadd]+=1
       else:
           recievedfrom[self.udpadd]=1
       mysocket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       mysocket.connect((self.ip,self.port))
       mysocket.sendall(self.filename.encode('utf-8'))
       mysocket.settimeout(5)
       try:
           myfile = mysocket.recv(10000)
           fp = open(clusterdir+self.filename,'wb')
           fp.write(myfile)
           fp.close()
       except:
           print("we are not able to connect try again later")
       
       



class discoverysend(threading.Thread):
   def __init__(self):
      threading.Thread.__init__(self)
   def run(self):
       sendsocket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       while(True):
           mutex.acquire()
           iplist=list(clusterdictionary.values())
           iplist.remove(clusterdictionary[name])
           byte_massage=bytes("discovery-"+json.dumps(clusterdictionary),"utf-8")
           for ip in iplist:
               sendsocket.sendto(byte_massage,(ip.split(":")[0],int(ip.split(":")[1])))
           mutex.release()
           time.sleep(distimer)


class requestthread(threading.Thread):
   def __init__(self):
      threading.Thread.__init__(self)
   def run(self):
       getsocket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       getsocket.bind(("127.0.0.1",int(port)))
       while(True):
           message, address = getsocket.recvfrom(1024)
           strmessage=message.decode("utf-8")
           print("i recieved : " , strmessage)
           command=strmessage.split('-')[0]
           data=strmessage.split('-')[1]
           if(command.lower()=="discovery"):
               mutex.acquire()
               dicdata=json.loads(data)
               clusterdictionary.update(dicdata)
               print("i am :",name,"my cluster after discovery is:",clusterdictionary)
               mutex.release()
           else:
               if(command.lower()=="get"):
                   dicdata=json.loads(data)
                   try:
                       file = open(clusterdir+dicdata['nameoffile'],"rb")
                       senddata={'address':'127.0.0.1','port':port,'tcpport':free_tcpport}
                       sendsocket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                       byte_massage=bytes("send-"+json.dumps(senddata),"utf-8")
                       if(dicdata['address']+":"+dicdata['port'] in recievedfrom.keys()):
                           print("1")
                           sendsocket.sendto(byte_massage,(dicdata['address'],int(dicdata['port'])))
                       else:
                           print("2")
                           time.sleep(5)
                           sendsocket.sendto(byte_massage,(dicdata['address'],int(dicdata['port']))) 
                   except:
                       pass
               else:
                   if(awake==1):
                       pass
                   else:
                       rectime=time.time()
                       dicdata=json.loads(data)
                       dicdata['time']=rectime-now
                       availablenodes.append(dicdata)
                       



def getfile():
    global availablenodes
    availablenodes=[]
    sendsocket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    nameoffile=input("please enter the name of file that u want(with format):")
    iplist=list(clusterdictionary.values())
    iplist.remove(clusterdictionary[name])
    data={'nameoffile':nameoffile,'address':'127.0.0.1','port':port}
    byte_massage=bytes("get-"+json.dumps(data),"utf-8")
    global now
    now=time.time()
    sleeptime=int(input("please enter the time u want to wait for response"))
    global awake
    awake=0
    for ip in iplist:
        sendsocket.sendto(byte_massage,(ip.split(":")[0],int(ip.split(":")[1])))
    time.sleep(sleeptime)
    awake=1
    if(len(availablenodes)==0):
        print("this file not exist in anynode")
    else:
        selected = availablenodes[0]
        for node in availablenodes:
            if(node['time']<selected['time']):
                selected=node
        getfilethread= tcpget(selected['address'],selected['tcpport'],nameoffile,selected['port'])
        getfilethread.start()



name=input("please enter the name of node:")
port=input("please enter the port number:")
distimer=int(input("please enter the timer of discovery:"))
clusterdir=input("please enter root directory:(cluster.txt is in it and files are in this directory,ends with '/')")
maxserverusers=int(input("for the load balancing how many people do u want to service at the same time:"))
clusterfile = open(clusterdir+"cluster.txt", "r")
clusterlist=clusterfile.read().splitlines()
clusterfile.close()
clusterdictionary={}
for a in clusterlist:
    tmp=a.split(' ')
    clusterdictionary[tmp[0]]=tmp[1]
clusterdictionary[name]="127.0.0.1:"+port
dissendthread=discoverysend()
getthread=requestthread()
awake=1
now=0
availablenodes=[]
recievedfrom={}
currusers=0
with socketserver.TCPServer(("localhost", 0), None) as s:
    free_tcpport = s.server_address[1]
tcpserver=tcpserverthread()
mutex = threading.Lock()
tcpserver.start()
dissendthread.start()
getthread.start()


while(True):
    choice=input("please enter your command:")
    if(choice.lower()=="list"):
        print(clusterdictionary)
    else:
        if(choice.lower()=="get"):
            getfile()
        else:
            if(choice.lower()=="exit"):
                exit()
            else:
                print("invalid input")