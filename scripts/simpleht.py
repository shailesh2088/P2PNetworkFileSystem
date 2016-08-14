"""
Description:
The XmlRpc API for this library is:
get(base64 key)
Returns the value and ttl associated with the given key using a dictionary
or an empty dictionary if there is no matching key
Example usage:
rv = rpc.get(Binary("key"))
print rv => {"value": Binary, "ttl": 1000}
print rv["value"].data => "value"
put(base64 key, base64 value, int ttl)
Inserts the key / value pair into the hashtable, using the same key will
over-write existing values
Example usage: rpc.put(Binary("key"), Binary("value"), 1000)
print_content()
Print the contents of the HT
read_file(string filename)
Store the contents of the Hahelperable into a file
write_file(string filename)
Load the contents of the file into the Hahelperable
"""

"""
making changes to run each node in separate ports

1.) giving port number as an arguement.
splist[0] and [2] are succ
splist[1] and [3] are predec

changing find_node to accomodate data_ids lesser than the predessors id

removed indefinite loop
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest,random,hashlib
from datetime import datetime, timedelta 
from xmlrpclib import Binary
import threading,thread


# Presents a HT interface
class SimpleHT:

  def __init__(self,par):
    print par
    port=int(par[0])
    p1=int(par[1])
    p2=int(par[2])
    iden=str(par[3])
    iden1=str(par[4])
    iden2=str(par[5])
    self.quit=0
    self.max_value_size = 1024
    self.bits = 128
    self.min = 0
    self.max = pow(2, self.bits)
   
    self.spl=[]
    self.spl.append(iden2)
    self.spl.append(iden1)
    self.port=port
    self.iden=iden
    self.data = {}
    self.next_check = datetime.now() + timedelta(minutes = 5)
    self.finger_table={}	#finger table with node id as keys and port number as value    
    self.finger_table[iden2]=p2
    self.finger_table[iden1]=p1

  def s(self,port):
    return xmlrpclib.Server('http://localhost:'+str(port))

  # initialises the finger table run only once in the beginning
  def init_finger_thread(self):
    time.sleep(5)
    self.init_finger()
    return True

  def init_finger(self):
    ret=self.s(self.finger_table[self.spl[0]]).get_successor()
    self.finger_table[ret[0]]=ret[1]
    self.spl.append(ret[0])
    ret=self.s(self.finger_table[self.spl[1]]).get_predecessor()
    self.finger_table[ret[0]]=ret[1]
    self.spl.append(ret[0])
    t=long(str(self.iden)[0:1])
    for i in range(1,4):
      t=((t+30)%100)
      t1=t*(10**36)
      t2=self.find_succ(str(t1))
      print t2
      t3=t2.split(' ')
      self.finger_table[t3[0]]=t3[1]
    return True

# returns successors [id,port] as a list
  def get_successor(self):
    return [self.spl[0],self.finger_table[self.spl[0]]]

# returns predeccessors [id,port] as a list
  def get_predecessor(self):
    return [self.spl[1],self.finger_table[self.spl[1]]]

# used by init_finger and stab to find new neighbour nodes
  def find_succ(self,key_test):
    target_t=self.find_node(str(key_test))
    target=target_t.split(' ')
    if long(target[1])!=long(self.port):
      while long(key_test) > long(target[0]) :
        if long(target[1])==long(self.port) : break
        target_t1=self.s(target[1]).find_node(str(key_test))  
        target_t2=target_t1.split(' ')
        if long(target_t2[0])==long(target[0]) or long(target_t2[1])==long(self.port):
          break
        target=target_t2
    return str(target[0])+' '+str(target[1])

  def print_finger(self):
    print self.iden,len(str(self.iden)),self.port
    for i in self.finger_table.keys():
      print i,len(str(i)),self.finger_table[i]
    return True


# the heart of the code used to ascertain where to put a particular data
  def find_node(self,data_key):
    if long(data_key) <= long(self.iden):
      if long(self.iden)==long(data_key):
        t1=str(self.iden)+' '+str(self.port)       
      elif long(self.iden)<long(self.spl[1]):
        t1=str(self.iden)+' '+str(self.port)
      elif long(self.iden) > long(self.spl[1]) and long(data_key)>long(self.spl[1]):
        t1=str(self.iden)+' '+str(self.port)
      else:
        iden_list=self.finger_table.keys()
        iden_list_new1=[]
        for i in iden_list:
          iden_list_new1.append(long(i))
        iden_list_new1.sort()
        iden_list_new1.reverse()
        iden_list=iden_list_new1
        for i in iden_list:
          if long(i) < long(data_key):
            t1=str(i)+' '+str(self.finger_table[str(i)])
            break
          else : t1=str(iden_list[0])+' '+str(self.finger_table[str(iden_list[0])])

    if long(data_key) > long(self.iden):
      if long(data_key) > long(self.spl[1]) and long(self.spl[1]) > long(self.iden):
        t1=str(self.iden)+' '+str(self.port)
      elif long(data_key) <= long(self.spl[0]):
        t1=str(self.spl[0])+' '+str(self.finger_table[self.spl[0]])
      elif long(self.spl[0]) < long(self.iden):
        t1=str(self.spl[0])+' '+str(self.finger_table[self.spl[0]])
      else:
        iden_list=self.finger_table.keys()
        iden_list_new2=[]
        for i in iden_list:
          iden_list_new2.append(long(i))
        iden_list_new2.sort()
        iden_list_new2.reverse()
        iden_list=iden_list_new2
        t5=0
        for i in iden_list:
          if long(i) < long(data_key):
            t5=t5+1
            t1=str(i)+' '+str(self.finger_table[str(i)])
            break
    return t1


  def msg(self,dest_port,typ,val_iden,val_port):
    self.s(dest_port).receive(typ,val_iden,val_port)
    return True

# used to checking failure and to tell the neighbours to update their successor and predecessor
  def receive(self,typ,val_iden,val_port):
    if str(typ)=='s2':
      del self.finger_table[self.spl[2]]
      self.spl[2]=val_iden
    if str(typ)=='p2':
      del self.finger_table[self.spl[3]]
      self.spl[3]=val_iden
    else : return True
    self.finger_table[val_iden]=val_port
    return True

# check for failure
  def check_port(self,port):
    try:
      self.s(port).receive('check',1,1)
      return True
    except:
      return False
   
# stabilises the finger table in the nodes when a failure occurs
  def stab_thread(self):
    while not self.quit:
      time.sleep(30)
      self.stab()
    return True

  def stab(self):
    # check for successor
    ret=self.check_port(self.finger_table[self.spl[0]])
    if not(ret == True) :
      del self.finger_table[self.spl[0]]
      self.spl[0]=self.spl[2]
      ret=self.s(self.finger_table[self.spl[0]]).get_successor()
      self.spl[2]=ret[0]
      self.finger_table[ret[0]]=ret[1]
      self.msg(self.finger_table[self.spl[1]],'s2',  self.spl[0],self.finger_table[self.spl[0]]  )
      print "found failed successor"
      print "moving backup to new successor"
      for i in self.data.keys():
        self.s(self.finger_table[self.spl[0]]).put_backup(i,self.data[i],1000)

    # check for predecessor
    ret=self.check_port(self.finger_table[self.spl[1]])
    if not(ret == True ):
      del self.finger_table[self.spl[1]]
      self.spl[1]=self.spl[3]
      ret=self.s(self.finger_table[self.spl[1]]).get_predecessor()
      self.spl[3]=ret[0]
      self.finger_table[ret[0]]=ret[1]
      self.msg(self.finger_table[self.spl[0]],'p2',  self.spl[1],self.finger_table[self.spl[1]]  )
      # maintaining backup 
      print "found failed predesseccor"
      print "performing maintaining backup opertaion"
      for i in self.data.keys():
        self.s(self.finger_table[self.spl[0]]).put_backup(i,self.data[i],1000)


    # check for others
    for i in self.finger_table:
      ret=self.check_port(self.finger_table[self.spl[0]])
      if not( ret == True ):
        del self.finger_table[i]
        t=long(str(i)[0:1])
        t1=t*(10**36)
        t2=self.s(self.finger_table[self.spl[0]]).find_succ(str(t1))	
        self.finger_table[t2[0]]=t2[1]
    return True

          
  def count(self):
    # Remove expired entries
    self.next_check = datetime.now() - timedelta(minutes = 5)
    self.check()
    return len(self.data)

  # Retrieve something from the HT
  def get(self, data_key, key):
    target_t=self.find_node(data_key)
    target=target_t.split(' ')
    target_port=target[1]
    if long(self.port)==long(target_port) :
      # Remove expired entries
      self.check()
      # Default return value
      rv = {}
      # If the key is in the data structure, return properly formated results
      key = key.data
      if key in self.data:
        ent = self.data[key]
        now = datetime.now()
        ttl = (now - ent[1]).seconds
        if ttl > 0:
          rv = {"value": Binary(ent[0]), "ttl": ttl}
        else:
          del self.data[key]
      return rv
    else :
      if not self.check_port(target_port) :
        self.stab()
      rv=self.s(target_port).get(data_key,key)
      return rv

  # Insert something into the HT
  def put(self,data_key, key, value, ttl):
    # Remove expired entries
    print data_key,self.iden,self.port
    target_t=self.find_node(data_key)
    target=target_t.split(' ')
    target_port=target[1]
    print target[0]
    if long(self.port)==long(target_port) :
      self.check()
      end = datetime.now() + timedelta(seconds = ttl)
      self.data[key.data] = (value.data, end)
#     for backup 
      if not self.check_port(self.finger_table[self.spl[0]]) : 
        self.stab()      
      self.s(self.finger_table[self.spl[0]]).put_backup(key,value,ttl)
      return True
    else :
      if not self.check_port(target_port) : self.stab()      
      self.s(target_port).put(data_key,key,value,ttl)
      return True

# sends backup data to other nodes
  def put_backup(self,key, value, ttl):
    # Remove expired entries
    self.check()
    end = datetime.now() + timedelta(seconds = ttl)
    self.data[key.data] = (value.data, end)
    return True
   
    
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

  # Remove expired entries
  def check(self):
    now = datetime.now()
    if self.next_check > now:
      return
    self.next_check = datetime.now() + timedelta(minutes = 5)
    to_remove = []
    for key, value in self.data.items():
      if value[1] < now:
        to_remove.append(key)
    for key in to_remove:
      del self.data[key]
       
  def kill(self):
    self.quit=1
    return True

  def get_finger_avg(self):
    ret=0
    for i in self.finger_table.keys():
      ret=ret+long(i)
    avg=str(ret/len(self.finger_table.keys()))
    return avg
  

  def serve(self):

    print 'node:' ,self.iden ,len(str(self.iden)), self.port 
    for i in self.finger_table.keys():
      print i,len(str(i)),self.finger_table[i]
    print ' '

    file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', self.port))
    file_server.register_introspection_functions()
    file_server.register_function(self.get)
    file_server.register_function(self.put)
    file_server.register_function(self.print_content)
    file_server.register_function(self.read_file)
    file_server.register_function(self.write_file)
    file_server.register_function(self.kill)
    file_server.register_function(self.init_finger)
    file_server.register_function(self.find_succ)
    file_server.register_function(self.print_finger)
    file_server.register_function(self.get_predecessor)
    file_server.register_function(self.get_successor)
    file_server.register_function(self.put_backup)
    file_server.register_function(self.receive)
    file_server.register_function(self.find_node)
    file_server.register_function(self.check_port)
    file_server.register_function(self.count)
    file_server.register_function(self.get_finger_avg)

    thread.start_new_thread(self.stab_thread, () )
    thread.start_new_thread(self.init_finger_thread, () )

    while not self.quit:  
      file_server.handle_request()
    print 'killed' ,self.port
    return True



def main(par):
  sht=SimpleHT(par)
  sht.serve()

if __name__ == "__main__":
  par=sys.argv[1:]
  main(par)


er_function(self.find_node)
    file_server.register_function(self.check_port)
    file_server.register_function(self.count)
    file_server.register_function(self.get_finger_avg)

    thread.start_new_thread(self.stab_thread, () )
    thread.start_new_thread(self.init_finger_thread, () )

    while not self.quit:  
      file_server.handle_request()
    print 'killed' ,self.port
    return True



def main(par):
  sht=SimpleHT(par)
  sht.serve()

if __name__ == "__main__"