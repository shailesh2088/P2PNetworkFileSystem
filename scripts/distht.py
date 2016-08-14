#!/usr/bin/env python
"""
Description:
The XmlRpc API for this library is:
Note: node_id can either be a 32-bit int or string representation of an
integer for longer ints
DistHT(count = N, bits = M)
count is the number of SimpleHTs to generate inside the DistHT
bits is the bit length of the node_id (i.e. log2 N, where N is the maximum
number size)
get(node_id, base64 key)
Returns the value and ttl associated with the given key from the node
nearest to node_id using a dictionary or an empty dictionary if there is
no matching key
Example:
rv = rpc.get(5846006549, Binary("key"))
print rv => {"value": Binary, "ttl": 1000}
print rv["value"].data => "value"
put(node_id, base64 key, base64 value, int ttl)
Inserts the key / value pair into the hashtable the node nearest to
node_id, using the same key will over-write existing values, returns
true if successful. Value must be less than or equal to 1024 bytes.
Example: rpc.put(5846006549, Binary("key"), Binary("value"), 1000)

The following methods are for debugging only, using them in your submission
will result in a 0 for the coding portion of the assignment:

count(node_id)
Return the number of entries at this node
list_nodes()
Return a list of node ids represented by strings
print_content(node_id)
Print the contents of the HT
read_file(node_id, string filename)
Store the contents of the Hahelperable into a file
write_file(node_id, string filename)
Load the contents of the file into the Hahelperable
"""

import bisect, random, unittest, getopt, sys, SimpleXMLRPCServer, threading,subprocess ,os
import xmlrpclib, struct, math, time ,hashlib
from xmlrpclib import Binary
from simpleht_v10 import SimpleHT
from operator import itemgetter

class DistHT():

  def __init__(self, count = 50, bits = 128):
    self.nodes = {}
    start_port=51234
    self.ports=range(start_port,start_port+count,1)
    self.port_id={}
    pdir=os.getcwd()
    self.count=count

    for i in range(count):
      self.port_id[self.hashr(self.ports[i])]=self.ports[i]  # contains the ports with the port id as key

    port_id_list=self.port_id.keys()
    port_id_list.sort()
    for i in range(count):
      print port_id_list[i],len(str(port_id_list[i]))
    n37=[]
    n38=[]
    n39=[]

    # sorting did not work properly, to overcome the problems we did in the following way

    for i in port_id_list:
      if len(str(i))==37: n37.append(i)    
      if len(str(i))==38: n38.append(i)
      if len(str(i))==39: n39.append(i)
    
    n37.sort()
    n38.sort()
    n39.sort()
    port_id_list=n37+n38+n39


    self.p=[]

    print port_id_list
    for i in range(count):
      print port_id_list[i],len(str(port_id_list[i]))

    for i in range(count):
#      seperate  processes for each node
      cmdline= "/usr/bin/python simpleht_v10.py "+str(self.port_id[port_id_list[i]])+' '+str(self.port_id[port_id_list[i-1]])+' '+str(self.port_id[port_id_list[(i+1)%count]])+' '+str(port_id_list[i])+' '+str(port_id_list[i-1])+' '+str(port_id_list[(i+1)%count])
      pid = subprocess.Popen(cmdline.split(' '))
      self.p.append(pid)

  def kill(self):
    for i in range(self.count):
      self.p[i].kill()
    
  def hashr(self,key):
    print 'before:' ,int(hashlib.md5(str(key)).hexdigest(),16)
    t1=int(hashlib.md5(str(key)).hexdigest(),16) 
    print 'after:'  ,t1
    return str(t1)


def main():
  dht = DistHT()
  raw_input("enter kill threads")
  dht.kill()

if __name__ == "__main__":
  main()


+' '+str(port_id_list[i-1])+' '+str(port_id_list[(i+1)%count])
      pid = subprocess.Popen