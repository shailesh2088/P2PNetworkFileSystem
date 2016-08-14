#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import xmlrpclib
import pickle,re
import hashlib
import time as time1

class Memory(LoggingMixIn, Operations):
    """Example memory filesystem. Supports only one level of files."""
    
    def __init__(self,server):
        self.s=xmlrpclib.Server(server)
        if self.get('/') == '' :
            self.put('fd','0')
            now = time()
            value = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                st_mtime=now, st_atime=now, st_nlink=2)
            self.put('/',value)
            self.put('dirl',['/'])  
        
    def chmod(self, path, mode):
        value=self.get(path,'st_mode') 
        value &= 0770000
        value |= mode
        self.put(path,value,'st_mode')
        return 0

    def chown(self, path, uid, gid):
        if uid != -1: self.put(path,uid,'st_uid')
        if gid != -1: self.put(path,gid,'st_gid')
    
    def create(self, path, mode):
        value = dict(st_mode=(S_IFREG | mode), st_nlink=1,
            st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.put(path,value)
        fd=self.incrfd()
        self.appdir(path)
        return fd
    
    def getattr(self, path, fh=None):
        dir_list=self.get('dirl')
        if path not in dir_list:
            raise FuseOSError(ENOENT)
        st = self.get(path)
        return st
    
    def getxattr(self, path, name, position=0):
        t1 = self.get(path)
        attrs= t1.get('attrs',{})
        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR
    
    def listxattr(self, path):
        t1 = self.get(path)
        attrs= t1.get('attrs',{})
        return attrs.keys()
    
    def mkdir(self, path, mode):
        dict_v = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())
        self.put(path,dict_v)
        st_nlink=self.get('/','st_nlink')+1
        self.put('/',st_nlink,'st_nlink')
        self.appdir(path)
    
    def open(self, path, flags):
        return self.incrfd()
    
    def read(self, path, size, offset, fh):
        data1=self.get(path,'data')
        data=data1[offset:offset+size]        
        return data
    
    def readdir(self, path, fh):
        dir_list=self.get('dirl')        
        return ['.', '..'] + [x[1:] for x in dir_list if x != '/']
    
    def readlink(self, path): #note
        return self.get(path,'data')
    
    def removexattr(self, path, name):
        t1 = self.get(path)
        attrs= t1.get('attrs',{})
        try:
            del attrs[name]
            self.put(path,attrs,'attrs')
        except KeyError:
            pass        # Should return ENOATTR
    
    def rename(self, old, new):
        values=self.get(old)
        self.put(new,values)
        self.remobj(old)
        self.appdir(new)
    
    def rmdir(self, path):
        self.remobj(path)
    
    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        t1 = self.get(path)
        attrs= t1.setdefault('attrs',{})
        attrs[name] = value
        self.put(path,attrs,'attrs')
    
    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)
    
    def symlink(self, target, source):
        values=dict(st_mode=(S_IFLNK | 0777), st_nlink=1,st_size=len(source))
        self.put(target,values)
        self.put(target,source,'data')
        self.appdir(target)
    
    def truncate(self, path, length, fh=None):
        data_O=self.get(path,'data')
        data_N=data_O[:length]#+'\n'
        self.put(path,data_N,'data')
        self.put(path,len(data_N),'st_size')
    
    def unlink(self, path):
        self.remobj(path)
    
    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        self.put(path,atime,'st_atime')
        self.put(path,mtime,'st_mtime')
    
    def write(self, path, data, offset, fh):
        data_O=self.get(path,'data')
        data_N=data_O[:offset]+data+data_O[offset+len(data):]
        self.put(path,data_N,'data')
        self.put(path,len(data_N),'st_size')
        return len(data)

##############################################################
############# RPC STUBS and Other functions###################
##############################################################

    def incrfd(self):
        fd=int(self.get('fd'))
        fd += 1
        self.put('fd',fd)    
        return fd  

    def appdir(self,path):
        dir_v=self.get('dirl')
        dir_v.append(path)
        self.put('dirl',dir_v)

    def remobj(self,path):
        mkey='zzzz'+path+'$'
        t1v=self.s.get(str(int(hashlib.md5(mkey).hexdigest(),16)),xmlrpclib.Binary(mkey))['value'].data
        for j in t1v:
            self.s.put(str(int(hashlib.md5(j).hexdigest(),16)),xmlrpclib.Binary(j),xmlrpclib.Binary(''),-1000)  
        self.s.put(str(int(hashlib.md5(mkey).hexdigest(),16)),xmlrpclib.Binary(mkey),xmlrpclib.Binary(''),-1000)          
        dir_v=self.get('dirl')
        dir_v.remove(path)
        self.put('dirl',dir_v)        

    def get(self,path,att=''):
        mkey='zzzz'+path+'$'
        t1=self.s.get(str(int(hashlib.md5(mkey).hexdigest(),16)),xmlrpclib.Binary(mkey))
        try:
            t1v=pickle.loads(t1["value"].data)
        except :
            return ''
        flag=0
        if re.findall(r'.(l)\d+',t1v[len(t1v)-1])==['l']:
            new_list=t1v[:len(t1v)-1]
            next_list_1=self.s.get(str(int(hashlib.md5(t1v[len(t1v)-1]).hexdigest(),16)),xmlrpclib.Binary(t1v[len(t1v)-1]))
            next_list=pickle.loads(next_list_1["value"].data)
        elif re.findall(r'.(lEND)',t1v[len(t1v)-1])==['lEND']:
            flag=1
            new_list=t1v[:len(t1v)-1]
        while flag==0:
            if re.findall(r'.(l)\d+',next_list[len(next_list)-1])==['l']:
                new_list+=next_list[:len(next_list)-1]
                next_list_1=self.s.get(str(int(hashlib.md5(next_list[len(next_list)-1]).hexdigest(),16)),xmlrpclib.Binary(next_list[len(next_list)-1]))
                next_list=pickle.loads(next_list_1["value"].data)
            elif re.findall(r'.(lEND)',next_list[len(next_list)-1])==['lEND']:
                new_list+=next_list[:len(next_list)-1]
                flag=1
        ret=''
        for j in new_list:
            t2v=self.s.get(str(int(hashlib.md5(j).hexdigest(),16)),xmlrpclib.Binary(j))['value'].data
            ret+=t2v                
        ret=pickle.loads(ret)  
        if att=='':
            return ret   
        else:
            try:
                return ret[att]
            except:
                return ''

    def put(self,path,value,att=''):
        if att=='':
            self.put_l(path,pickle.dumps(value),100000)          
        else :
            d_value=self.get(path)
            d_value[att]=value
            self.put_l(path,pickle.dumps(d_value),100000)       

    def put_l(self,key,value,ttl):
        mkey='zzzz'+key+'$'
        mvalue=[]
        if len(value) > 1024 :
            n=len(value)/1024
            r=len(value)%1024
            for i in range(n):
                nkey=mkey+str(i)                		
                nvalue=value[1024*i:1024*(i+1)]
                self.s.put(str(int(hashlib.md5(nkey).hexdigest(),16)),xmlrpclib.Binary(nkey),xmlrpclib.Binary(nvalue),ttl)  
                mvalue.append(nkey)
            if r > 0 :
                nkey=mkey+str(i+1)
                nvalue=value[1024*(i+1):]
                mvalue.append(nkey)
                self.s.put(str(int(hashlib.md5(nkey).hexdigest(),16)),xmlrpclib.Binary(nkey),xmlrpclib.Binary(nvalue),ttl)
        else:
            self.s.put(str(int(hashlib.md5(mkey+str(0)).hexdigest(),16)),xmlrpclib.Binary(mkey+str(0)),xmlrpclib.Binary(value),ttl)                   
            mvalue.append(mkey+str(0))  
        t_mvalue=mvalue+[mkey+'lEND']
        chk= self.s.put(str(int(hashlib.md5(mkey).hexdigest(),16)),xmlrpclib.Binary(mkey),xmlrpclib.Binary(pickle.dumps(t_mvalue)),ttl)


        if chk==False:
            v1=pickle.dumps(mvalue)
            n1=len(v1)/1024
            r1=len(v1)%1024
            flag=0
            j=0
            i=0
            limit=len(pickle.dumps(mvalue[:2]))-len(pickle.dumps(mvalue[0]))
            while flag==0:
                for i in range(len(mvalue)+1):
                    if mvalue[:i]==mvalue[:] : flag=1
                    if j==0:
                        if len(pickle.dumps(mvalue[:i]))>(1024-(3*limit)):
                            suff=1
                            n_key=[mkey+'l'+str(suff)]
                            t_mvalue=mvalue[:i]+n_key
                            self.s.put(str(int(hashlib.md5(mkey).hexdigest(),16)),xmlrpclib.Binary(mkey),xmlrpclib.Binary(pickle.dumps(t_mvalue)),ttl)
                            l_key=n_key
                            j=i
                    else:
                        if len(pickle.dumps(mvalue[j:i]))>(1024-(3*limit)) or flag==1 :
                            suff+=1
                            if flag !=1 : n_key=[mkey+'l'+str(suff)]
                            else : n_key=[mkey+'lEND']
                            t_mvalue=mvalue[j:i]+n_key
                            self.s.put(str(int(hashlib.md5(l_key[0]).hexdigest(),16)),xmlrpclib.Binary(l_key[0]),xmlrpclib.Binary(pickle.dumps(t_mvalue)),ttl)
                            l_key=n_key
                            j=i  
 
if __name__ == "__main__":
    if len(argv) != 3:
        print 'usage: %s %s %s<mountpoint>' % argv[0] ,argv[1] ,argv[2] 
        exit(1)
    fuse = FUSE(Memory(server=argv[2]), argv[1], foreground=True)

