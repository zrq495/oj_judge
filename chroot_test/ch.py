#!/usr/bin/env python
#coding=utf-8
import os
real_root = os.open("/", os.O_RDONLY)
data_root = os.open("/data/1000/data1.in", os.O_RDONLY)
print data_root
os.chroot('/jail')
os.setuid(65534) 
cmd = "/a.out"
os.system(cmd)
print os.read(data_root,100)

#while True:
cmd = raw_input('>>')
os.system(cmd)

os.fchdir(real_root)
os.setuid(0) 
os.chroot('.')
cmd = raw_input('>>')
os.system(cmd)
