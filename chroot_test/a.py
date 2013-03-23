#!/usr/bin/env python
#coding=utf-8
import os
import threading
def test():
#    os.setuid(int(os.popen("id -u %s"%"nobody").read())) 
    os.system("ls / > /aa")

os.chroot('/jail')
t = threading.Thread(target=test)
t.start()
os.system("ls / > /bb")

