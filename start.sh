#!/bin/bash
sudo kill -9 `ps aux | egrep "^nobody .*? protect.py" | awk '{print $2}'` 
sudo nohup python protect.py &
