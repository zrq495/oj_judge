#!/bin/bash
#sudo kill `ps aux | egrep "^nobody .*? protect.py" | cut -d " "  -f5` 
sudo pkill python
sudo nohup python protect.py &
