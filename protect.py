#!/usr/bin/env python
#coding=utf-8
import os
import re
import sys
import psutil
import subprocess
import codecs
import logging
import shlex
import time
import MySQLdb
import config
import lorun
from Queue import Queue
from threading import Thread,Lock
os.setuid(int(os.popen("id -u %s"%"nobody").read()))
q = Queue(config.queue_size)   #初始化队列
dblock = Lock()
def worker():
    while True:
        if q.empty() is True:
            logging.info("idle")
        task = q.get()
        solution_id = task['solution_id']
        problem_id = task['problem_id']
        language = task['pro_lang']
        user_id = task['user_id']
        data_count = get_data_count(task['problem_id'])
        logging.info("judging %s"%solution_id)
        result=run(problem_id,solution_id,language,data_count,user_id)
        logging.info("%s result %s"%(result['solution_id'],result['result']))
        dblock.acquire()
        update_result(result)
        dblock.release()
        q.task_done()

def start_work_thread():
    for i in range(config.count_thread): #依次启动工作线程
        t = Thread(target=worker, name="judge%d"%i)
        t.deamon = True
        t.start()
def start_get_task():
    t = Thread(target=put_task_into_queue, name="get_task")
    t.deamon = True
    t.start()

def update_compile_info(solution_id,info):
    con = None
    try:
        con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                              config.db_name,charset=config.db_charset)
    except:
        logging.error('runid %s in update_compile_info cannot connect to database'%solution_id)
        sys.exit(-1)
    cur = con.cursor()
    info = MySQLdb.escape_string(info)
    sql = "insert into compile_info(solution_id,compile_info) values (%s,'%s')"%(solution_id,info)
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()

def get_problem_limit(problem_id):
    con = None
    try:
        con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                              config.db_name,charset=config.db_charset)
    except:
        logging.error('in get_problem_limit cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    sql = "select time_limit,memory_limit from problem where problem_id = %s"%problem_id
    cur.execute(sql)
    data = cur.fetchone()
    cur.close()
    con.close()
    return data

def update_solution_status(solution_id,result=12):
    logging.debug("update solution status")
    con = None
    try:
        con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                              config.db_name,charset=config.db_charset)
    except:
        logging.error('in update_solution_status cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    update_sql = "update solution set result = %s where solution_id = %s"%(solution_id,result)
    cur.execute(update_sql)
    con.commit()
    cur.close()
    con.close()
    return 0


def update_result(result):
    con = None
    try:
        con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                              config.db_name,charset=config.db_charset)
    except:
        logging.error('in update_result cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    sql = "update solution set take_time = %s , take_memory = %s, result = %s where solution_id = %s"%(result['take_time'],result['take_memory'],result['result'],result['solution_id'])
    cur.execute(sql)
    update_ac_sql = "update user set accept = (select count(distinct problem_id) from solution where result = 1 and user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_sub_sql = "update user set submit = (select count(problem_id) from solution where user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_problem_ac="UPDATE problem SET accept=(SELECT count(*) FROM solution WHERE problem_id=%s AND result=1) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    update_problem_sub="UPDATE problem SET submit=(SELECT count(*) FROM solution WHERE problem_id=%s) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    cur.execute(update_ac_sql)
    cur.execute(update_sub_sql)
    cur.execute(update_problem_ac)
    cur.execute(update_problem_sub)
    con.commit()
    cur.close()
    con.close()
    return 0


def get_data_count(problem_id):
    full_path = "/data/%s/"%problem_id
    try:
        files = os.listdir(full_path)
    except OSError,e:
        logging.error("124")
        logging.error(e)
        return 0
    count = 0
    for item in files:
        if item.endswith(".in"):
            count += 1
    return count

def get_code(solution_id,problem_id,pro_lang):
    file_name = {
        "gcc":"main.c",
        "g++":"main.cpp",
        "java":"Main.java",
        "pascal":"main.pas",
    }
    con = None
    try:
        con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                              config.db_name,charset=config.db_charset)
    except:
        logging.error('in put_task_into_queue cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    select_code_sql = "select code_content from code where solution_id = %s"%solution_id
    cur.execute(select_code_sql)
    try:
        feh = cur.fetchone()
        if feh is not None:
            code = feh[0]
        else:
            code = ''
            logging.error("cannot get code of runid %s"%solution_id)
            return False
    except TypeError,e:
        logging.error("163")
        logging.error(e)
        return False
    try:
        os.mkdir('/work/%s/'%solution_id)
    except OSError,e:
        if str(e).find("exist")>0:
            pass
        else:
            logging.error("172")
            logging.error(e)
            return False
    try:
        real_path = "/work/%s/%s"%(solution_id,file_name[pro_lang])
    except KeyError,e:
        logging.error("177")
        logging.error(e)
        return False
    try:
        f = codecs.open(real_path,'w',encoding='utf-8',errors='ignore')
        code = del_code_note(code)
        try:
            f.write(code)
        except:
            logging.error("not write code to file")
            f.close()
            return False
        f.close()
    except OSError,e:
        logging.error("189")
        logging.error(e)
        return False
    return True

def del_code_note(code):
    code = re.sub("//.*",'',code)
    code = re.sub("/\*.*?\*/",'',code,flags=re.M|re.S)
    return code


def put_task_into_queue():
#    judged = []
    while True:
        con = None
        try:
            con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                                  config.db_name,charset=config.db_charset)
        except:
            logging.error('in put_task_into_queue cannot connect to database')
            sys.exit(-1)
        cur = con.cursor()
        sql = "select solution_id,problem_id,user_id,contest_id,pro_lang from solution where result = 0"
#        logging.debug('getting solution')
        cur.execute(sql)
        data = cur.fetchall()
        cur.close()
        con.close()
        for i in data:
            solution_id,problem_id,user_id,contest_id,pro_lang = i
            dblock.acquire()
            ret = get_code(solution_id,problem_id,pro_lang)
            dblock.release()
            if ret == False:
                update_solution_status(solution_id,11)
            task = {
                "solution_id":solution_id,
                "problem_id":problem_id,
                "contest_id":contest_id,
                "user_id":user_id,
                "pro_lang":pro_lang,
            }
#            if solution_id in judged:
#                continue
 #           else:
#            judged.append(solution_id)
            q.put(task)
            dblock.acquire()
            update_solution_status(solution_id)
            dblock.release()
        time.sleep(0.2)


def compile(solution_id,language):
    '''将程序编译成可执行文件'''
    language = language.lower()
    dir_work = "/work/%s/"%solution_id
    if language == "gcc":
        cmd = "gcc main.c -o main -Wall -lm -O2 -std=c99 --static -DONLINE_JUDGE"
    elif language == 'g++':
        cmd = "g++ main.cpp -O2 -Wall -lm --static -DONLINE_JUDGE -o main"
    elif language == "java":
        cmd = "javac Main.java"
    elif language == 'pascal':
        cmd = 'fpc main.pas -O2 -Co -Ct -Ci'
    else:
        return False
    p = subprocess.Popen(cmd,shell=True,cwd=dir_work,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err =  p.communicate()
    f = file("/work/%s/error.txt"%solution_id,'w')
    f.write(err)
    f.write(out)
    f.close()
    if p.returncode == 0:
        return True
    dblock.acquire()
    update_compile_info(solution_id,err+out)
    dblock.release()
    return False

def judge_result(problem_id,solution_id,data_num):
    logging.debug("Judging result")
    currect_result = "/data/%s/data%s.out"%(problem_id,data_num)
    user_result = "/work/%s/out%s.txt"%(solution_id,data_num)
    curr = file(currect_result).read().replace('\r','')
    user = file(user_result).read().replace('\r','')
    if curr == user:
        return "Accepted"
    if curr.split() == user.split():
        return "Presentation Error"
    if curr in user:
        return "Output limit"
    return "Wrong Answer"

def get_max_mem(pid):
	glan = psutil.Process(pid)
	max = 0
	while True:
		try:
			rss,vms = glan.get_memory_info()
			if rss > max:
				max = rss
		except:
			print "max rss = %s"%max
			return max

def judge_one_c(solution_id,problem_id,data_num,time_limit,mem_limit):
    input_data = file("/data/%s/data%s.in"%(problem_id,data_num))
    temp_out_data = file("/work/%s/out%s.txt"%(solution_id,data_num),'w')
    main_exe = '/work/%s/main'%(solution_id)
    runcfg = {
        'args':[main_exe],
        'fd_in':input_data.fileno(),
        'fd_out':temp_out_data.fileno(),
        'timelimit':time_limit, #in MS
        'memorylimit':mem_limit, #in KB
    }
    rst = lorun.run(runcfg)
    input_data.close()
    temp_out_data.close()
    logging.info(rst)
    return rst

#def judge_java(solution_id,problem_id,data_count,time_limit,mem_limit):

def run(problem_id,solution_id,language,data_count,user_id):
    '''获取程序执行时间和内存'''
    dblock.acquire()
    time_limit,mem_limit=get_problem_limit(problem_id)
    dblock.release()
    time_limit = (time_limit+10)/1000.0
    mem_limit = mem_limit * 1024
    program_info = {
        "solution_id":solution_id,
        "problem_id":problem_id,
        "take_time":0,
        "take_memory":0,
        "user_id":user_id,
        "result":None,
    }
    result_code = {
        "Waiting":0,
        "Accepted":1,
        "Time Limit Exceeded":2,
        "Memory Limit Exceeded":3,
        "Wrong Answer":4,
        "Runtime Error":5,
        "Output limit":6,
        "Compile Error":7,
        "Presentation Error":8,
        "System Error":11,
        "Judging":12,
    }
    compile_result = compile(solution_id,language)
    if compile_result is False:#编译错误
        program_info['result'] = result_code["Compile Error"]
        return program_info
    if data_count == 0:#没有测试数据
        program_info['result'] = result_code["System Error"]
        return program_info
    if language == 'java':
        cmd = "/usr/bin/java Main"
    work_dir = '/work/%s'%solution_id
    max_rss = 0
    max_vms = 0
    total_time = 0
    for i in range(data_count):
        args = shlex.split(cmd)
        input_data = file('/data/%s/data%s.in'%(problem_id,i+1))
        output_data = file('/work/%s/out%s.txt'%(solution_id,i+1),'w')
        run_err_data = file('/work/%s/run_err%s.txt'%(solution_id,i+1),'w')
#        fcntl.flock(output_data,fcntl.LOCK_EX|fcntl.LOCK_NB)
        p = subprocess.Popen(args,env={"PATH":"/nonexistent"},cwd=work_dir,stdout=output_data,stdin=input_data,stderr=run_err_data)
#        a = Thread(target=get_max_mem,args=([p.pid,]))
#        a.daemon = True
#        a.start()
        start = time.time()
        pid = p.pid
        logging.debug(pid)
        glan = psutil.Process(pid)
        while True:
            time_to_now = time.time()-start + total_time
            if psutil.pid_exists(pid) is False:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024.0
                program_info['result'] = result_code["Runtime Error"]
                return program_info
#            fcntl.flock(output_data,fcntl.LOCK_EX|fcntl.LOCK_NB)
            rss,vms = glan.get_memory_info()
#            p.communicate(input_data.read())
#            fcntl.flock(output_data,fcntl.LOCK_UN)
            if p.poll() == 0:
                end = time.time()
                break
#            logging.debug((rss,vms))
            if max_rss < rss:
                max_rss = rss
                print 'max_rss=%s'%max_rss
            if max_vms < vms:
                max_vms = vms
            if time_to_now > time_limit:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024.0
                program_info['result'] = result_code["Time Limit Exceeded"]
                glan.terminate()
                return program_info
            if max_rss > mem_limit:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024.0
                program_info['result'] =result_code["Memory Limit Exceeded"]
                glan.terminate()
                return program_info

        total_time += end - start
        logging.debug("max_rss = %s"%max_rss)
#        print "max_rss=",max_rss
        logging.debug("max_vms = %s"%max_vms)
#        logging.debug("take time = %s"%(end - start))
        result = judge_result(problem_id,solution_id,i+1)
        if result == "Wrong Answer" or result == "Output limit":
            program_info['result'] = result
            break
        elif result == 'Presentation Error':
            program_info['result']=result
        elif result == 'Accepted':
            if program_info['result'] != 'Presentation Error':
                program_info['result'] = result
        else:
            logging.error("judge did not get result")

    program_info['take_time'] = total_time*1000
    program_info['take_memory'] = max_rss/1024.0
    program_info['result'] = result_code[program_info['result']]
    return program_info


def main():
    logging.basicConfig(level=logging.INFO,
                        format = '%(asctime)s --- %(message)s',)
    judge_one_c(321814,1498,1,1000,65536*1000)
    raise
    start_work_thread()
#    start_get_task()
    put_task_into_queue()

if __name__=='__main__':
    main()
