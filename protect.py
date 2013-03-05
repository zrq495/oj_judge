#!/usr/bin/env python
#coding=utf-8
import psutil
import subprocess
import logging
import shlex
import time
import os
import sys
import MySQLdb
import config
from Queue import Queue
from threading import Thread
os.setuid(int(os.popen("id -u %s"%"nobody").read()))
q = Queue(config.queue_size)   #初始化队列

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
        result=run(problem_id,solution_id,language,data_count,user_id)
        update_result(result)
        q.task_done()

def start_work_thread():
    for i in range(config.count_thread): #依次启动工作线程
        t = Thread(target=worker, name="thread%d"%i)
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
        logging.error('cannot connect to database')
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
        logging.error('cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    sql = "select time_limit,memory_limit from problem where problem_id = %s"%problem_id
    cur.execute(sql)
    data = cur.fetchone()
    cur.close()
    con.close()
    return data

def update_solution_status(solution_id):
    logging.debug("update solution status")
    con = None
    try:
        con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                              config.db_name,charset=config.db_charset)
    except:
        logging.error('cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    update_sql = "update solution set result = 12 where solution_id = %s"%solution_id
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
        logging.error('cannot connect to database')
        sys.exit(-1)
    cur = con.cursor()
    sql = "update solution set take_time = %s , take_memory = %s, result = %s where solution_id = %s"%(result['take_time'],result['take_memory'],result['result'],result['solution_id'])
    cur.execute(sql)
    update_ac_sql = "update user set accept = (select count(distinct problem_id) from solution where result = 1 and user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_sub_sql = "update user set submit = (select count(problem_id) from solution where user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_problem_ac="UPDATE problem SET accept=(SELECT count(*) FROM solution WHERE problem_id=%s AND result=1) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    update_problem_sub="UPDATE problem SET accept=(SELECT count(*) FROM solution WHERE problem_id=%s) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
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
        logging.error(e)
        return 0
    count = 0
    for item in files:
        if item.endswith(".in"):
            count += 1
    return count


def put_task_into_queue():
    file_name = {
        "gcc":"main.c",
        "g++":"main.cpp",
        "java":"Main.java",
        "pascal":"main.pas",
    }
    
    while True:
        con = None
        try:
            con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                                  config.db_name,charset=config.db_charset)
        except:
            logging.error('cannot connect to database')
            sys.exit(-1)
        cur = con.cursor()
        sql = "select solution_id,problem_id,user_id,contest_id,pro_lang from solution where result = 0"
        judged = []
#        logging.debug('getting solution')
        cur.execute(sql)
        data = cur.fetchall()
        for i in data:
            solution_id,problem_id,user_id,contest_id,pro_lang = i
            select_code_sql = "select code_content from code where solution_id = %s"%solution_id
            cur.execute(select_code_sql)
            try:
                code = cur.fetchone()[0]
            except TypeError,e:
                logging.error(e)
                continue
            try:
                os.mkdir('/work/%s/'%solution_id)
            except OSError,e:
                if str(e).find("exist")>0:
                    pass
                else:
                    logging.error(e)
            try:
                real_path = "/work/%s/%s"%(solution_id,file_name[pro_lang])
            except KeyError,e:
                logging.error(e)
                continue
            try:
                f = file(real_path,'w')
                f.write(code)
                f.close()
            except OSError,e:
                logging.error(e)
                continue
            task = {
                "solution_id":solution_id,
                "problem_id":problem_id,
                "contest_id":contest_id,
                "user_id":user_id,
                "pro_lang":pro_lang,
            }
            if solution_id in judged:
                continue
            else:
                judged.append(solution_id)
                q.put(task)
                update_solution_status(solution_id)
        time.sleep(0.1)
        cur.close()
        con.close()


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
    update_compile_info(solution_id,err+out)
    return False

def judge_result(problem_id,solution_id,data_num):
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


def run(problem_id,solution_id,language,data_count,user_id):
    '''获取程序执行时间和内存'''
    time_limit,mem_limit=get_problem_limit(problem_id)
    time_limit = time_limit/1000
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
        cmd = "java Main"
    else:
        cmd = "./main"
    work_dir = '/work/%s'%solution_id
    max_rss = 0
    max_vms = 0
    total_time = 0
    for i in range(data_count):
        args = shlex.split(cmd)
        input_data = file('/data/%s/data%s.in'%(problem_id,i+1))
        output_data = file('/work/%s/out%s.txt'%(solution_id,i+1),'w')
        p = subprocess.Popen(args,cwd=work_dir,stdout=output_data,stdin=input_data)
        start = time.time()
        pid = p.pid
        logging.debug(pid)
        glan = psutil.Process(pid)
        while True:
            time_to_now = time.time()-start + total_time
            if psutil.pid_exists(pid) is False:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024
                program_info['result'] = result_code["Runtime Error"]
                return program_info
            rss,vms = glan.get_memory_info()
            if p.poll() == 0:
                end = time.time()
                break
#            logging.debug((rss,vms))
            if max_rss < rss:
                max_rss = rss
            if max_vms < vms:
                max_vms = vms
            if time_to_now > time_limit:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024
                program_info['result'] = result_code["Time Limit Exceeded"]
                glan.terminate()
                return program_info
            if max_rss > mem_limit:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024
                program_info['result'] =result_code["Memory Limit Exceeded"]
                glan.terminate()
                return program_info

        total_time += end - start
        logging.debug("max_rss = %s"%max_rss)
        logging.debug("max_vms = %s"%max_vms)
    #    logging.debug("take time = %s"%(end - start))
        result = judge_result(problem_id,solution_id,i+1)
        if result == "Wrong Answer":
            program_info['result'] = result
            break
        elif result == 'Presentation Error':
            program_info['result']=result
        elif result == 'Accepted':
            if program_info['result'] != 'Presentation Error':
                program_info['result'] = result

    program_info['take_time'] = total_time*1000
    program_info['take_memory'] = max_rss/1024
    program_info['result'] = result_code[program_info['result']]
    return program_info

def main():
    logging.basicConfig(level=logging.DEBUG,
                        format = '%(asctime)s --- %(message)s',)
#    start_get_task()
    start_work_thread()
    put_task_into_queue()

if __name__=='__main__':
    main()
