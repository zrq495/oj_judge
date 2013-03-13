#!/usr/bin/env python
#coding=utf-8
import os
import re
import psutil
import shutil
import subprocess
import codecs
import logging
import shlex
import time
import MySQLdb
import config
import lorun
import threading
from Queue import Queue
try:
    os.setuid(int(os.popen("id -u %s"%"nobody").read()))
except:
    logging.error("please run this program as root")
q = Queue(config.queue_size)   #初始化队列
dblock = threading.Lock()
def worker():
    while True:
        if q.empty() is True:
            logging.info("%s idle"%(threading.current_thread().name))
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
        if config.auto_clean == True:
            clean_work_dir(result['solution_id'])
        q.task_done()

def clean_work_dir(solution_id):
    dir_name = os.path.join(config.work_dir,str(solution_id))
    shutil.rmtree(dir_name)

def start_work_thread():
    for i in range(config.count_thread):
        t = threading.Thread(target=worker)
        t.deamon = True
        t.start()

def start_get_task():
    t = threading.Thread(target=put_task_into_queue, name="get_task")
    t.deamon = True
    t.start()

def update_compile_info(solution_id,info):
    con=connect_to_db()
    cur = con.cursor()
    info = MySQLdb.escape_string(info)
    sql = "insert into compile_info(solution_id,compile_info) values (%s,'%s')"%(solution_id,info)
    try:
        cur.execute(sql)
    except MySQLdb.OperationalError,e:
        logging.error(e)
        return False
    con.commit()
    cur.close()
    con.close()

def get_problem_limit(problem_id):
    con=connect_to_db()
    cur = con.cursor()
    sql = "select time_limit,memory_limit from problem where problem_id = %s"%problem_id
    try:
        cur.execute(sql)
    except MySQLdb.OperationalError,e:
        logging.error(e)
        return False
    data = cur.fetchone()
    cur.close()
    con.close()
    return data

def update_solution_status(solution_id,result=12):
    con=connect_to_db()
    cur = con.cursor()
    update_sql = "update solution set result = %s where solution_id = %s"%(result,solution_id)
    try:
        cur.execute(update_sql)
    except MySQLdb.OperationalError,e:
        logging.error(e)
        return False
    con.commit()
    cur.close()
    con.close()
    return 0

def connect_to_db():
    con = None
    while True:
        try:
            con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                                  config.db_name,charset=config.db_charset)
            return con
        except:
            logging.error('Cannot connect to database,trying again')
            time.sleep(1)

def update_result(result):
    con=connect_to_db()
    cur = con.cursor()
    sql = "update solution set take_time = %s , take_memory = %s, result = %s where solution_id = %s"%(result['take_time'],result['take_memory'],result['result'],result['solution_id'])
    update_ac_sql = "update user set accept = (select count(distinct problem_id) from solution where result = 1 and user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_sub_sql = "update user set submit = (select count(problem_id) from solution where user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_problem_ac="UPDATE problem SET accept=(SELECT count(*) FROM solution WHERE problem_id=%s AND result=1) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    update_problem_sub="UPDATE problem SET submit=(SELECT count(*) FROM solution WHERE problem_id=%s) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    try:
        cur.execute(sql)
        cur.execute(update_ac_sql)
        cur.execute(update_sub_sql)
        cur.execute(update_problem_ac)
        cur.execute(update_problem_sub)
    except MySQLdb.OperationalError,e:
        logging.error(e)
        return False
    con.commit()
    cur.close()
    con.close()
    return 0


def get_data_count(problem_id):
    full_path = os.path.join(config.data_dir,str(problem_id))
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
        "golang":"main.go",
    }
    con=connect_to_db()
    cur = con.cursor()
    select_code_sql = "select code_content from code where solution_id = %s"%solution_id
    try:
        cur.execute(select_code_sql)
    except MySQLdb.OperationalError,e:
        logging.error(e)
        return False
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
        work_path = os.path.join(config.work_dir,str(solution_id))
        os.mkdir(work_path)
    except OSError,e:
        if str(e).find("exist")>0:
            pass
        else:
            logging.error("172")
            logging.error(e)
            return False
    try:
#        real_path = "/work/%s/%s"%(solution_id,file_name[pro_lang])
        real_path = os.path.join(config.work_dir,str(solution_id),file_name[pro_lang])
    except KeyError,e:
        logging.error("177")
        logging.error(e)
        return False
    try:
        f = codecs.open(real_path,'w')
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
    while True:
        q.join()
        con=connect_to_db()
        cur = con.cursor()
        sql = "select solution_id,problem_id,user_id,contest_id,pro_lang from solution where result = 0"
        try:
            cur.execute(sql)
        except MySQLdb.OperationalError,e:
            logging.error(e)
            return False
        data = cur.fetchall()
        cur.close()
        con.close()
        time.sleep(0.2)
        for i in data:
            solution_id,problem_id,user_id,contest_id,pro_lang = i
            dblock.acquire()
            ret = get_code(solution_id,problem_id,pro_lang)
            dblock.release()
            if ret == False:
                '''防止因速度太快不能获取代码'''
                time.sleep(0.5)
                dblock.acquire()
                ret = get_code(solution_id,problem_id,pro_lang)
                dblock.release()
            if ret == False:
                dblock.acquire()
                update_solution_status(solution_id,11)
                dblock.release()
                continue
            task = {
                "solution_id":solution_id,
                "problem_id":problem_id,
                "contest_id":contest_id,
                "user_id":user_id,
                "pro_lang":pro_lang,
            }
            q.put(task)
            dblock.acquire()
            update_solution_status(solution_id)
            dblock.release()
        time.sleep(0.5)

def compile(solution_id,language):
    '''将程序编译成可执行文件'''
    language = language.lower()
#    dir_work = "/work/%s/"%solution_id
    dir_work = os.path.join(config.work_dir,str(solution_id))
    if language == "gcc":
        cmd = "gcc main.c -o main -Wall -lm -O2 -std=c99 --static -DONLINE_JUDGE"
    elif language == 'g++':
        cmd = "g++ main.cpp -O2 -Wall -lm --static -DONLINE_JUDGE -o main"
    elif language == "java":
        cmd = "javac Main.java"
    elif language == 'pascal':
        cmd = 'fpc main.pas -O2 -Co -Ct -Ci'
    elif language == 'golang':
        cmd = 'go build main.go'
    else:
        return False
    p = subprocess.Popen(cmd,shell=True,cwd=dir_work,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err =  p.communicate()
    err_txt_path = os.path.join(config.work_dir,str(solution_id),'error.txt')
    f = file(err_txt_path,'w')
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
#    currect_result = "/data/%s/data%s.out"%(problem_id,data_num)
    currect_result = os.path.join(config.data_dir,str(problem_id),'data%s.out'%data_num)
#    user_result = "/work/%s/out%s.txt"%(solution_id,data_num)
    user_result = os.path.join(config.work_dir,str(solution_id),'out%s.txt'%data_num)
    curr = file(currect_result).read().replace('\r','').rstrip()
    user = file(user_result).read().replace('\r','').rstrip()
    if curr == user:
        return "Accepted"
    if curr.split() == user.split():
        return "Presentation Error"
    if curr in user:
        return "Output limit"
    return "Wrong Answer"

def judge_one_c_mem_time(solution_id,problem_id,data_num,time_limit,mem_limit):
    input_path = os.path.join(config.data_dir,str(problem_id),'data%s.in'%data_num)
    input_data = file(input_path)
    output_path = os.path.join(config.work_dir,str(solution_id),'out%s.txt'%data_num)
    temp_out_data = file(output_path,'w')
#    main_exe = '/work/%s/main'%(solution_id)
    main_exe = os.path.join(config.work_dir,str(solution_id),'main')
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
    logging.debug(rst)
    return rst

def judge_c(solution_id,problem_id,data_count,time_limit,mem_limit,program_info,result_code):
    max_mem = 0
    max_time = 0
    for i in range(data_count):
        ret = judge_one_c_mem_time(solution_id,problem_id,i+1,time_limit+10,mem_limit)
        logging.debug(ret)
        if ret['result'] == 2:
            program_info['result'] = result_code["Time Limit Exceeded"]
            program_info['take_time'] = time_limit+10
            return program_info
        elif ret['result'] == 3:
            program_info['result'] =result_code["Memory Limit Exceeded"]
            program_info['take_memory'] = mem_limit
            return program_info
        elif ret['result'] == 5:
            program_info['result'] =result_code["Runtime Error"]
            return program_info
        if max_time < ret["timeused"]:
            max_time = ret['timeused']
        if max_mem < ret['memoryused']:
            max_mem = ret['memoryused']
        result = judge_result(problem_id,solution_id,i+1)
        if result == "Wrong Answer" or result == "Output limit":
            program_info['result'] = result_code[result]
            break
        elif result == 'Presentation Error':
            program_info['result'] = result_code[result]
        elif result == 'Accepted':
            if program_info['result'] != 'Presentation Error':
                program_info['result'] = result_code[result]
        else:
            logging.error("judge did not get result")
    program_info['take_time'] = max_time
    program_info['take_memory'] = max_mem
    return program_info

def judge_java(solution_id,problem_id,data_count,time_limit,mem_limit,program_info,result_code):
    cmd = "/usr/bin/java Main"
#    work_dir = '/work/%s'%solution_id
    work_dir = os.path.join(config.work_dir,str(solution_id))
    max_rss = 0
    max_vms = 0
    max_time = 0
    for i in range(data_count):
        args = shlex.split(cmd)
        input_path = os.path.join(config.data_dir,str(problem_id),'data%s.in'%(i+1))
        output_path = os.path.join(config.work_dir,str(solution_id),'out%s.txt'%(i+1))
        err_txt_path = os.path.join(config.work_dir,str(solution_id),'run_err%s.txt'%(i+1))
        input_data = file(input_path)
        output_data = file(output_path,'w')
        run_err_data = file(err_txt_path,'w')
        p = subprocess.Popen(args,env={"PATH":"/nonexistent"},cwd=work_dir,stdout=output_data,stdin=input_data,stderr=run_err_data)
        start = time.time()
        pid = p.pid
        logging.debug(pid)
        glan = psutil.Process(pid)
        while True:
            time_to_now = time.time()-start 
            if psutil.pid_exists(pid) is False:
                program_info['take_time'] = time_to_now*1000
                program_info['take_memory'] = max_rss/1024.0
                program_info['result'] = result_code["Runtime Error"]
                return program_info
            rss,vms = glan.get_memory_info()
            if p.poll() == 0:
                end = time.time()
                break
            if max_rss < rss:
                max_rss = rss
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
        use_time = end - start
        if max_time < use_time:
            max_time = use_time
        logging.debug("max_rss = %s"%max_rss)
        logging.debug("max_vms = %s"%max_vms)
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
    program_info['take_time'] = max_time*1000
    program_info['take_memory'] = max_rss/1024.0
    program_info['result'] = result_code[program_info['result']]
    return program_info


def run(problem_id,solution_id,language,data_count,user_id):
    '''获取程序执行时间和内存'''
    dblock.acquire()
    time_limit,mem_limit=get_problem_limit(problem_id)
    dblock.release()
    program_info = {
        "solution_id":solution_id,
        "problem_id":problem_id,
        "take_time":0,
        "take_memory":0,
        "user_id":user_id,
        "result":0,
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
    if language == "java":
        time_limit = (time_limit+10)/1000.0
        mem_limit = mem_limit * 1024
        result = judge_java(solution_id,problem_id,data_count,
                            time_limit,mem_limit,program_info,result_code)
    else:
        result = judge_c(solution_id,problem_id,data_count,time_limit,mem_limit,program_info,result_code)
    logging.debug(result)
    return result

def check_thread():
    while True:
        try:
            if threading.active_count() < config.count_thread + 2:
                logging.info("start new thread")
                t = threading.Thread(target=worker)
                t.deamon = True
                t.start()
            time.sleep(1)
        except:
            pass

def start_protect():
    t = threading.Thread(target=check_thread, name="check_thread")
    t.deamon = True
    t.start()


def main():
    logging.basicConfig(level=logging.INFO,
                        format = '%(asctime)s --- %(message)s',)
    start_get_task()
    start_work_thread()
#    put_task_into_queue()
    start_protect()

if __name__=='__main__':
    main()
