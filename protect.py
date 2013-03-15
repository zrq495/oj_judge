#!/usr/bin/env python
#coding=utf-8
import os
import re
import sys
import psutil
import shutil
import subprocess
import codecs
import logging
import shlex
import time
import config
import lorun
import threading
import MySQLdb
from db import run_sql
from Queue import Queue
try: 
    #降低程序运行权限，防止恶意代码
    os.setuid(int(os.popen("id -u %s"%"nobody").read())) 
except:
    logging.error("please run this program as root!")
    sys.exit(-1)
#初始化队列
q = Queue(config.queue_size)
#数据库锁，保证一个时间只能一个程序都写数据库
dblock = threading.Lock()

def worker():
    '''工作线程，循环扫描队列，获得评判任务并执行'''
    while True:
        if q.empty() is True: #队列为空，空闲
            logging.info("%s idle"%(threading.current_thread().name))
        task = q.get()  # 获取任务，如果队列为空则阻塞
        solution_id = task['solution_id']
        problem_id = task['problem_id']
        language = task['pro_lang']
        user_id = task['user_id']
        data_count = get_data_count(task['problem_id']) #获取测试数据的个数
        logging.info("judging %s"%solution_id)
        result=run(problem_id,solution_id,language,data_count,user_id) #评判
        logging.info("%s result %s"%(result['solution_id'],result['result']))
        dblock.acquire()
        update_result(result) #将结果写入数据库
        dblock.release()
        if config.auto_clean == True:  #清理work目录
            clean_work_dir(result['solution_id'])
        q.task_done()   #一个任务完成

def clean_work_dir(solution_id):
    '''清理word目录，删除临时文件'''
    dir_name = os.path.join(config.work_dir,str(solution_id))
    shutil.rmtree(dir_name)

def start_work_thread():
    '''开启工作线程'''
    for i in range(config.count_thread):
        t = threading.Thread(target=worker)
        t.deamon = True
        t.start()

def start_get_task():
    '''开启获取任务线程'''
    t = threading.Thread(target=put_task_into_queue, name="get_task")
    t.deamon = True
    t.start()

def get_data_count(problem_id):
    '''获得测试数据的个数信息'''
    full_path = os.path.join(config.data_dir,str(problem_id))
    try:
        files = os.listdir(full_path)
    except OSError,e:
        logging.error(e)
        return 0
    count = 0
    for item in files:
        if item.endswith(".in") and item.startswith("data"):
            count += 1
    return count

def update_solution_status(solution_id,result=12):
    '''实时更新评测信息'''
    update_sql = "update solution set result = %s where solution_id = %s"%(result,solution_id)
    run_sql(update_sql)
    return 0

def update_result(result):
    '''更新评测结果'''
    #更新solution信息
    sql = "update solution set take_time = %s , take_memory = %s, result = %s where solution_id = %s"%(result['take_time'],result['take_memory'],result['result'],result['solution_id'])
    #更新用户解题数和做题数信息
    update_ac_sql = "update user set accept = (select count(distinct problem_id) from solution where result = 1 and user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_sub_sql = "update user set submit = (select count(problem_id) from solution where user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    #更新题目AC数和提交数信息
    update_problem_ac="UPDATE problem SET accept=(SELECT count(*) FROM solution WHERE problem_id=%s AND result=1) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    update_problem_sub="UPDATE problem SET submit=(SELECT count(*) FROM solution WHERE problem_id=%s) WHERE problem_id=%s"%(result['problem_id'],result['problem_id'])
    run_sql([sql,update_ac_sql,update_sub_sql,update_problem_ac,update_problem_sub])
    return 0

def update_compile_info(solution_id,info):
    '''更新数据库编译错误信息'''
    info = MySQLdb.escape_string(info)
    sql = "insert into compile_info(solution_id,compile_info) values (%s,'%s')"%(solution_id,info)
    run_sql(sql)
    return 0

def get_problem_limit(problem_id):
    '''获得题目的时间和内存限制'''
    sql = "select time_limit,memory_limit from problem where problem_id = %s"%problem_id
    data = run_sql(sql)
    return data[0]


def get_code(solution_id,problem_id,pro_lang):
    '''从数据库获取代码并写入work目录下对应的文件'''
    file_name = {
        "gcc":"main.c",
        "g++":"main.cpp",
        "java":"Main.java",
        "pascal":"main.pas",
        "golang":"main.go",
    }
    select_code_sql = "select code_content from code where solution_id = %s"%solution_id
    feh = run_sql(select_code_sql)
    if feh is not None:
        code = feh[0][0]
    else:
        logging.error("cannot get code of runid %s"%solution_id)
        return False
    try:
        work_path = os.path.join(config.work_dir,str(solution_id))
        os.mkdir(work_path)
    except OSError,e:
        if str(e).find("exist")>0: #文件夹已经存在
            pass
        else:
            logging.error(e)
            return False
    try:
        real_path = os.path.join(config.work_dir,str(solution_id),file_name[pro_lang])
    except KeyError,e:
        logging.error(e)
        return False
    try:
        f = codecs.open(real_path,'w')
        if pro_lang in ['gcc','g++','java','golang']:
            code = del_code_note(code) #删除注释,防止因为中文问题无法将代码写入文件
        try:
            f.write(code)
        except:
            logging.error("not write code to file")
            f.close()
            return False
        f.close()
    except OSError,e:
        logging.error(e)
        return False
    return True

def del_code_note(code):
    '''删除代码注释'''
    code = re.sub("//.*",'',code)
    code = re.sub("/\*.*?\*/",'',code,flags=re.M|re.S)
    return code

def put_task_into_queue():
    '''循环扫描数据库,将任务添加到队列'''
    while True:
        q.join() #阻塞程序,直到队列里面的任务全部完成
        sql = "select solution_id,problem_id,user_id,contest_id,pro_lang from solution where result = 0"
        data = run_sql(sql)
        time.sleep(0.2) #延时0.2秒,防止因速度太快不能获取代码
        for i in data:
            solution_id,problem_id,user_id,contest_id,pro_lang = i
            dblock.acquire()
            ret = get_code(solution_id,problem_id,pro_lang)
            dblock.release()
            if ret == False:
                #防止因速度太快不能获取代码
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
    dir_work = os.path.join(config.work_dir,str(solution_id))
    build_cmd = {
        "gcc"    : "gcc main.c -o main -Wall -lm -O2 -std=c99 --static -DONLINE_JUDGE",
        "g++"    : "g++ main.cpp -O2 -Wall -lm --static -DONLINE_JUDGE -o main",
        "java"   : "javac Main.java",
        "pascal" : 'fpc main.pas -O2 -Co -Ct -Ci',
        "golang" : 'go build main.go',
    }
    if language not in build_cmd.keys():
        return False
    p = subprocess.Popen(build_cmd[language],shell=True,cwd=dir_work,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    out,err =  p.communicate()#获取编译错误信息
    err_txt_path = os.path.join(config.work_dir,str(solution_id),'error.txt')
    f = file(err_txt_path,'w')
    f.write(err)
    f.write(out)
    f.close()
    if p.returncode == 0: #返回值为0,编译成功
        return True
    dblock.acquire()
    update_compile_info(solution_id,err+out) #编译失败,更新题目的编译错误信息
    dblock.release()
    return False

def judge_result(problem_id,solution_id,data_num):
    '''对输出数据进行评测'''
    logging.debug("Judging result")
    currect_result = os.path.join(config.data_dir,str(problem_id),'data%s.out'%data_num)
    user_result = os.path.join(config.work_dir,str(solution_id),'out%s.txt'%data_num)
    try:
        curr = file(currect_result).read().replace('\r','').rstrip()#删除\r,删除行末的空格和换行
        user = file(user_result).read().replace('\r','').rstrip()
    except:
        return False
    if curr == user:       #完全相同:AC
        return "Accepted"
    if curr.split() == user.split(): #除去空格,tab,换行相同:PE
        return "Presentation Error"
    if curr in user:  #输出多了
        return "Output limit"
    return "Wrong Answer"  #其他WA

def judge_one_c_mem_time(solution_id,problem_id,data_num,time_limit,mem_limit):
    '''评测一组数据'''
    input_path = os.path.join(config.data_dir,str(problem_id),'data%s.in'%data_num)
    try:
        input_data = file(input_path)
    except:
        return False
    output_path = os.path.join(config.work_dir,str(solution_id),'out%s.txt'%data_num)
    temp_out_data = file(output_path,'w')
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
    '''评测编译类型语言'''
    max_mem = 0
    max_time = 0
    for i in range(data_count):
        ret = judge_one_c_mem_time(solution_id,problem_id,i+1,time_limit+10,mem_limit)
        if ret == False:
            continue
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
        if result == False:
            continue
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
    '''评测java程序'''
    cmd = "/usr/bin/java Main"
    work_dir = os.path.join(config.work_dir,str(solution_id))
    max_rss = 0
    max_vms = 0
    max_time = 0
    for i in range(data_count):
        args = shlex.split(cmd)
        input_path = os.path.join(config.data_dir,str(problem_id),'data%s.in'%(i+1))
        output_path = os.path.join(config.work_dir,str(solution_id),'out%s.txt'%(i+1))
        err_txt_path = os.path.join(config.work_dir,str(solution_id),'run_err%s.txt'%(i+1))
        try:
            input_data = file(input_path)
            output_data = file(output_path,'w')
            run_err_data = file(err_txt_path,'w')
        except:
            continue
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
        if result == False:
            continue
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
    '''检测评测程序是否存在,小于config规定数目则启动新的'''
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
    '''开启守护进程'''
    t = threading.Thread(target=check_thread, name="check_thread")
    t.deamon = True
    t.start()

def main():
    logging.basicConfig(level=logging.INFO,
                        format = '%(asctime)s --- %(message)s',)
    start_get_task()
    start_work_thread()
    start_protect()

if __name__=='__main__':
    main()
