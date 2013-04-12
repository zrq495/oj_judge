#!/usr/bin/env python
#coding=utf-8

import time
import config
import MySQLdb
import logging
import types

def run_sql(sql):
    '''执行sql语句,并返回结果'''
    con = None
    while True:
        try:
            con = MySQLdb.connect(config.db_host,config.db_user,config.db_password,
                                  config.db_name,charset=config.db_charset)
            break
        except: 
            logging.error('Cannot connect to database,trying again')
            time.sleep(1)
    cur = con.cursor()
    try:
        if type(sql) == types.StringType:
            cur.execute(sql)
        elif type(sql) == types.ListType:
            for i in sql:
                cur.execute(i)
    except MySQLdb.OperationalError,e:
        logging.error(e)
        cur.close()
        con.close()
        return False
    con.commit()
    data = cur.fetchall()
    cur.close()
    con.close()
    return data



def update_solution_status(solution_id,result=12):
    '''实时更新评测信息'''
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
    '''连接数据库,连接失败自动重新连接,防止因数据库重启或中断导致评测程序中断'''
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
    '''更新评测结果'''
    con=connect_to_db()
    cur = con.cursor()
    #更新solution信息
    sql = "update solution set take_time = %s , take_memory = %s, result = %s where solution_id = %s"%(result['take_time'],result['take_memory'],result['result'],result['solution_id'])
    #更新用户解题数和做题数信息
    update_ac_sql = "update user set accept = (select count(distinct problem_id) from solution where result = 1 and user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    update_sub_sql = "update user set submit = (select count(problem_id) from solution where user_id = %s) where user_id = %s;"%(result['user_id'],result['user_id'])
    #更新题目AC数和提交数信息
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


def update_compile_info(solution_id,info):
    '''更新数据库编译错误信息'''
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
    '''获得题目的时间和内存限制'''
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
