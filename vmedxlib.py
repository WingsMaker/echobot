#------------------------------------------------------------------------------------------------------#
# / _ \ _ __ ___  _ __ (_)  \/  | ___ _ __ | |_ ___  _ __
#| | | | '_ ` _ \| '_ \| | |\/| |/ _ \ '_ \| __/ _ \| '__|
#| |_| | | | | | | | | | | |  | |  __/ | | | || (_) | |
# \___/|_| |_| |_|_| |_|_|_|  |_|\___|_| |_|\__\___/|_|
#
# Library functions by KH
# This is for EDX interface
#------------------------------------------------------------------------------------------------------
import pandas as pd
import pymysql
import pymysql.cursors
import json
import datetime
import re, sys
import requests
import vmsvclib
from vmsvclib import *

global edx_api_header, edx_api_url

max_iu_cnt = 20
piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
str2date = lambda x : string2date(x,"%d/%m/%Y")
dmy_str = lambda v : piece(v,'-',2) + '/' + piece(v,'-',1) + '/' + piece(v,'-',0)
ymd_str = lambda v : piece(v,'-',0) + '/' + piece(v,'-',1) + '/' + piece(v,'-',2)

def edx_coursename(course_id):
    # Status : Tested
    global edx_api_header, edx_api_url
    coursename = ""
    url = f"{edx_api_url}/course/fetch/name"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0:
            coursename = str(data)
    return coursename

def edx_day0(course_id):
    # Status : Tested
    global edx_api_header, edx_api_url
    start_date = ""
    url = f"{edx_api_url}/course/fetch/startdate"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0:
            start_date = datetime.datetime.strptime(str(data)[:10],"%Y-%m-%d").date()
    return start_date

def edx_courseid(cohort_id):
    # Status : Tested
    global edx_api_header, edx_api_url
    course_id = ""
    url = f"{edx_api_url}/course/fetch/id"
    response = requests.post(url, data=cohort_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0:
            course_id = str(data)
    return course_id

def edx_iu_counts(course_id, client_name):
    qry = "SELECT count(m.iu) AS cnt  FROM module_iu m INNER JOIN playbooks p "
    qry += f"ON m.module_code=p.module_code AND m.client_name=p.client_name "
    qry += f"WHERE p.client_name = '{client_name}' AND p.course_id = '{course_id}';"
    cnt = rds_param(qry)
    return cnt

def edx_mcqcnt(course_id):
    # Status : Tested
    global edx_api_header, edx_api_url
    mcqcnt = 0
    url = f"{edx_api_url}/course/fetch/mcq/count"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0 and data.isnumeric():
            mcqcnt = int(str(data))
    return mcqcnt

def edx_ascnt(course_id):
    # Status : Tested
    global edx_api_header, edx_api_url
    ascnt = 0
    url = f"{edx_api_url}/course/fetch/assignment/count"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0 and data.isnumeric():
            ascnt = int(str(data))
    return ascnt

def edx_userdata(course_id):
    # Status : Tested
    # url = f"{edx_api_url}/course/fetch/users"
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'student_id':[],'course_id':[],'username':[],'email':[]})
    url = f"{edx_api_url}/course/fetch/users"
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    syslog(f"completed with status code {response.status_code}")
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data) > 0:
            userinfo = json.loads(data)
            rows_cnt = len(list(userinfo))
            user_list = []
            course_list = []
            username_list = []
            email_list = []
            if rows_cnt > 0:
                for usr in userinfo:
                    if course_id in [x['course_id'] for x in usr['enrolments']]:
                        user_list.append(usr['user_id'])
                        course_list.append(course_id)
                        username_list.append(usr['username'])
                        email_list.append(usr['email'])
            data = {'student_id':user_list,'course_id':course_list,'username':username_list,'email':email_list}
            df = pd.DataFrame.from_dict(data)
    return df

def student_course_list(student_id):
    # Status : Tested
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'course_id':[]})
    url = f"{edx_api_url}/user/fetch/{student_id}"
    response = requests.get(url, headers=edx_api_header)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0:
            userinfo = json.loads(response.content.decode('utf-8'))
            course_list = [x['course_id'] for x in userinfo['enrolments']]
            df = pd.DataFrame.from_dict({'course_id':course_list})
    return df

def edx_grade(course_id, student_id=0):
    # Status : Tested
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'student_id':[], 'grade':[]})
    if student_id==0:
        url = f"{edx_api_url}/user/fetch/grades/list"
    else:
        url = f"{edx_api_url}/user/fetch/grades/list/{student_id}"
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    syslog(f"completed with status code {response.status_code}")
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)==0:
            data = []
        else:
            data = eval(data)
        stud = [x['student_id'] for x in data]
        grade = [x['grade'] for x in data]
        data = {'student_id' : stud , 'grade' : grade }
        df = pd.DataFrame.from_dict(data)
    return df

def edx_mcqinfo(client_name, course_id, student_id=0):
    # Status : Tested
    global edx_api_header, edx_api_url

    def getnumstr(qn_str):
        if ' ' in qn_str:
            qn_str = qn_str.split(' ')[0]
        qn_numstr  = ''.join([x for x in qn_str if x in '0123456789'])
        qn_numstr = '0' if qn_numstr=='' else qn_numstr
        qn = int(qn_numstr)
        if (qn > max_iu_cnt) or (qn<1):
            qn = 1
        return qn

    df_mcq = pd.DataFrame.from_dict( {'client_name':[],'course_id':[],'student_id':[], 'score':[], \
        'mcq':[],'qn':[],'attempts':[], 'avgscore': []} )
    if student_id==0:
        url = f"{edx_api_url}/user/fetch/mcq/scores/list"
    else:
        url = f"{edx_api_url}/user/fetch/mcq/scores/list/{student_id}" 
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False) # it takes 3 secs
    syslog(f"completed with status code {response.status_code}")
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if str(data) == "":
            data = []
        else:
            data = json.loads(data)
        course_id_list = []
        student_id_list = []
        iu_list = []
        qn_list = []
        grade_list = []
        att_list = []
        avgscore_list = []
        for rec in [ x for x in list(data) if 'attempts' in x['state']]  :
            if 'student_id' in list(rec):
                sid = int(rec['student_id'])
            else:
                sid = 0
            if 'score' in list(rec):
                sc = rec['score']
            else:
                sc = 0
            if 'points_possible' in list(rec):
                pp = rec['points_possible']
            else:
                pp = 0
            if 'options_display_name' in list(rec):
                qn = getnumstr(rec['options_display_name'])
            if 'option_display_name' in list(rec):
                qn = getnumstr(rec['option_display_name'])
            if 'IU' in list(rec):
                iu = int(rec['IU'])
            else:
                iu = 0
            state = eval(rec['state'].replace('null','""').replace('false','0').replace('true','1'))
            #attempts = state['attempts']            
            statekey = list(state['correct_map'])[0]
            correctness = state['correct_map'][statekey]['correctness']
            qnscore = 1.0 if correctness=='correct' else 0
            grade = 1.0 if pp==0 else float(sc/pp)
            #attempts = 0 if qnscore == 0 else ( state['attempts'] if 'attempts' in list(rec) else 0)
            attempts = 0 if qnscore == 0 else ( state['attempts'] if 'attempts' in list(state) else 0)
            course_id_list.append(rec['course_id'])
            student_id_list.append(rec['student_id'])
            iu_list.append(iu)
            qn_list.append(qn)
            grade_list.append(qnscore)
            att_list.append(int(attempts))
            # actual average score
            avgscore_list.append(grade)
        client_list = [client_name for x in iu_list]
        data = {'client_name':client_list, 'course_id': course_id_list, 'student_id':student_id_list, \
            'score':grade_list ,'mcq':iu_list , 'qn':qn_list , 'attempts': att_list,  'avgscore': avgscore_list}
        df_mcq = pd.DataFrame.from_dict(data)
    return df_mcq

def edx_assignment_score(course_id, student_id=0):
    # Status : Tested
    global edx_api_header, edx_api_url
    if student_id==0:
        url = f"{edx_api_url}/user/fetch/assignment/scores/list"
    else:
        url = f"{edx_api_url}/user/fetch/assignment/scores/list/{student_id}"
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False) # it tooks 3 seconds
    syslog(f"completed with status code {response.status_code}")
    df = pd.DataFrame.from_dict({'student_id':[], 'score':[], 'points_possible':[], 'IU': [], 'attempts': []})
    if response.status_code==200:
        data = response.content.decode('utf-8')
        rec = eval(data) if len(data)>0 else []
        if rec != []:
            student_id_list = [x['student_id'] for x in rec]
            grade_list = [x['score'] for x in rec]
            pp_list = [x['points_possible'] for x in rec]
            iu_list = [x['IU'] for x in rec]
            #att_list = [x['attempts'] for x in rec]
            att_list = [ 0 if x['score']==0 else x['attempts'] for x in rec]
            data = {'student_id': student_id_list, 'score': grade_list, 'points_possible':pp_list, 'IU': iu_list ,'attempts': att_list}
            df = pd.DataFrame.from_dict(data)
            df['is_num'] = df.apply(lambda x: 1 if x['IU'].isnumeric() else 0, axis=1)
            df.drop(df[ df.is_num == 0 ].index, inplace=True)
            df.drop(columns=['is_num'], inplace=True)
    return df

def sms_attendance(course_id, student_id):
    # Status : okay
    global edx_api_header, edx_api_url
    if student_id==0:
        return []
    url = f"{edx_api_url}/user/attendance/fetch/{student_id}"
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    syslog(f"completed with status code {response.status_code}")
    date_list = []
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data)>0:
            result  = eval(str(data))            
            #date_list = [string2date(x['timetable_date'].split(' ')[0],"%m/%d/%Y") for x in result ]
            #for x in result:                printdict(x)
            #for x in result:
            #    print(x['timetable_date'],x['attendance_type_desc'],x['class_type_code'])
            try:
                date_list = [string2date(x['timetable_date'].split(' ')[0],"%m/%d/%Y") \
                     for x in result if x['attendance_type_desc']=='Attend']
            except:
                date_list = [string2date(x['timetable_date'].split(' ')[0],"%d/%m/%Y") \
                     for x in result if x['attendance_type_desc']=='Attend']
    else:
        syslog(f"responsed with no data! code = {response.status_code}")
    return date_list

def sms_missingdates(client_name, course_id, student_id, cols, date_list):
    f2flag = lambda x,y : ('' if y in date_list else x) if x[:2] in ['FC', 'PM'] else ''
    f2f_pass = lambda x : '' if x=='' else ('' if bypass_map[x]==1 else x)
    if (student_id==0) or (client_name==""):
        return []
    #date_list = sms_attendance(course_id, student_id) if dtlist==[] else dtlist;
    missed_fsf = []
    df = rds_df(f"select * from stages where courseid = '{course_id}' and client_name = '{client_name}';")
    if df is not None:
        df.columns = cols
        stgid_list = [x for x in df.id]
        fsf_dates = [string2date(x,"%d/%m/%Y") for x in df.startdate]
        stage_list = [x for x in df.stage]
        f2f_list = [x for x in df.f2f]
        bypass_map = dict(zip(stage_list,f2f_list))
        arr_stgf2f = dict(zip(stage_list, fsf_dates))
        missing_fsf = [f2flag(stage_list[n],fsf_dates[n]) for n in range(len(stgid_list))]
        missed_fsf = [ f2f_pass(x) for x in missing_fsf ]
    return missed_fsf

def sms_att_rate(client_name, course_id, student_id, date_list):
    if (student_id==0) or (client_name==""):
        return []
    # should use startdate
    if ('.db' in vmsvclib.rds_connstr):
        #query = f"select count(*) as cnt from stages where client_name = '{client_name}' and courseid='{course_id}' "
        query = f"select startdate from stages where client_name = '{client_name}' and courseid='{course_id}' "
        query += " and strftime(substr(startdate,7,4)||'-'||substr(startdate,4,2)||'-'||substr(startdate,1,2)) "
        query += "<= strftime(date('now')) and (stage like 'FC%' OR stage like 'PM%');"
    else:
        #query = f"select count(*) as cnt from stages where client_name = '{client_name}' and courseid='{course_id}' "
        query = f"select startdate from stages where client_name = '{client_name}' and courseid='{course_id}' "
        query += "and (stage like 'FC%' OR stage like 'PM%') and STR_TO_DATE(startdate,'%d/%m/%Y') <= CURDATE();"
    cnt2 = 0
    df = rds_df(query)
    if df is None:
        return []
    else:
        df.columns = ['startdate']
    dt_list = [string2date(x,"%d/%m/%Y") for x in df.startdate]    
    cnt2 = len(dt_list)
    cnt1 = len([x for x in dt_list if x in date_list])
    #cnt2 = rds_param(query)
    #cnt1 = len(date_list)
    att_rate = 0 if cnt2 == 0 else (cnt1/cnt2)
    dt2 = [x for x in dt_list if x not in date_list]
    return att_rate

def sms_pmstage(client_name, course_id):
    if (course_id==0) or (client_name==""):
        return []
    if ('.db' in vmsvclib.rds_connstr):
        query = f"select count(*) as cnt from stages where client_name = '{client_name}' and courseid='{course_id}' "
        query += " and strftime(substr(startdate,7,4)||'-'||substr(startdate,4,2)||'-'||substr(startdate,1,2)) "
        query += "<= strftime(date('now')) and stage like 'PM%';"
    else:
        query = f"select count(*) as cnt from stages where client_name = '{client_name}' and courseid='{course_id}' "
        query += "and stage like 'FC%' and STR_TO_DATE(startdate,'%d/%m/%Y') <= CURDATE();"
    pmcounts = rds_param(query)
    pmstage = 0 if pmcounts == 0 else 1
    return pmstage

def sms_lecturer(client_name, course_id, student_id):
    global edx_api_header, edx_api_url
    if student_id==0:
        return []
    url = f"{edx_api_url}/user/attendance/fetch/{student_id}"
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    syslog(f"completed with status code {response.status_code}")
    lecturer = ""
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data) > 0:
            result  = eval(str(data))
            if len(result) > 0:
                lecturer = result[0]['account_name']
    return lecturer

def search_course_list(keyword):
    # Status : Tested
    global edx_api_header, edx_api_url
    course_list = []
    url = f"{edx_api_url}/course/search/course/list"
    syslog(f"calling api {url} via requests.post")
    response = requests.post(url, data=keyword, headers=edx_api_header, verify=False)
    syslog(f"completed with status code {response.status_code}")
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data) > 0:
            course_list = eval(data)
    return course_list

def edx_course_started(client_name, course_id):
    query = "SELECT count(*) as cnt FROM stages WHERE id=1 and client_name ="
    query += f"'{client_name}' AND courseid='{course_id}' AND "
    if ('.db' in vmsvclib.rds_connstr):
        query += f"(strftime(substr(stagedate,7,4)||'-'||substr(stagedate,4,2)||'-'||substr(stagedate,1,2)) <= strftime(date('now')));"
    else:
        query += "STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE();"
    try:
        cnt = rds_param(query)
        cnt = int(cnt)
    except:
        cnt = 0
    return cnt

def edx_endofcourse(client_name, course_id):
    sub_str  = "SUBSTRING" if ':' in vmsvclib.rds_connstr else "SUBSTR"
    query = f"SELECT (case {sub_str}(`name`,-3) when 'EOC' then 1 else 0 END) eoc FROM stages WHERE client_name ="
    query += f"'{client_name}' AND courseid='{course_id}' AND "
    if ('.db' in vmsvclib.rds_connstr):
        query += f"(strftime(substr(stagedate,7,4)||'-'||substr(stagedate,4,2)||'-'||substr(stagedate,1,2)) "
        query += f"<= strftime(date('now'))) ORDER BY id DESC LIMIT 1;"
    else:
        query += "STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    try:
        result = rds_param(query)
        eoc = 1 if result=='' else int(result)
    except:
        #eoc = 1
        eoc = 0
    return eoc

def edx_eocgap(client_name, course_id, gap=7):
    query = "select COUNT(*) as cnt from stages  where stage = 'EOC' and "
    if ('.db' in vmsvclib.rds_connstr):
        query += "strftime(substr(stagedate,7,4)||'-'||substr(stagedate,4,2)||'-'||(substr(stagedate,1,2))) "
        query += f" between date('now','-{gap} days') and date('now') "
    else:
        query += f"STR_TO_DATE(stagedate,'%d/%m/%Y') BETWEEN (CURDATE() - INTERVAL {gap} DAY) AND CURDATE() "
    query += f" and client_name ='{client_name}' and courseid = '{course_id}';"
    try:
        results = rds_param(query)
        eocgap=int(results)
    except:
        eocgap = 0
    return eocgap

def edx_eocend(client_name, course_id, gap=7):
    query = "select COUNT(*) as cnt from stages  where stage = 'EOC' and "
    if ('.db' in vmsvclib.rds_connstr):
        query += f"date(strftime(substr(stagedate,7,4)||'-'||substr(stagedate,4,2)||'-'||(substr(stagedate,1,2))),'+{gap} days') = date('now') "
    else:
        query += f"CURDATE() - STR_TO_DATE(stagedate,'%d/%m/%Y') = {gap} "
    query += f" and client_name ='{client_name}' and courseid = '{course_id}';"
    try:
        results = rds_param(query)
        eoc7=int(results)
    except:
        eoc7 = 0
    return eoc7

def update_schedule(course_id, client_name):
    # Status : Tested
    dstr = lambda x : piece(piece(x.strip(),':',1),'+',2)
    dtype = lambda x : ('%d%b%Y' if len(dstr(x))==9 else '%d%B%Y') if dstr(x)[0].isdigit() else '%B%Y'
    cohort_date = lambda x : string2date(dstr(x),dtype(x))
    qry = f"select cohort_id from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
    cohort_id = rds_param(qry)
    if cohort_id=="":
        cohort_id = piece(piece(course_id,':',1),'+',1)    
    qry = f"select module_code from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"    
    module_code = rds_param(qry)
    if module_code=="":
        module_code = piece(cohort_id,'-',0)
    stage_list = get_google_calendar(course_id, client_name)
    if stage_list == []:
        syslog("google calendar return nothing")
    try:
        stage_list = update_stage_table(stage_list, course_id, client_name, cohort_id, module_code)
        if stage_list == []:
            syslog(f"System calendar not match for {course_id}")
            return
        dt = string2date(stage_list[0][3],"%d/%m/%Y")
        stg=[x for x in stage_list if x[0]=='EOC']
        eoc_date = string2date(stg[0][4],"%d/%m/%Y")
        date_today = datetime.datetime.now().date()
        days = (date_today - eoc_date).days
    except:
        syslog(f"update_stage_table failed for {course_id}")
        return
    condqry = "client_name = '_c_' and courseid = '_x_';"
    condqry = condqry.replace('_c_', client_name)
    condqry = condqry.replace('_x_', course_id)
    qry = "select * from stages where " + condqry
    df = rds_df(qry)
    if df is None:
        stage_list = []
        days_list = []
        as_str = []
        mcq_str = []
    else:
        df.columns = get_columns("stages")
        stage_list = [x for x in df.name]
        days_list = [x for x in df.days]
        as_str = [x for x in df.assignment]
        mcq_str = [x for x in df.mcq]
    for n in range(len( days_list )):
        stg = stage_list[n]
        dd = days_list[n]
        stg_date = dt + datetime.timedelta( days = (dd-1) )
        stagedate = stg_date.strftime('%d/%m/%Y')
        qry = "update stages set days = _y_ where name = '_x_' and " + condqry
        qry = qry.replace('_x_', stg)
        query = qry.replace('_y_', str(dd))
        rds_update(query)
    #cohort_id = piece(piece(course_id,':',1),'+',1)
    #module_code = piece(cohort_id,'-',0)
    query = "update stages a SET IU =  IFNULL((SELECT b.IU FROM stages_master b WHERE b.client_name=a.client_name "
    query += f"AND b.module_code='{module_code}' AND b.stage=a.stage LIMIT 1) ,'0') "
    query += f"WHERE a.courseid = '{course_id}' AND a.client_name = '{client_name}';"
    try:
        rds_update(query)
    except:
        pass
    if ('.db' in vmsvclib.rds_connstr):
        query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND "
        query += f"(strftime(substr(startdate,7,4)||'-'||substr(startdate,4,2)||'-'||substr(startdate,1,2)) "
        query += f"<= strftime(date('now'))) ORDER BY id DESC LIMIT 1;"
    else:
        query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND "
        query += "STR_TO_DATE(startdate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    current_stage = rds_param(query)
    query = f"update userdata set stage = '{current_stage}' where " + condqry
    rds_update(query)
    query = f"update stages set stagedate = '' where (stagedate is null) and "
    query += condqry
    rds_update(query)
    query = f"update stages set startdate = '' where (startdate is null) and "
    query += condqry
    rds_update(query)
    query = "UPDATE userdata SET mcq_zero = '[]',mcq_failed = '[]',as_zero = '[]',as_failed = '[]' "
    query += " WHERE mcq_zero IS NULL AND client_name='{client_name}' and courseid='{course_id}';"
    rds_update(query)
    syslog(f"completed on {course_id}")
    return

def update_assignment(course_id, client_name, student_id=0):
    # Status : Tested
    condqry = f"client_name = '{client_name}' and courseid = '{course_id}';"
    df = edx_grade(course_id, student_id)
    if df is None:
        return False
    if len(df) == 0:
        return False
    stud_grade_dict = dict(zip([x for x in df.student_id],[x for x in df.grade]))
    df = edx_assignment_score(course_id, student_id)
    if len(df) == 0:
        sid_list = []
        score_list = []
        iu_list = []
        attempts_list = []
        pp_list = []
    else:
        if 'attempts' not in list(df.columns):
            df['attempts'] = 1
        # Create lists of student_id, scores and IUs in df
        sid_list = [x for x in df.student_id]
        score_list = [x for x in df.score]
        iu_list = [x for x in df.IU]
        attempts_list = [x for x in df.attempts]
        pp_list = [x for x in df.points_possible]
    # Creates a dictionary of student_id as key, sub-dict of IU:Score as pair {student_id : {IU:score}}
    stud_list = []
    iu_score = dict()
    iu_attempts = dict()
    for n in range(len(df)):
        sid = sid_list[n]
        iu_num = int(iu_list[n])
        pp_num = pp_list[n]
        score = score_list[n]
        attempts = attempts_list[n]
        if sid not in stud_list:
            stud_list.append(sid)
            iu_score[sid] = dict()
            iu_attempts[sid] = dict()
        #iu_score[sid][iu_num] = int(score)/100
        iu_score[sid][iu_num] = 1 if pp_num == 0 else (int(score)/pp_num)
        attempts = 0 if score==0 else attempts
        iu_attempts[sid][iu_num] = attempts
    # Creates a list of student_ids
    for sid in list(stud_grade_dict):
        if sid not in stud_list:
            iu_score[sid] = dict()
            iu_attempts[sid] = dict()
    stud_list = list(iu_score)
    # If student_id in stud_list is not found in stud_grade_dict, change the corresponding grade for missing student to 0
    for sid in stud_list:
        if sid not in list(stud_grade_dict):
            stud_grade_dict[sid] = 0
    # Isnt this same as the 3 lines above?
    # If student_id in stud list is not in stud_grade_dict, set student's grade to 0
    for n in range(len(stud_list)):
        sid = stud_list[n]
        if sid not in list(stud_grade_dict):
            stud_grade_dict[sid] = 0
        updsqlavg = ''.join([ ", as_avg" + str(x) + " = " + str(iu_score[sid][x]) for x in list(iu_score[sid]) ])
        updsqlatt = ''.join([ ", as_attempts" + str(x) + " = " + str(iu_attempts[sid][x]) for x in list(iu_score[sid]) ])
        updsql = "update userdata set grade = " + str(stud_grade_dict[sid]) + updsqlavg + updsqlatt
        updsql += " where studentid = " + str(sid) + " and courseid = '" + course_id + "';"
        try:
            rds_update(updsql)
        except:
            syslog(f"SQL update error\n{updsql}")
    syslog("completed for course_id " + course_id)
    return True

def update_mcq(course_id, client_name, student_id=0):
    # Status : Tested
    cohort_id = piece(piece(course_id,':',1),'+',1)
    if student_id==0:
        condqry = f"client_name = '{client_name}' and courseid = '{course_id}'"
        cond_qry = f"client_name = '{client_name}' and course_id = '{course_id}'"
    else:
        condqry = f"client_name = '{client_name}' and courseid = '{course_id}' and studentid  = {student_id}"
        cond_qry = f"client_name = '{client_name}' and course_id = '{course_id}' and student_id  = {student_id}"
    syslog("edx_mcqcnt : " + course_id)
    mcqcnt = edx_mcqcnt(course_id)
    if mcqcnt<=0:
        syslog(f"edx_mcqcnt returns {mcqcnt}")
        mcqcnt = edx_iu_counts(course_id, client_name)
        if mcqcnt==0:
            return 
    syslog(f"edx_mcqinfo : {mcqcnt} IUs found on {course_id}")
    mcq_df = edx_mcqinfo(client_name, course_id, student_id)
    if len(mcq_df) > 0:
        query = "delete from mcq_data where " + cond_qry
        rds_update(query) 
    else:
        pass
    df = mcq_df[['client_name', 'course_id', 'student_id', 'score', 'mcq', 'qn', 'attempts']]
    syslog(f"copydbtbl on mcq_data")
    copydbtbl(df, "mcq_data")

    # save into local database with tablename mcq_score with score/max_attempts per mcq tests/students/cohorts
    #query = "select client_name, course_id, student_id, mcq, count(*) as max, sum(score) as mcqscore, \
    #    max(attempts) as max_attempts from mcq_data where " + cond_qry \
    #    + " group by client_name, course_id, student_id, mcq order by client_name, course_id, student_id, mcq"
    #query = "select client_name,course_id,student_id,mcq,avg(score) AS mcqscore,max(attempts) as max_attempts from mcq_data where " 
    query = "select client_name,course_id,student_id,mcq,count(*) as max,sum(score) as score,max(attempts) as max_attempts from mcq_data where " 
    query += cond_qry 
    query += " group by client_name,course_id,student_id,mcq"
    query += " order by client_name,course_id,student_id,mcq;"
    scoredf = rds_df(query)
    cnt = 0
    if scoredf is None :
        syslog(f"there is no data for table mcq_data")
    else:
        cnt = len(scoredf)
        if cnt > 0:
            query = "delete from mcq_score where " + condqry
            rds_update(query)
            scoredf.columns = ['client_name', 'courseid', 'studentid', 'mcq', 'max','score','max_attempts']
            syslog("copydbtbl : mcq_score")
            copydbtbl(scoredf, "mcq_score")
    if cnt == 0:
        max_mcq = 0
    else:
        query = "select max(mcq) AS maxmcq from mcq_score where " + condqry
        df = rds_df(query)
        max_mcq = 0
        if df is not None:
            df.columns = ['maxmcq']
            max_mcq = df['maxmcq'][0]
        if max_mcq > max_iu_cnt:
            max_mcq = 0
 
    if max_mcq==0:
        return 
    
    # reset mcq attempts
    syslog("reset mcq attempts")
    updqry = "update userdata set " 
    updqry += ','.join([ "mcq_attempts" + str(x) + " = 0"  for x in range( 1, max_mcq + 1 )]) + " where "
    updqry += condqry   
    rds_update(updqry)

    # update mcq attempts
    syslog("update mcq attempts")
    query = "SELECT distinct student_id as sid FROM mcq_data WHERE " + cond_qry + " order by student_id;"
    df = rds_df(query)
    if df is None:
        sid_list = []
    else:
        df.columns = ['sid']
        sid_list = [x for x in df.sid]        
        
    for sid in sid_list:
        query = f"SELECT mcq , max(attempts) as att FROM mcq_data WHERE client_name = '{client_name}' AND course_id = '{course_id}' AND student_id={sid} GROUP BY mcq"
        df = rds_df(query)
        if df is None:
            att_dict = {}
        else:
            df.columns = ['mcq', 'att']
            mcq_list = [x for x in df.mcq]
            att_list = [x for x in df.att]
            att_dict = dict(zip(mcq_list,att_list))
            expr = ','.join( [ f"mcq_attempts{x} = {att_dict[x]}"  for x in mcq_list] )            
            updqry = "update userdata set " + expr + f" WHERE client_name = '{client_name}' AND courseid = '{course_id}' AND studentid={sid};"
            rds_update(updqry)        
    
    # reset mcq scores 
    syslog("reset mcq scores")
    updqry = "update userdata set " + ','.join([ "mcq_avg" + str(x) + " = 0"  for x in range( 1, max_mcq + 1 )]) + " where "
    updqry += condqry        
    rds_update(updqry)
    
    # update on  mcq scores 
    userlist = list(set([x for x in mcq_df.student_id]))
    for sid in userlist:            
        updqry = ""
        df1 = mcq_df[mcq_df.student_id==sid][['mcq', 'avgscore']].copy()
        mcq_list = list(set([x for x in df1.mcq]))
        for mcq in mcq_list:
            df2 = df1[ df1.mcq==mcq ]
            score = df2.avgscore.values[0]
            updqry += ",mcq_avg" + str(mcq) + " = " + str(score)
        updqry = "update userdata set " + updqry[1:] + " where studentid = " + str(sid) + " and " + condqry
        rds_update(updqry)
    syslog(f"completed on {course_id}")
    return

def edx_import(course_id, client_name):
    qry = f"select cohort_id from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
    cohort_id = rds_param(qry)
    if cohort_id=="":
        cohort_id = piece(piece(course_id,':',1),'+',1)
    qry = f"select module_code from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"    
    module_code = rds_param(qry)
    if module_code=="":
        module_code = piece(cohort_id,'-',0)
    condqry = f"client_name = '{client_name}' and courseid = '{course_id}';"
    qry = f"select pillar from course_module where client_name = '{client_name}' and module_code = '{module_code}';"
    module_id = rds_param(qry)
    if module_id=="":  # either incomplete course and just a course header
        syslog("incomplete information , edx_import stopped")
        return 
    course_name = edx_coursename(course_id)  
    #eoc = edx_endofcourse(client_name, course_id)
    eoc = 0
    update_playbooklist(course_id, client_name, course_name, eoc)
    if eoc==1:
        syslog(f"this course {course_id} already expired. No need to import again")        
        return
    
    cnt = rds_param("select count(*) as cnt from stages where client_name='{client_name}' and courseid='{course_id}';")
    cnt = int("0" + str(cnt))
    if cnt==0:
        qry = f"select * from stages_master where client_name = '{client_name}' and module_code = '{module_code}';"
        df = rds_df(qry)
        if df is not None:
            df.columns = get_columns("stages_master")
            query = "delete from stages where " + condqry
            rds_update(query)
            #df.drop(columns=['course_code'], inplace=True)
            df.drop(columns=['module_code'], inplace=True)
            df['stagedate'] = ''
            df['courseid'] = course_id
            df1 = df[['client_name','courseid', 'id', 'stage', 'name', 'desc', 'days', 'f2f', 'mcq', 'flipclass', 'assignment', 'IU', 'stagedate']]
            syslog("update stages table")
            copydbtbl(df1, "stages")
            
    df = edx_userdata(course_id)
    nrows = len(df)
    if nrows == 0:
        syslog("no user data from edx api found")
        return 
    query = "delete from userdata where " + condqry
    rds_update(query)
    if ('.db' in vmsvclib.rds_connstr):
        query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND "
        query += f"(strftime(substr(startdate,7,4)||'-'||substr(startdate,4,2)||'-'||substr(startdate,1,2)) "
        query += f"<= strftime(date('now'))) ORDER BY id DESC LIMIT 1;"
    else:
        query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND "
        query += "STR_TO_DATE(startdate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    stage = rds_param(query)                
    if stage=="":
        stage = "SOC Days"
    df['client_name'] = client_name
    df['module_id'] = module_id
    df['amt'] = 0
    df['grade'] = 0
    df['stage'] = stage
    df['f2f'] = 0
    df['risk_level'] = 0
    df['mcq_zero'] = '[]'
    df['mcq_failed'] = '[]'
    df['as_zero'] = '[]'
    df['as_failed'] = '[]'
    df.rename(columns={'course_id':'courseid','student_id':'studentid'} , inplace=True)
    df1 = df[['client_name', 'module_id', 'courseid', 'studentid', 'username', 'amt', 'grade', 'stage', 'f2f', \
              'risk_level', 'mcq_zero', 'mcq_failed', 'as_zero', 'as_failed']]
    copydbtbl(df1, "userdata")    

    syslog("get # of mcq and assignment tests")
    #mcqcnt = edx_mcqcnt(course_id)
    #ascnt = edx_ascnt(course_id)
    #max_iu = edx_iu_counts(course_id, client_name)
    #if max_iu==0:
    #    sysog("max_iu returns 0")
    #    return
    #ascnt = mcqcnt
    #try:
    mlist = []
    #if max_iu > max_iu_cnt:
    #    max_iu = max_iu_cnt
    #rng_iu = range( 1, max_iu + 1 )
    rng_iu = range( 1, max_iu_cnt + 1 )
    list1 = [ "mcq_avg"+ str(x) + " = 0" for x in rng_iu]
    list2 = [ "mcq_attempts"+ str(x) + " = 0" for x in rng_iu]
    list3 = [ "as_avg"+ str(x) + " = 0" for x in rng_iu]
    list4 = [ "as_attempts"+ str(x) + " = 0" for x in rng_iu]
    mlist += list1 + list2 + list3 + list4
    query = "update userdata set " + ','.join(mlist) + " where " + condqry
    try:
        rds_update(query)
    except:
        syslog("unable to initialized table userdata")      
        return
    #if mcqcnt > 0:
    #    mlist += list1 + list2
    #if ascnt > 0:
    #    mlist += list1 + list2
    #except:
    #    syslog(f"error reset the mcq & assignment variables")
    #    return 
    
    #if (mcqcnt + ascnt) > 0:
    max_iu = edx_iu_counts(course_id, client_name)
    if max_iu > 0:
        update_assignment(course_id, client_name)
        update_mcq(course_id, client_name)
    update_schedule(course_id, client_name)
    return 

def count_avg_cols(client_name, course_id, maxmcqtest, colname):
    condqry = " from userdata where courseid = '" + course_id + "' and client_name = '" + client_name + "';"
    query =  'select ' + ','.join([ colname + str(x) for x in range( 1, maxmcqtest + 1 )]) + condqry
    df = rds_df(query)
    if df is None:
        return []
    rowcnt = len(df)
    if rowcnt==0:
        return []
    query =  'select ' + ','.join([ 'sum(' + colname + str(x) + ')/' + str(rowcnt) + ' as x' + str(x) for x in range( 1, maxmcqtest + 1 )])
    query +=  condqry
    df = rds_df(query)
    return [df[x][0] for x in list(df) ]

def gen_mcqas_df(colsum, client_name, course_id):
    if sum(colsum)==0:
        return None
    query = "select client_name, courseid as course_id, studentid, grade, 0 as mcq_sumscore, 0 as as_sumscore"
    xopts = ["mcq" , "as"]
    cols = ['client_name','courseid', 'studentid', 'grade', 'mcq_sumscore', 'as_sumscore', 'mcq_maxattempts']
    for xvar in xopts:
        n = xopts.index(xvar)
        rsum = colsum[n]
        if rsum==0:
            query += ",0 as " + xvar + "_maxattempts"
        else:
            xavg = xvar + "_avg"
            xatt = xvar + "_attempts"        
            cols += [ xavg + str(x) for x in range( 1, rsum + 1 ) ]
            cols += [ xatt + str(x) for x in range( 1, rsum + 1 ) ]
            updqry1 = ''.join([ ', ' + xavg + str(x) for x in range( 1, rsum + 1 )])        
            updqry2 = ''.join([ ', ' + xatt + str(x) for x in range( 1, rsum + 1 )])
            updqry3 = ', '.join([ xatt + str(x) for x in range( 1, rsum + 1 )])
            query += updqry1 + updqry2 
    query += " from userdata where client_name = '" + client_name + "' and courseid = '" + course_id + "';"
    df = rds_df(query)    
    if df is None:
        return    
    df.columns = cols    
    for xvar in xopts:    
        df[xvar + "_maxattempts"] = 0
        df_expr = ', '.join([ "x." + xatt + str(x) for x in range( 1, rsum + 1 ) ])
        df_expr = "max(" + df_expr + ")"
        df[xvar + "_maxattempts"] = df.apply(lambda x : eval(df_expr), axis=1)    
    for xvar in xopts:
        try:
            n = xopts.index(xvar)
            rsum = colsum[n]
            if rsum==0:
                df[xvar + '_cnt'] = 0
                df[xvar + "_avgattempts"] = 0
                df[xvar + '_avgscore'] = 0
            else:
                xavg = xvar + "_avg"
                xatt = xvar + "_attempts"
                cols = [ xavg + str(x) for x in range( 1, rsum + 1 )]
                df[ xvar + "_sumscore" ] = df[cols].sum(axis=1)
                cols = [ xatt + str(x) for x in range( 1, rsum + 1 )]
                updqry2 = '+ '.join(cols)
                df[ xvar + "_sumattempts" ] = df[cols].sum(axis=1)
                for n in range(1, rsum + 1):      
                    attflag = xatt + str(n)
                df[attflag] = df[attflag].apply(lambda x: 1 if x>0 else 0)    
                df[xvar + '_cnt'] = df.eval(updqry2)
                df[xvar + "_avgattempts"] = df.apply(lambda x: (x[xvar + "_sumattempts"] / x[xvar + "_cnt"]) if x[xvar + "_cnt"] > 0 else 0, axis=1)
                df[xvar + '_avgscore'] = df.apply(lambda x: ( x[xvar + "_sumscore"] / x[xvar + "_cnt"]) if x[xvar + "_cnt"] > 0 else 0, axis=1)
        except:
            syslog(f"error updating {xvar}")

    try:
        df1 = df[["client_name", "course_id","studentid","grade","mcq_avgscore","mcq_avgattempts", \
             "mcq_maxattempts","mcq_cnt","as_avgscore","as_avgattempts","as_maxattempts","as_cnt"]]
        return df1
    except:
        return None

def update_mcqas_info(client_name, course_id):
    iu_cnt = max_iu_cnt
    query = f"delete from mcqas_info where client_name = '{client_name}' and course_id = '{course_id}';"
    rds_update(query)
    
    colsum=[]
    for xvar in ["mcq_avg" , "as_avg"]:
        avgsum_list = count_avg_cols(client_name, course_id, iu_cnt, xvar)
        if len(avgsum_list)>0:
            rr = [ 1 if r>0 else 0 for r in avgsum_list ]
            rsum = sum(rr)
            colsum.append(rsum)

    if len(colsum) > 0:
        df = gen_mcqas_df(colsum, client_name, course_id)        
        if df is None:
            syslog(f"there is no data for {course_id}")
            return
        copydbtbl(df,"mcqas_info")
    syslog(f"completed on {course_id}")
    return

def generate_mcq_as(client_name):
    df = rds_df("select distinct course_id from playbooks where client_name = '" + client_name + "';")
    df.columns = ['course_id']
    course_list = [x for x in df.course_id]
    syslog(f"Generating data based on playbooks")
    for course_id in course_list:
        try:
            update_mcqas_info(client_name, course_id)
        except:
            pass
    syslog(f"completed on {client_name}")
    return

def update_userdf(userdf, client_name):
    query = f"select distinct studentid from user_master where client_name = '{client_name}' order by studentid;"
    df = rds_df(query)    
    if df is None:
        sid_list = []
    else:
        df.columns = ['studentid']
        sid_list = [ x for x in df.studentid]
    df = userdf    
    df.rename(columns={'course_id':'courseid','student_id':'studentid'} , inplace=True)
    df['client_name'] = client_name 
    df['courseid'] = ''
    df['chat_id'] = 0
    df['binded'] = 0
    df['usertype'] = 1
    df['existed'] = df.apply(lambda x: 1 if x['studentid'] in sid_list else 0, axis=1)
    df = df[df.existed == 0]
    df = df[['client_name','studentid','username','email','usertype','binded','chat_id','courseid']] 
    if len(df)==0:
        return df
    email_filter = rds_param(f"SELECT `value` from params WHERE  `key` = 'email_filter' and client_name = '{client_name}';")
    efilter = email_filter.split(',')
    df['usertype'] = df.apply(lambda x: 11 if x['email'].lower().split('@')[1] in efilter else 1, axis=1)
    return df

def get_calendar_json(api_url):
    data = {}
    syslog(f"calling api {api_url} via requests.post")
    response = requests.get(api_url)
    syslog(f"completed with status code {response.status_code}")
    if response.status_code==200:
        result = response.content.decode('utf-8')        
        if len(result)>0:
            data = json.loads(result)
    return data

def get_stage_list(data, module_node):
    if data == {}:
        return []
    resp = data['events']
    if len(resp)==0:
        return []
    stage_list = []
    soc_date = resp[0]['startDate'][:10] 
    socd = dmy_str(soc_date)    
    socdt = ymd_str(soc_date)
    s_dict = {}
    s_list = []    
    for x in resp:
        if 'summary' not in list(x):
            syslog("summary element missing")
            continue
        if 'description' not in list(x):
            syslog("description element missing")
            continue
        x_summary = x['summary']
        x_desc = x['description']
        
        # https://sambaash.atlassian.net/browse/VMSUPPORT-17
        # x_summary = x_summary.replace(' : ',':')
        # x_summary = x_summary.replace('::',':')
        # x_list = x_summary.split(':')
        x_list = x_summary.split(' : ')        
        n=len(x_list)
        cohort = x_list[0] if n>0 else ""
        stage_name = x_list[1] if n>2 else ""
        stage_desc = x_list[2] if n>2 else stage_name
        stage_desc = stage_desc.replace('?','')        
        cohort = cohort.strip()
        iu_list = "0"
        if x_desc is not None:
            if "IU " in x_desc:
                iu_list = ','.join([ x.split(':')[0].split(' ')[1] for x in x_desc.split('>') if "IU " in x])        
        stdate = ymd_str(x['startDate'][:10])
        sdate = dmy_str(x['startDate'][:10])
        edate = dmy_str(x['endDate'][:10])        
        stage_name = stage_code(stage_name)
        if (stage_name != "") and (cohort==module_node) :
            stage_info = [stage_name, cohort, stage_desc, sdate, edate, iu_list]
            s_dict[stdate] = stage_info
            s_list.append(stdate)
    if s_list==[]:
        syslog("stages info s_list empty")
        return []
    
    s_list.sort()
    s_list = sorted(set(s_list))
    n = 0
    cohort_dict = {}
    index_dict = {}    
    cohort_list = []
    for x in [ s_dict[x][1] for x in s_list ]:
        if x not in cohort_list:
            cohort_list.append(x)    
    for x in cohort_list:
        cohort_dict[x] = {}
        index_dict[x] = []    
    
    for x in s_list:
        y = s_dict[x][1]
        cohort_dict[y][x]=s_dict[x]
        index_dict[y].append(x)        
    for x in cohort_list:
        z = cohort_dict[x]
        w = [ vv[0] for (kk,vv) in z.items() ]
        dt0 = min([ kk for (kk,vv) in z.items() ])
        startdt = cohort_dict[x][dt0][3]
        dt0 = str2date(startdt) - datetime.timedelta(days=1)
        if 'SOC' not in w:
            dt0a = ymd_str(str(dt0))
            dt0b = dmy_str(str(dt0))
            cohort_dict[x][dt0a] = ["SOC","SOC Days","Start of course",dt0b, startdt, "0"]
            index_dict[x].append(dt0a)
        if 'EOC' not in w:
            dt1 = max([ kk for (kk,vv) in z.items() ])        
            enddt = cohort_dict[x][dt1][3]
            dt1 = str2date(enddt) + datetime.timedelta(days=1)
            dt1a = ymd_str(str(dt1))
            dt1b = dmy_str(str(dt1))
            cohort_dict[x][dt1a] = ["EOC",x,"End of course",enddt, dt1b, "0"]
            index_dict[x].append(dt1a)
        s_list = sorted(index_dict[x])
        index_dict[x] = s_list
        stg = "SOC"
        sorted_stage_list = []        
        for y in list(index_dict[x]):
            if cohort_dict[x][y][0] == "SOC":
                cohort_dict[x][y][1] = "SOC Days"
            else:
                if stg == cohort_dict[x][y][0]:
                    cohort_dict[x][y][0] += "2"
                    cohort_dict[x][y][2] += " 2"
                stg = stg + ' - ' + cohort_dict[x][y][0]
                cohort_dict[x][y][1] = stg
            stg = cohort_dict[x][y][0]
            cohort_dict[x][y].append((str2date(cohort_dict[x][y][3]) - dt0).days )
            sorted_stage_list.append(cohort_dict[x][y])
        cohort_dict[x] = sorted_stage_list       
    return cohort_dict

def stage_code(txt):
    tt = [t for t in txt]
    n=len(txt)
    wt = ''.join([ ' ' if tt[i-1].isnumeric() and not tt[i].isnumeric() else tt[i] for i in range(1,n)])
    wlist = ['learning','assignment','flipped','implementation','mentoring','assessment','orientation']
    vlist = ['EL','A','FC','PI','PM','SA', 'SOC']
    ulist = [ w for w in wt.split(' ') if w!='' and w.isnumeric() ]
    uu = ulist[0] if len(ulist)>0 else ''
    tt = txt.lower()
    result = [w for w in wlist if w in tt]
    stg = ""
    if result == []:
        result = [ x for x in vlist if txt.startswith(x) ]
        if result == []:        
            return ""
        stg = txt
    else:
        n = wlist.index(result[0])
        stg = vlist[n] + uu
    return stg

def get_google_calendar(course_id, client_name):
    qry = f"select cohort_id from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
    cohort_id = rds_param(qry)
    if cohort_id=="":
        cohort_id = piece(piece(course_id,':',1),'+',1)
    qry = f"select module_code from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"    
    module_code = rds_param(qry)
    if module_code=="":
        module_code = piece(cohort_id,'-',0)
    qry = f"select course_code from course_module where module_code = '{module_code}' and client_name='{client_name}' limit 1;"
    course_code = rds_param(qry)
    api_url = f"https://realtime.sambaash.com/v1/calendar/fetch?cohortId={course_code}%20:%20{cohort_id}"
    data  = get_calendar_json(api_url)
    if data == {}:
        # https://sambaash.atlassian.net/browse/VMSUPPORT-17
        # api_url = f"https://realtime.sambaash.com/v1/calendar/fetch?cohortId={course_code}:{cohort_id}"
        # data  = get_calendar_json(api_url)
        # if data == {}:    return []
        syslog("there is no data from google calendar")
        return []
    #sorted_stage_list = get_stage_list(data) # add cohort_id ?
    sorted_stage_list = get_stage_list(data, cohort_id)
    stage_list = []
    for cohort in list(sorted_stage_list) :
        stage_list = sorted_stage_list[cohort]        
    return stage_list

def update_stage_table(stage_list, course_id, client_name, cohort_id, module_code):
    # Status : Tested  f2f field got loss of data
    qry = " client_name = '_c_' and courseid = '_x_';"
    qry = qry.replace('_c_', client_name)
    qry = qry.replace('_x_', course_id)    
    mcq_dict = {}
    ast_dict = {}
    stg_list = []    
    fc_dict = {}
    iu_dict = {}
    f2f_dict = {}

    query = "select * from stages where " + qry
    df = rds_df(query)
    if (df is None) or (len(stage_list)==0):        
        #cohort_id = piece(piece(course_id,':',1),'+',1)
        #module_code = piece(cohort_id,'-',0)
        query = f"select * from stages_master where client_name = '{client_name}' and module_code = '{module_code}';"
        df = rds_df(query)
        if df is None:
            return []
        df.columns = get_columns("stages_master")
        stg_list = [x for x in df.stage]
        stgname_list = [x for x in df.name]
        stgdesc_list = [x for x in df.desc]
        stgdays_list = [x for x in df.days]
        mcq_list = [x for x in df.mcq]
        ast_list = [x for x in df.assignment]
        fc_list = [x for x in df.flipclass]
        iu_list = [x for x in df.IU]
        f2f_list = [x for x in df.f2f]
        mcq_dict = dict(zip(stg_list,mcq_list))
        ast_dict = dict(zip(stg_list,ast_list))
        fc_dict = dict(zip(stg_list,fc_list))
        iu_dict = dict(zip(stg_list,iu_list))
        f2f_dict = dict(zip(stg_list,f2f_list))
        dt0 = edx_day0(course_id)
        stage_list = []
        id = 0
        cnt = len(stg_list)
        for n in range(cnt):
            fld1 = stg_list[n]
            fld2 = stgname_list[n]
            fld3 = stgdesc_list[n]
            m = n if (cnt == n + 1) else n+1
            fld7 = stgdays_list[n]
            dd = stgdays_list[m]
            dt1 = dt0 + datetime.timedelta( days = (fld7-1) )
            dt2 = dt0 + datetime.timedelta( days = (dd-1) )
            fld4 = dt1.strftime('%d/%m/%Y')
            fld5 = dt2.strftime('%d/%m/%Y')
            fld6 = iu_list[n]            
            stage_list.append([fld1, fld2, fld3, fld4, fld5, fld6, fld7])
    else:
        df.columns = get_columns("stages")        
        stg_list = [x for x in df.stage]
        mcq_list = [x for x in df.mcq]
        ast_list = [x for x in df.assignment]
        fc_list = [x for x in df.flipclass]
        iu_list = [x for x in df.IU]
        f2f_list = [x for x in df.f2f]
        mcq_dict = dict(zip(stg_list,mcq_list))
        ast_dict = dict(zip(stg_list,ast_list))
        fc_dict = dict(zip(stg_list,fc_list))
        iu_dict = dict(zip(stg_list,iu_list))
        f2f_dict = dict(zip(stg_list,f2f_list))

    m = len(stage_list)    
    if m==0:        
        return []
    
    for x in stage_list:
        if x[0] not in stg_list:
            mcq_dict[ x[0] ] = "0"
            ast_dict[ x[0] ] = "0"
            fc_dict[ x[0] ] = "0"
            iu_dict[ x[0] ] = "0"
            f2f_dict[ x[0] ] = 0
    for x in stage_list:
        x.append( mcq_dict[x[0]] )
        x.append( ast_dict[x[0]] )
        x.append( fc_dict[x[0]] )
        x.append( iu_dict[x[0]] )
        x.append( f2f_dict[x[0]] )
    
    query = "delete from stages where " + qry
    rds_update(query)
    
    n = 0
    for n in range(len(stage_list)):
        stg = stage_list[n][0]
        query = "insert into stages(client_name,courseid,id,stage,`name`,`desc`,days,f2f,mcq,assignment,IU,flipclass) values("
        query += "'" + client_name + "','" + course_id + "'," + str(n+1) # client_name, courseid, id
        query += ",'" + str(stage_list[n][0]) + "','"  # stage
        query += str(stage_list[n][1]) + "','"         # stage_name
        query += str(stage_list[n][2]) + "',"          # stage_desc
        query += str(stage_list[n][6]) + ","           # days
        query += str(stage_list[n][11]) + ",'"         # f2f
        query += str(stage_list[n][7]) + "','"         # mcq
        query += str(stage_list[n][8]) + "','"         # assignment
        query += str(stage_list[n][10])  + "','"       # IU
        query += str(stage_list[n][9]) + "')"          # flipclass
        rds_update(query)
    
    arr_stagedate = [ x[3] for x in stage_list]
    m = len(arr_stagedate)    
    if m==0:
        syslog("There is not enough information to build the schedule")
        return []
    for n in range(m - 1):
        id = n + 1
        start_date = arr_stagedate[n] 
        stg_date = arr_stagedate[id] 
        query = f"update stages set startdate = '{start_date}', stagedate = '{stg_date}' where id = {id} and " + qry        
        try:
            rds_update(query)
        except:
            syslog("error in sql update\n{query}")
            pass
    
    start_date = arr_stagedate[-1] 
    eoc_date = string2date(stage_list[-1][4],"%d/%m/%Y")
    stg_date = eoc_date.strftime('%d/%m/%Y')
    query = f"update stages set startdate = '{start_date}',stagedate = '{stg_date}' where id = {m} and " + qry
    rds_update(query)           
    if ('.db' in vmsvclib.rds_connstr):
        query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND "
        query += f"(strftime(substr(stagedate,7,4)||'-'||substr(stagedate,4,2)||'-'||substr(stagedate,1,2)) "
        query += f"<= strftime(date('now'))) ORDER BY id DESC LIMIT 1;"
    else:
        query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND "
        query += "STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    stage = rds_param(query)
    if stage != "":
        query=f"update userdata set stage='{stage}' WHERE client_name = '{client_name}' AND courseid='{course_id}' ;"
        try:
            rds_update(query)
        except:
            syslog("error in sql update\n{query}")
    return stage_list

def update_playbooklist(course_id, client_name, course_name, eoc):
    try:
        #query = f"delete from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
        #rds_update(query)
        query = f"select count(*) as cnt from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
        cnt = rds_param(query)
        if cnt==1:
            return
        #cohort_id = piece(piece(course_id,':',1),'+',1)
        #module_code = piece(cohort_id,'-',0)
        #if module_code.lower() == 'master':
        #    module_code = piece(cohort_id,'-',1)
        #qry = f"select course_code from course_module where module_code = '{module_code}' and client_name='{client_name}' limit 1;"
        #course_code = rds_param(qry)            
        qry = f"select cohort_id from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
        cohort_id = rds_param(qry)
        if cohort_id=="":
            cohort_id = piece(piece(course_id,':',1),'+',1)    
        qry = f"select module_code from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"    
        module_code = rds_param(qry)
        if module_code=="":
            module_code = piece(cohort_id,'-',0)
        query = "insert into playbooks(client_name,module_code,cohort_id,course_id,course_name,mentor,eoc) \
            values('_c_', '_w_', '_x_', '_y_', '_z_', '', _e_);"
        query = query.replace("_c_", client_name)
        query = query.replace("_w_", module_code)
        query = query.replace("_x_", cohort_id)
        query = query.replace("_y_", course_id)
        query = query.replace("_z_", course_name)
        query = query.replace("_e_", str(eoc))
        rds_update(query)
    except:
        syslog("Unable to update playbook list")
    return

def edx_mass_update(func, clt):
    # Status : Tested
    global edx_api_header, edx_api_url
    module_code = lambda x : piece(piece(piece(x,':',1),'+',1),'-',0)
    date_today = datetime.datetime.now().date()
    keyword = date_today.strftime('%Y')
    if clt=="":
        with open("vmbot.json") as json_file:  
                bot_info = json.load(json_file)
        client_name = bot_info['client_name']
    else:
        client_name = clt

    qry = f"delete from userdata where client_name='{client_name}' and module_id = '';"   
    rds_update(qry)
    course_list = search_course_list(keyword)
    if course_list==[]:
        syslog("there is nothing found, check the rest api.")
        return
    qry = f"SELECT DISTINCT module_code FROM course_module WHERE enabled=1 and client_name='{client_name}';"
    df = rds_df(qry)
    if df is None:
        mc_list = []
    else:
        df.columns = ['module_code']
        mc_list = [x for x in df.module_code]
        course_list = [ x for x in course_list if module_code(x) in mc_list ]    
    for course_id in course_list:        
        eoc = edx_endofcourse(client_name, course_id)
        #eoc = 0
        query = f"update playbooks set eoc = {eoc} where client_name = '{client_name}' and course_id = '{course_id}';"
        rds_update(query)
        if eoc == 0:            
            func(course_id, client_name)
    return

def edx_mass_import(client_name):
    edx_mass_update(edx_import,client_name)
    return

def mass_update_assignment(client_name):
    edx_mass_update(update_assignment,client_name)
    return

def mass_update_mcq(client_name):
    edx_mass_update(update_mcq,client_name)
    return

def mass_update_schedule(client_name):
    edx_mass_update(update_schedule,client_name)
    return

def mass_update_usermaster(client_name):
    userdf = edx_alluserdata()
    if len(userdf)==0:
        return        
    df = update_userdf(userdf, client_name)
    if df is None:
        syslog("Failed to perform user master update")
        return
    if len(df)==0:
        syslog("nothing to update")
        return    
    copydbtbl(df,"user_master")    
    syslog(f"completed on {client_name}")
    return

def test_google_calendar(course_id, client_name):
    stage_list = get_google_calendar(course_id, client_name)
    for x in stage_list:
        print(x)
    print("\n")
    return stage_list

def edx_alluserdata():
    # Status : open
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'student_id':[],'course_id':[],'username':[],'email':[]})
    url = f"{edx_api_url}/user/fetch/all"
    syslog(f"calling api {url} via requests.post")
    response = requests.get(url, headers=edx_api_header)      
    syslog(f"completed with status code {response.status_code}")
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if str(data) == "":
            return None
        userinfo = json.loads(data)
        rows_cnt = len(list(userinfo))
        if rows_cnt == 0:
            return df
        user_list = []
        course_list = []
        username_list = []
        email_list = []
        for usr in userinfo:
            uid = usr['user_id']
            course_id = usr['enrolments'][0]['course_id']
            user_list.append(uid) 
            course_list.append(course_id)
            username_list.append(usr['username'])
            email_list.append(usr['email'])
        data = {'student_id':user_list,'course_id':course_list,'username':username_list,'email':email_list}
        df = pd.DataFrame.from_dict(data)
    return df

def perform_unit_tests(client_name = 'Sambaash', course_id = "course-v1:Lithan+FOS-0620A+17Jun2020", sid = 6464):
    #edx_api_url = "https://om.sambaash.com/edx/v1"
    edx_api_url = "https://omnimentor.lithan.com/edx/v1"
    edx_api_header = {'Authorization': 'Basic ZWR4YXBpOlVzM3VhRUxJVXZENUU4azNXdG9E', 'Content-Type': 'text/plain'}
    print("====== test case 1 edx_coursename ==========")
    coursename = edx_coursename(course_id)
    print( coursename )
    print("====== test case 2 edx_day0 ==========")
    edx_daystart = edx_day0(course_id)
    print( edx_daystart )
    print("====== test case 3 edx_courseid ==========")
    course_id = edx_courseid("FOS-1219A")
    print( course_id )
    print("====== test case 4 edx_mcqcnt ==========")
    mcqcnt = edx_mcqcnt(course_id)
    #mcqcnt = edx_iu_counts(course_id, client_name)
    print( mcqcnt )
    print("====== test case 5 edx_ascnt ==========")
    ascnt = edx_ascnt(course_id)
    #ascnt = edx_iu_counts(course_id, client_name)
    print( ascnt )
    print("====== test case 6 edx_userdata ==========")
    df = edx_userdata(course_id)
    print( df[:5] )
    print("====== test case 7 student_course_list ==========")
    df = student_course_list(sid)
    print( df )
    print("====== test case 8 edx_grade ==========")
    df = edx_grade(course_id)
    print( df[:5] )
    print("====== test case 9 edx_mcqinfo ==========")
    df = edx_mcqinfo(client_name, course_id)
    print( df[:5] )
    print("====== test case 10 edx_assignment_score ==========")
    df = edx_assignment_score(course_id)
    print( df[:5] )
    print("====== test case 11 search_course_list ==========")
    course_list = search_course_list("FOS%2020")
    print( course_list )
    print("====== test case 12 google calendar ==========")
    test_google_calendar(course_id, client_name)
    print("test_google_calendar completed")    
    print("====== test case 13 edx_import ==========")    
    #edx_import(course_id,  client_name)
    print( f"edx_import {course_id},  {client_name}")
    print("====== test case 14 generate_mcq_as ==========")
    #generate_mcq_as(client_name)
    print("generate_mcq_as completed")       
    print("====== test case 15 mass_update_usermaster ==========")
    #mass_update_usermaster(client_name)
    print("mass_update_usermaster completed")
    print("====== test case 16 edx_alluserdata ==========")
    #df = edx_alluserdata()
    print( df[:5] )
    print("edx_alluserdata completed")
    print("end of unit tests 1 - 12")
    return

if __name__ == "__main__":    
    global use_edxapi, edx_api_header, edx_api_url
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    #edx_api_url = "https://omnimentor.lithan.com/edx/v1"
    edx_api_url = "https://omnimentor.sambaash.com/edx/v1"
    #edx_api_url = "http://localhost:8080/edx/v1"
    edx_api_header = {'Authorization': 'Basic ZWR4YXBpOlVzM3VhRUxJVXZENUU4azNXdG9E', 'Content-Type': 'text/plain'}
    vmsvclib.rds_connstr = bot_info['omdb']
    vmsvclib.rdscon = None
    vmsvclib.rds_pool = 0
    vmsvclib.rds_schema = "omnimentor"
    if len(sys.argv)>2:
        cmd = str(sys.argv[1])
        resp = str(sys.argv[2])
        course_list = search_course_list("%" + resp + "%" )
        if cmd=="search":
            for course_id in course_list:
                print(course_id)
        elif len(course_list)==1:
            course_id = course_list[0]
            print(course_id, client_name)
            if cmd=="import":
                edx_import(course_id, client_name)
                print("Edx import completed")
            elif cmd=="mcq":
                update_mcq(course_id, client_name)
                print("MCQ update completed")
            elif cmd=="assignment":
                update_assignment(course_id, client_name)
                print("Assignment update completed")
            elif cmd=="schedule":
                update_schedule(course_id, client_name)
                print("Schedule update completed")
            elif cmd=="calendar":
                test_google_calendar(course_id, client_name)
                print("google calendar test completed")
            elif cmd=="info":
                results = edx_day0(course_id)
                print(results)
                eoc = edx_endofcourse(client_name, course_id)
                results = f"End of course (1:Yes,0:No) = {eoc}"
                print(results)
        elif len(course_list)>0:
            print("which course_id is the one ?")
            for course_id in course_list:
                print(course_id)
        else:
            print("Unable to find matching information")
    else:
        #results = edx_iu_counts(course_id, client_name)
        #results = sms_lecturer(client_name, course_id, sid)    
        #cols = get_columns("stages")
        #date_list = sms_attendance(course_id, sid)
        #print(date_list)
        #results = sms_missingdates(client_name, course_id, sid, cols, date_list)             
        #results = edx_eocgap(client_name, course_id, 7)
        #results = sms_pmstage(client_name, course_id)
        #results = edx_eocend(client_name, course_id, 7)
        #print(results)
        #att_rate = sms_att_rate(client_name, course_id, sid, date_list)    
        #print(f"Attendance rate = {att_rate}")
        #
        #=====================================
        # edx_mass_import(client_name)
        # mass_update_mcq(client_name)
        # mass_update_assignment(client_name)
        # mass_update_schedule(client_name)
        # mass_update_usermaster(client_name)
        # perform_unit_tests(client_name, course_id, sid)
        #=====================================
        #    
        prog = str(sys.argv[0])
        print(f"usage :\n\tpython3 {prog} [commands] [cohort_id]")
        print("commands:\n\timport\n\tmcq\n\tassignment\n\tschedule\n\tcalendar\n\tinfo")
        print(f"Example:\n\tpython3 {prog} search DQV-0820")
