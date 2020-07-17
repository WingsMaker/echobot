# Note
# this code already merged into vmsvcbot.py for the next implementation
# keeping this file for reference
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
import re
import requests
import vmsvclib
from vmsvclib import *

global edx_api_header, edx_api_url

piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
str2date = lambda x : string2date(x,"%d/%m/%Y")            
dmy_str = lambda v : piece(v,'-',2) + '/' + piece(v,'-',1) + '/' + piece(v,'-',0)
ymd_str = lambda v : piece(v,'-',0) + '/' + piece(v,'-',1) + '/' + piece(v,'-',2)
#debug = lambda x : vmsvclib.debug(x)
#callgraph = lambda x : vmsvclib.callgraph(x)

def edx_coursename(course_id):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchCourseNameByCourseId
    # https://om.sambaash.com/edx/v1/course/fetch/name
    global edx_api_header, edx_api_url
    coursename = ""
    url = f"{edx_api_url}/course/fetch/name"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')        
        coursename = str(data)
    return coursename

def edx_day0(course_id):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchCourseStartDateByCourseId
    # https://om.sambaash.com/edx/v1/course/fetch/startdate
    global edx_api_header, edx_api_url
    start_date = ""
    url = f"{edx_api_url}/course/fetch/startdate"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if str(data) == "":
            return ""
        start_date = datetime.datetime.strptime(str(data)[:10],"%Y-%m-%d").date()
    return start_date

def edx_courseid(cohort_id):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchCourseIdByCourseLike
    # https://om.sambaash.com/edx/v1/course/fetch/id
    global edx_api_header, edx_api_url
    course_id = ""
    url = f"{edx_api_url}/course/fetch/id"
    response = requests.post(url, data=cohort_id, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        course_id = str(data)
    return course_id

def edx_mcqcnt(course_id):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchMCQCountByCourseId
    # https://om.sambaash.com/edx/v1/course/fetch/mcq/count
    global edx_api_header, edx_api_url
    mcqcnt = 0
    url = f"{edx_api_url}/course/fetch/mcq/count"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)        
    if response.status_code==200:
        data = response.content.decode('utf-8')            
        mcqcnt = int("0" + str(data))
    return mcqcnt

def edx_ascnt(course_id):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchAssignmentCountByCourseId
    # https://om.sambaash.com/edx/v1/course/fetch/assignment/count
    global edx_api_header, edx_api_url
    ascnt = 0
    url = f"{edx_api_url}/course/fetch/assignment/count"        
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)        
    if response.status_code==200:
        data = response.content.decode('utf-8')            
        ascnt = int("0" + str(data))    
    return ascnt

def edx_userdata(course_id):
    # Status : Tested    
    # url = f"{edx_api_url}/course/fetch/users"
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchAllUsers
    # https://om.sambaash.com/edx/v1/course/fetch/users
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'student_id':[],'course_id':[],'username':[],'email':[]})
    url = f"{edx_api_url}/course/fetch/users"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)        
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
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/User/fetchUserByUserId
    # https://om.sambaash.com/edx/v1/user/fetch/1234
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'course_id':[]})
    url = f"{edx_api_url}/user/fetch/{student_id}"
    response = requests.get(url, headers=edx_api_header)
    if response.status_code==200:
        userinfo = json.loads(response.content.decode('utf-8'))
        course_list = [x['course_id'] for x in userinfo['enrolments']]
        df = pd.DataFrame.from_dict({'course_id':course_list})            
    return df

def edx_grade(course_id, student_id=0):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/User/fetchUserGradesByCourseId
    # https://om.sambaash.com/edx/v1/user/fetch/grades/list
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict( {'student_id' : [] , 'grade' : [] } )
    if student_id==0:
        url = f"{edx_api_url}/user/fetch/grades/list"
    else:
        url = f"{edx_api_url}/user/fetch/grades/list/{student_id}"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)        
    if response.status_code==200:
        data = response.content.decode('utf-8')            
        data = eval(data)
        stud = [x['student_id'] for x in data]
        grade = [x['grade'] for x in data]            
        data = {'student_id' : stud , 'grade' : grade }
        df = pd.DataFrame.from_dict(data)
    return df

def edx_mcqinfo(client_name, course_id, student_id=0):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/User/fetchUserMCQScoresByCourseId
    # https://om.sambaash.com/edx/v1/user/fetch/mcq/scores/list
    # https://om.sambaash.com/edx/v1/user/fetch/mcq/scores/list/{userId}    
    global edx_api_header, edx_api_url    
    
    def getnumstr(qn_str):
        if ' ' in qn_str:
            qn_str = qn_str.split(' ')[0]
        qn_numstr  = ''.join([x for x in qn_str if x in '0123456789'])
        qn_numstr = '0' if qn_numstr=='' else qn_numstr
        qn = int(qn_numstr)
        if (qn > 13) or (qn<1):
            qn = 1
        return qn
    
    df_mcq = pd.DataFrame.from_dict( {'client_name':[],'course_id':[],'student_id':[], 'score':[],'mcq':[],'qn':[],'attempts':[]} )    
    if student_id==0:
        url = f"{edx_api_url}/user/fetch/mcq/scores/list"
    else:
        url = f"{edx_api_url}/user/fetch/mcq/scores/list/{student_id}"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)                
    if response.status_code==200:
        data = json.loads(response.content.decode('utf-8'))
        course_id_list = []
        student_id_list = []
        iu_list = []
        qn_list = []
        grade_list = []
        att_list = []
        for rec in [ x for x in list(data) if 'attempts' in x['state']]  :            
            sc = rec['score']
            pp = rec['points_possible']
            qn = getnumstr(rec['options_display_name'])
            iu = getnumstr(rec['chapter_title'])
            grade = 1.0 if pp==0 else float(sc/pp)
            course_id_list.append(rec['course_id'])
            student_id_list.append(rec['student_id'])                
            #iu_list.append(int(rec['IU']))
            iu_list.append(iu)
            #qn_list.append(int(rec['options_display_name'].split('.')[0][1:]))
            qn_list.append(qn)
            grade_list.append(grade)
            attempts = eval(rec['state'].replace('null','""').replace('false','0').replace('true','1'))['attempts']
            att_list.append(int(attempts))
        client_list = [client_name for x in iu_list]        
        data = {'client_name':client_list, 'course_id': course_id_list, 'student_id':student_id_list, \
            'score':grade_list ,'mcq':iu_list , 'qn':qn_list , 'attempts': att_list}            
        df_mcq = pd.DataFrame.from_dict(data)     
    return df_mcq

def edx_assignment_score(course_id, student_id=0):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/User/fetchUserAssignmentScoresByCourseId
    # https://om.sambaash.com/edx/v1/user/fetch/assignment/scores/list
    global edx_api_header, edx_api_url    
    if student_id==0:
        url = f"{edx_api_url}/user/fetch/assignment/scores/list"
    else:
        url = f"{edx_api_url}/user/fetch/assignment/scores/list/{student_id}"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    #df = pd.DataFrame.from_dict({'student_id': [], 'score': [] , 'IU': [] ,'attempts': []})
    df = pd.DataFrame.from_dict({'student_id':[], 'score':[], 'points_possible':[], 'IU': [], 'attempts': []})
    if response.status_code==200:
        data = response.content.decode('utf-8')
        rec = eval(data)        
        if rec != []:            
            student_id_list = [x['student_id'] for x in rec]
            grade_list = [x['score'] for x in rec]
            pp_list = [x['points_possible'] for x in rec]
            iu_list = [x['IU'] for x in rec]
            att_list = [x['attempts'] for x in rec]
            data = {'student_id': student_id_list, 'score': grade_list, 'points_possible':pp_list, 'IU': iu_list ,'attempts': att_list}
            df = pd.DataFrame.from_dict(data)
            df['is_num'] = df.apply(lambda x: 1 if x['IU'].isnumeric() else 0, axis=1)               
            df.drop(df[ df.is_num == 0 ].index, inplace=True)
            df.drop(columns=['is_num'], inplace=True)
    return df

def sms_attendance(course_id, student_id):
    # Status : okay
    # https://omnimentor.lithan.com/edx/v1/user/attendance/fetch/{student_id}
    global edx_api_header, edx_api_url
    if student_id==0:
        return []
    url = f"{edx_api_url}/user/attendance/fetch/{student_id}"
    response = requests.post(url, data=course_id, headers=edx_api_header, verify=False)
    date_list = []
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if data != "":
            result  = eval(str(data))
            date_list = [string2date(x['timetable_date'].split(' ')[0],"%m/%d/%Y") for x in result]
    return date_list
    
def sms_missingdates(client_name, course_id, student_id):
    f2flag = lambda x,y : ('' if y in date_list else x) if x[:2] in ['FC', 'PM'] else ''
    if student_id==0:
        return 0
    date_list = sms_attendance(course_id, student_id)
    missed_fsf = []
    df = rds_df(f"select * from stages where courseid = '{course_id}' and client_name = '{client_name}';")
    if df is not None:
        df.columns = get_columns("stages")
        stgid_list = [x for x in df.id]
        fsf_dates = [string2date(x,"%d/%m/%Y") for x in df.startdate]
        #fsf_dates = [x for x in df.startdate]
        stage_list = [x for x in df.stage]
        arr_stgf2f = dict(zip(stage_list, fsf_dates))
        #stage_dates=[string2date(arr_stgf2f[x],"%d/%m/%Y") for x in stage_list if x[:2]=='FC' or x[:2]=='PM']
        #mdat = [x.strftime('%d/%m/%Y') for x in stage_dates if x not in date_list]
        #mstg = [x for x in stage_list if string2date(arr_stgf2f[x],"%d/%m/%Y").strftime('%d/%m/%Y') in mdat]
        #missed_fsf = dict(zip(mstg,mdat))
        missed_fsf = [f2flag(stage_list[n],fsf_dates[n]) for n in range(len(stgid_list))]
    return missed_fsf

def search_course_list(keyword):
    # Status : Tested
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchCourseListByCourseIdLike
    # https://om.sambaash.com/edx/v1/course/search/course/list
    global edx_api_header, edx_api_url        
    course_list = []
    url = f"{edx_api_url}/course/search/course/list"
    response = requests.post(url, data=keyword, headers=edx_api_header, verify=False)
    if response.status_code==200:
        data = response.content.decode('utf-8')
        if len(data) > 0:
            course_list = eval(data)               
    return course_list

def edx_endofcourse(client_name, course_id):
    query = "SELECT (case SUBSTRING(`name`,-3) when 'EOC' then 1 else 0 END) eoc FROM stages WHERE client_name ="
    query += f"'{client_name}' AND courseid='{course_id}' AND STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    try:
        eoc = rds_param(query)    
        eoc=int(eoc)
    except:
        eoc = 0
    return eoc

def update_schedule(course_id, client_name):
    # Status : Tested
    dstr = lambda x : piece(piece(x.strip(),':',1),'+',2)
    dtype = lambda x : ('%d%b%Y' if len(dstr(x))==9 else '%d%B%Y') if dstr(x)[0].isdigit() else '%B%Y'
    cohort_date = lambda x : string2date(dstr(x),dtype(x))
    cohort_id = piece(piece(course_id,':',1),'+',1)
    stage_list = get_google_calendar(course_id, client_name)

    #if stage_list == []:
    #    print(f"no data from google calendar for {course_id}")
    #    return
    
    try:
        stage_list = update_stage_table(stage_list, course_id, client_name)
        if stage_list == []:
            #print(f"{course_id} is not ready for schedule update")
            return
        dt = string2date(stage_list[0][3],"%d/%m/%Y")
        stg=[x for x in stage_list if x[0]=='EOC']
        eoc_date = string2date(stg[0][4],"%d/%m/%Y")
        date_today = datetime.datetime.now().date()
        days = (date_today - eoc_date).days
    except:
        print("update_stage_table failed")
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
    query = "update stages a INNER JOIN stages_master b on b.client_name=a.client_name AND b.stage=a.stage "
    query += "AND b.module_code = substring_index(substring_index(SUBSTRING_INDEX(a.courseid,'+',2),'+',-1),'-',1) "
    query += f"SET a.IU = b.IU WHERE a.client_name = '{client_name}' and a.courseid = '{course_id}';"
    #print(query)
    rds_update(query)
    
    query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND STR_TO_DATE(startdate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    current_stage = rds_param(query) 
    query = f"update userdata set stage = '{current_stage}' where " + condqry 
    rds_update(query)
    #print(f"update_schedule completed on {course_id}")    
    return

def update_assignment(course_id, client_name, student_id=0):
    # Status : Tested
    condqry = f"client_name = '{client_name}' and courseid = '{course_id}';"
    # df with columns ['student_id', 'grade'], overall score/grade sorted by student_ids
    # df = edx_grade(course_id)
    df = edx_grade(course_id, student_id)
    if df is None:        
        return False     
    if len(df) == 0:
        return False
    # Converts df into a dictionary format {student_id : grade}
    stud_grade_dict = dict(zip([x for x in df.student_id],[x for x in df.grade]))
    # df with columns ['student_id', 'score', 'IU'], assignment scores sorted by IU by student_ids
    #df = edx_assignment_score(course_id)    
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
        iu_num = iu_list[n]
        pp_num = pp_list[n]
        score = score_list[n]
        attempts = attempts_list[n]
        if sid not in stud_list:
            stud_list.append(sid)
            iu_score[sid] = dict()
            iu_attempts[sid] = dict()
        #iu_score[sid][iu_num] = int(score)/100
        iu_score[sid][iu_num] = 1 if pp_num == 0 else (int(score)/pp_num)
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
        rds_update(updsql)
    #print("update_assignment completed for course_id " + course_id)
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
    mcqcnt = edx_mcqcnt(course_id)
    if mcqcnt==0:
        return 
    #try:
    else:
        mcq_df = edx_mcqinfo(client_name, course_id, student_id)        
        if len(mcq_df) > 0:            
            query = "delete from mcq_data where " + cond_qry
            rds_update(query)
        df = mcq_df[['client_name', 'course_id', 'student_id', 'score', 'mcq', 'qn', 'attempts']]        
        copydbtbl(df, "mcq_data")

        # save into local database with tablename mcq_score with score/max_attempts per mcq tests/students/cohorts
        query = "select client_name, course_id, student_id, mcq, count(*) as max, sum(score) as mcqscore, \
            max(attempts) as max_attempts from mcq_data where " + cond_qry \
            + " group by client_name, course_id, student_id, mcq order by client_name, course_id, student_id, mcq"
        scoredf = rds_df(query)        
        cnt = 0
        if scoredf is not None :
            cnt = len(scoredf)
            if cnt > 0:                
                query = "delete from mcq_score where " + condqry
                rds_update(query)                
                scoredf.columns = ['client_name', 'courseid', 'studentid', 'mcq', 'max','score','max_attempts']
                copydbtbl(scoredf, "mcq_score")                            

        # not all courses has the same total # of MCQ test, ususally 13
        if cnt == 0:
            max_mcq = 0
        else:            
            query = "select max(mcq) AS maxmcq from mcq_score where " + condqry            
            df = rds_df(query)
            max_mcq = 0
            if df is not None:
                df.columns = ['maxmcq']
                max_mcq = df['maxmcq'][0]
            if max_mcq > 13:
                max_mcq = 0
                
        if max_mcq==0:            
            return 

        # reset mcq attempts
        #print("reset mcq attempts")
        updqry = "update userdata set " 
        updqry += ','.join([ "mcq_attempts" + str(x) + " = 0"  for x in range( 1, max_mcq + 1 )]) + " where "
        updqry += condqry        
        rds_update(updqry)

        # update mcq attempts
        #print("update mcq attempts")        
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
        # total number of questions per mcq test is independent
        #print("max mcq score")
        #query = "select mcq,  max(score) as maxscore from mcq_score where " + condqry + " group by courseid, mcq;"
        query = "select mcq,  max(`max`) as maxscore from mcq_score where " + condqry + " group by courseid, mcq;"
        df = rds_df(query)
        df.columns = ['mcq' , 'maxscore']        
        mcqmaxscore = dict(zip( [x for x in df.mcq] , [x for x in df.maxscore] ))
        
        # reset mcq scores 
        #print("reset mcq scores")
        updqry = "update userdata set " + ','.join([ "mcq_avg" + str(x) + " = 0"  for x in range( 1, max_mcq + 1 )]) + " where "
        updqry += condqry        
        rds_update(updqry)
        
        # update on  mcq scores ( not average scores )
        df = rds_df("SELECT studentid, mcq, score  from mcq_score WHERE mcq >0 AND " + condqry)
        if df is None:
            return
        df.columns = ['studentid' , 'mcq', 'score']       
        
        df1 = pd.pivot_table(df, values='score', index=['studentid'],columns='mcq')
        df1 = df1.fillna(0)        
        cols = list(df1.columns)
        cnt = len(cols)        
        for index, row in df1.iterrows():            
            list1 = list(row)            
            updqry = ""    
            for n in range(cnt):
                try:
                    m = mcqmaxscore[n+1]
                except:
                    m = 0
                if m == 0 :
                    list1[n] = 0
                else:
                    list1[n] = list1[n] / m                
                m = n + 1
                updqry += ",mcq_avg" + str(m) + " = " + str(list1[n])
            updqry = "update userdata set " + updqry[1:] + " where studentid = " + str(index) + " and " + condqry
            rds_update(updqry)  
    #except:
        #pass
    #print("update_mcq completed for course_id " + course_id)
    return

def edx_import(course_id, client_name):
    # Status : Tested       
    cohort_id = piece(piece(course_id,':',1),'+',1)
    module_code = piece(cohort_id,'-',0)
    condqry = f"client_name = '{client_name}' and courseid = '{course_id}';"
    qry = f"select `pillar` from `omnimentor`.`course_module` where `client_name` = '{client_name}' and `module_code` = '{module_code}';"
    module_id = rds_param(qry)
    if module_id=="":  # either incomplete course and just a course header        
        return 
    
    course_name = edx_coursename(course_id)    
    update_playbooklist(course_id, client_name, course_name)
    
    cnt = rds_param("select count(*) as cnt from stages where client_name='{client_name}' and courseid='{course_id}';")
    cnt = int("0" + str(cnt))
    if cnt==0:
        qry = f"select * from `omnimentor`.`stages_master` where `client_name` = '{client_name}' and `module_code` = '{module_code}';"
        df = rds_df(qry)        
        if df is not None:
            df.columns = get_columns("stages_master")
            query = "delete from stages where " + condqry
            rds_update(query)
            df.drop(columns=['module_code'], inplace=True)
            #for fld in ['stagedate','fcdate','eldate','mcqdate','asdate']:
            #    df[fld] = ''
            df['stagedate'] = ''                
            df['courseid'] = course_id
            df1 = df[['client_name','courseid', 'id', 'stage', 'name', 'desc', 'days', 'f2f', 'mcq', 'flipclass', 'assignment', 'IU', 'stagedate']]
            copydbtbl(df1, "stages")
            
    df = edx_userdata(course_id)
    nrows = len(df)
    if nrows == 0:
        return 

    query = "delete from userdata where " + condqry
    rds_update(query)
    
    #query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND STR_TO_DATE(startdate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
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
    df.rename(columns={'course_id':'courseid','student_id':'studentid'} , inplace=True)
    #df1 = df[['client_name', 'module_id', 'courseid', 'studentid', 'username', 'amt', 'grade', 'stage', 'f2f']]
    df1 = df[['client_name', 'module_id', 'courseid', 'studentid', 'username', 'amt', 'grade', 'stage', 'f2f', 'risk_level']]
    copydbtbl(df1,"userdata")
    
    mcqcnt = edx_mcqcnt(course_id)
    ascnt = edx_ascnt(course_id)
    try:
        mlist = []
        list1 = [ "mcq_avg"+ str(x) + " = 0" for x in range( 1, 13 + 1 )]
        list2 = [ "mcq_attempts"+ str(x) + " = 0" for x in range( 1, 13 + 1 )]
        list3 = [ "as_avg"+ str(x) + " = 0" for x in range( 1, 13 + 1 )]
        list4 = [ "as_attempts"+ str(x) + " = 0" for x in range( 1, 13 + 1 )]
        mlist += list1 + list2 + list3 + list4
        query = "update userdata set " + ','.join(mlist) + " where " + condqry
        rds_update(query)
        if mcqcnt > 0:
            mlist += list1 + list2
        if ascnt > 0:
            mlist += list1 + list2
    except:
        #print("error reset the mcq & assignment variables")
        return 
    
    if (mcqcnt + ascnt) > 0:
        update_assignment(course_id, client_name)
        update_mcq(course_id, client_name)
    update_schedule(course_id, client_name)    
    return 

def count_avg_cols(client_name, course_id, maxmcqtest = 13, colname = "mcq_avg"):    
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
        #print("Nothing to process.")
        return None
    print(course_id)        
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
            print("Error update mcqas_info on ", xvar)        

    try:
        df1 = df[["client_name", "course_id","studentid","grade","mcq_avgscore","mcq_avgattempts", \
             "mcq_maxattempts","mcq_cnt","as_avgscore","as_avgattempts","as_maxattempts","as_cnt"]]
        return df1
    except:
        return None

def update_mcqas_info(client_name, course_id):
    iu_cnt = 13
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
            #print(f"there is no data for {course_id}")
            return
        copydbtbl(df,"mcqas_info")
    #print("update_mcqas_info has been completed")
    return

def generate_mcq_as(client_name):
    df = rds_df("select distinct course_id from playbooks where client_name = '" + client_name + "';")
    df.columns = ['course_id']
    course_list = [x for x in df.course_id]
    print("# of cohorts = ", len(course_list))    
    print('Generating data based on cohort id :')        
    for course_id in course_list:
        try:
            #print(client_name, course_id)            
            update_mcqas_info(client_name, course_id)
        except:
            pass
    print("generate_mcq_as completed for all cohorts")        
    return

def update_userdf(userdf, client_name):
    # Status : Tested
    df = rds_df(f"select * from user_master where client_name='{client_name}';" )
    if df is None:
        ut_dict = dict()
        cs_dict = dict()
        ct_dict = dict()
        bd_dict = dict()
    else:
        #df.drop_duplicates(keep=False,inplace=True)
        df.columns = get_columns("user_master")
        sl = [x for x in df.studentid]
        ut = [x for x in df.usertype]
        cs = [x for x in df.courseid]
        ct = [x for x in df.chat_id]
        bd = [x for x in df.binded]
        ut_dict = dict(zip(sl,ut))
        cs_dict = dict(zip(sl,cs))
        ct_dict = dict(zip(sl,ct))
        bd_dict = dict(zip(sl,bd))
        for x in list(cs_dict): 
            if cs_dict[x]=='':    cs_dict.pop(x)
        for x in list(ct_dict):
            if ct_dict[x]==0:    ct_dict.pop(x)
        for x in list(bd_dict):
            if bd_dict[x]==0:    bd_dict.pop(x)
    # eliminate duplicates    
    query = "select distinct client_name, studentid, max(username) as username, max(email) AS email, "
    query += "max(usertype) AS usertype, max(chat_id) AS chat_id, max(courseid) as course_id from user_master "
    query += f"where client_name = '{client_name}' group by client_name, studentid;"
    df1 = rds_df(query)
    if df1 is not None:
        if len(df) == len(df1):
            print("no duplicates found.")
        else:
            query = f"Delete FROM user_master WHERE client_name = '{client_name}';"
            rds_update(query)
            copydbtbl(df1,"user_master")
            
    df = userdf    
    df.rename(columns={'course_id':'courseid','student_id':'studentid'} , inplace=True)
    df['client_name'] = client_name 
    # no more hardcoding email filter to identify admin
    email_filter = rds_param(f"SELECT `value` from params WHERE  `key` = 'email_filter' and client_name = '{client_name}';")
    efilter = email_filter.split(',')    
    sl2 = [x for x in df.studentid]
    cs2 = [x for x in df.courseid]
    ut2_dict = dict()
    cs2_dict = dict(zip(sl2,cs2))
    ct2_dict = dict()
    bd2_dict = dict()
    for x in list(cs_dict):    cs2_dict[x] = cs_dict[x]
    for x in sl2:    ct2_dict[x] = ct_dict[x] if x in list(ct_dict) else 0
    for x in sl2:    ut2_dict[x] = ut_dict[x] if x in list(ut_dict) else 0
    for x in sl2:    bd2_dict[x] = bd_dict[x] if x in list(bd_dict) else 0
    df['courseid'] = df.apply(lambda x: cs2_dict[x['studentid']], axis=1)
    df['chat_id'] = df.apply(lambda x: ct2_dict[x['studentid']], axis=1)
    df['binded'] = df.apply(lambda x: bd2_dict[x['studentid']], axis=1)
    df['usertype'] = df.apply(lambda x: 11 if x['email'].lower().split('@')[1] in efilter else 1, axis=1)
    df = df [['client_name','studentid','username','email','usertype','binded','chat_id','courseid']] 
    return df

def get_calendar_json(api_url):
    response = requests.get(api_url)
    if response.status_code==200:
        data = json.loads(response.content.decode('utf-8'))    
    else:
        data = {}
    return data

def get_stage_list(data):
    if data == {}:
        return []
    resp = data['events']
    if len(resp)==0:
        return []       
    module_node = resp[0]['summary'].split(' : ')[0]    
    stage_list = []
    soc_date = resp[0]['startDate'][:10] 
    socd = dmy_str(soc_date)    
    socdt = ymd_str(soc_date)
    s_dict = {}
    s_list = []    
    for x in resp:
        if 'summary' not in list(x):
            continue
        if 'description' not in list(x):
            continue
        x_summary = x['summary']        
        x_desc = x['description']        
        x_list = x_summary.split(' : ')
        n=len(x_list)        
        cohort = x_list[0] if n>0 else ""
        stage_name = x_list[1] if n>1 else ""
        stage_desc = x_list[2] if n>2 else stage_name
        stage_desc = stage_desc.replace('?','')
        iu_list = "0"
        if x_desc is not None:
            if "IU " in x_desc:
                iu_list = ','.join([ x.split(':')[0].split(' ')[1] for x in x_desc.split('>') if "IU " in x])        
        stdate = ymd_str(x['startDate'][:10])
        sdate = dmy_str(x['startDate'][:10])
        edate = dmy_str(x['endDate'][:10])        
        stage_name = stage_code(stage_name)
        if stage_name != "" :
            stage_info = [stage_name, cohort, stage_desc, sdate, edate, iu_list]
            s_dict[stdate] = stage_info
            s_list.append(stdate)
    if s_list==[]:
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
            #cohort_dict[x][y].append((str2date(cohort_dict[x][y][3]) - dt0).days + 1)
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
    #stg = "SA1" if stg=="SA" else stg
    return stg

def get_google_calendar(course_id, client_name):
    cohort_id = piece(piece(course_id,':',1),'+',1)
    module_code = cohort_id.split('-')[0]
    course_code = rds_param(f"select `course_code` from `module_iu` where `module_code` = '{module_code}' and client_name='{client_name}' limit 1;")
    # direct_cohort_url = f"https://realtime.sambaash.com/v1/calendar/fetch?cohortId={cohort_id}"
    # multi_cohorts_url = "https://realtime.sambaash.com/v1/calendar/fetch?cohortId=EIT-0219A/EIT-0119B"
    # single_cohort_url = "https://realtime.sambaash.com/v1/calendar/fetch?cohortId=EIT%20:%20ICO-0520A"
    try:
        api_url = f"https://realtime.sambaash.com/v1/calendar/fetch?cohortId={course_code}%20:%20{cohort_id}"    
        #print(api_url)
        data  = get_calendar_json(api_url)
    except:
        data = {}
    if data == {}:
        try:
            cohort = cohort_id.replace("FOS","EIT")
            api_url = f"https://realtime.sambaash.com/v1/calendar/fetch?cohortId={cohort}"
            data  = get_calendar_json(api_url)
        except:
            data = {}        
    if data == {}:
        #print("there is no data from google calendar")
        return []
    
    sorted_stage_list =  get_stage_list(data)
    stage_list = []
    if cohort_id in list(sorted_stage_list) :
        stage_list = sorted_stage_list[cohort_id]
    return stage_list

def update_stage_table(stage_list, course_id, client_name):
    # Status : Tested
    qry = " client_name = '_c_' and courseid = '_x_';"
    qry = qry.replace('_c_', client_name)
    qry = qry.replace('_x_', course_id)    
    mcq_dict = {}
    ast_dict = {}    
    for x in stage_list:
        mcq_dict[ x[0] ] = ""
        ast_dict[ x[0] ] = ""
    query = "select * from stages where " + qry
    df = rds_df(query)
    mcq_dict = {}
    ast_dict = {}
    stg_list = []    
    if (df is None) or (len(stage_list)==0):        
        cohort_id = piece(piece(course_id,':',1),'+',1)
        module_code = piece(cohort_id,'-',0)
        query = f"select * from `omnimentor`.`stages_master` where `client_name` = '{client_name}' and `module_code` = '{module_code}';"
        df = rds_df(query)
        if df is None:
            return []
        df.columns = get_columns("stages_master")
        stg_list = [x for x in df.stage]
        stgname_list = [x for x in df.name]
        stgdesc_list = [x for x in df.desc]
        stgdays_list = [x for x in df.days]
        iu_list = [x for x in df.IU]
        mcq_list = [x for x in df.mcq]
        ast_list = [x for x in df.assignment]
        mcq_dict = dict(zip(stg_list,mcq_list))
        ast_dict = dict(zip(stg_list,ast_list))
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
        mcq_dict = dict(zip(stg_list,mcq_list))
        ast_dict = dict(zip(stg_list,ast_list))

    m = len(stage_list)    
    if m==0:
        print("stage_list is empty")
        return []
        
    for x in stage_list:
        #print(x)
        if x[0] not in stg_list:
            mcq_dict[ x[0] ] = "0"
            ast_dict[ x[0] ] = "0"
    
    for x in stage_list:
        #print(x)
        x.append( mcq_dict[x[0]] )
        x.append( ast_dict[x[0]] )
        
    query = "delete from stages where " + qry
    rds_update(query)
    
    n = 0
    for n in range(len(stage_list)):
        stg = stage_list[n][0]
        query = "insert into stages(client_name, courseid, id, stage, `name` , `desc` ,days, f2f, stagedate, startdate,"
        query += "mcq, assignment, IU, flipclass) values("
        query += "'" + client_name + "','" + course_id + "'," + str(n+1)
        query += ",'" + str(stage_list[n][0]) + "','"  
        query += str(stage_list[n][1]) + "','"  
        query += str(stage_list[n][2]) + "',"   
        query += str(stage_list[n][6]) + ",0,'','','" 
        query += str(stage_list[n][7]) + "','"   
        query += str(stage_list[n][8]) + "','" 
        query += str(stage_list[n][5])  + "','" + str(stage_list[n][5]) + "')"         
        rds_update(query)
    
    arr_stagedate = [ x[3] for x in stage_list]
    m = len(arr_stagedate)    
    if m==0:
        print("There is not enough information to build the schedule")
        return []
    for n in range(m - 1):
        id = n + 1
        start_date = arr_stagedate[n] 
        stg_date = arr_stagedate[id] 
        query = f"update stages set startdate = '{start_date}', stagedate = '{stg_date}' where id = {id} and " + qry        
        try:
            rds_update(query)
        except:
            print(query)
    
    start_date = arr_stagedate[-1] 
    eoc_date = string2date(stage_list[-1][4],"%d/%m/%Y")
    stg_date = eoc_date.strftime('%d/%m/%Y')
    query = f"update stages set startdate = '{start_date}',stagedate = '{stg_date}' where id = {m} and " + qry
    try:
        rds_update(query)           
    except:
        print(query)
        
    query = f"SELECT `name` FROM stages WHERE client_name = '{client_name}' AND courseid='{course_id}' AND STR_TO_DATE(stagedate,'%d/%m/%Y') <= CURDATE() ORDER BY id DESC LIMIT 1;"
    stage = rds_param(query)                
    if stage != "":
        query=f"update userdata set stage='{stage}' WHERE client_name = '{client_name}' AND courseid='{course_id}' ;"
        try:            
            rds_update(query)
        except:
            print(query)
    return stage_list

def update_playbooklist(course_id, client_name, course_name):
    try:
        query = f"delete from playbooks where client_name = '{client_name}' and course_id = '{course_id}';"
        rds_update(query)
        cohort_id = piece(piece(course_id,':',1),'+',1)
        module_code = piece(cohort_id,'-',0)
        query = "insert into playbooks(client_name,module_code,cohort_id,course_id,course_name) values('_c_', '_w_', '_x_', '_y_', '_z_');"                
        query = query.replace("_c_", client_name)
        query = query.replace("_w_", module_code)
        query = query.replace("_x_", cohort_id)
        query = query.replace("_y_", course_id)
        query = query.replace("_z_", course_name)
        rds_update(query)
    except:
        #print("Unable to update playbook list")
        pass
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
    qry = f"SELECT DISTINCT module_code FROM course_module WHERE client_name='{client_name}';"    
    df = rds_df(qry)
    if df is None:
        mc_list = []
    else:
        df.columns = ['module_code']
        mc_list = [x for x in df.module_code]
        course_list = [ x for x in course_list if module_code(x) in mc_list ]
    for course_id in course_list:        
        #if 'v1:lithan' in course_id.lower():        
        eoc = edx_endofcourse(client_name, course_id)
        #print( course_id , eoc)
        if eoc == 0:
            #print( "processing ", course_id )
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
        print("Failed to perform user master update")
        return
    #query = "DELETE um.* FROM user_master um INNER JOIN userdata ud ON um.client_name=ud.client_name"
    #query += f" AND um.studentid=ud.studentid where um.client_name = '{client_name}';"        
    query = f"delete from user_master where client_name = '{client_name}';"    
    rds_update(query)
    copydbtbl(df,"user_master")    
    #
    print(f"mass_update_usermaster completed for {client_name}")        
    return

#def test_get_stage_list():
#    api_url="https://realtime.sambaash.com/v1/calendar/fetch?cohortId=ERI-0220A"
#    api_url = "https://realtime.sambaash.com/v1/calendar/fetch?cohortId=EIT-0219A/EIT-0119B"
#    api_url="https://realtime.sambaash.com/v1/calendar/fetch?cohortId=EIT%20:%20ICO-0520A"             
#    api_url="https://realtime.sambaash.com/v1/calendar/fetch?cohortId=EIT%20:%20FOS-0620A"
#    data  = get_calendar_json(api_url)
#    course_stagelist = get_stage_list(data)      
#    for cohort_id in list(course_stagelist):
#        print(cohort_id)
#        stage_list = course_stagelist[cohort_id]        
#        for x in stage_list:
#            print(x)
#        print("="*50,"\n")    
#    return

def test_google_calendar(course_id):
    client_name = "Sambaash"
    #course_id = "course-v1:Lithan+FOS-1219A+04Dec2019"
    #course_id = 'course-v1:Lithan+FOS-0620A+17Jun2020'
    stage_list = get_google_calendar(course_id, client_name)
    for x in stage_list:
        print(x)
    print("\n")
    return stage_list

def edx_alluserdata():
    # Status : open
    # url = f"{edx_api_url}/course/fetch/all"
    # https://om.sambaash.com/edx/api/swagger-ui/index.html?configUrl=/edx/v3/api-docs/swagger-config#/Course/fetchAllUsers
    # https://om.sambaash.com/edx/v1/user/fetch/all
    global edx_api_header, edx_api_url
    df = pd.DataFrame.from_dict({'student_id':[],'course_id':[],'username':[],'email':[]})
    url = f"{edx_api_url}/user/fetch/all"
    response = requests.get(url, headers=edx_api_header)      
    if response.status_code==200:
        userinfo = json.loads(response.content.decode('utf-8'))    
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
    print( mcqcnt )
    print("====== test case 5 edx_ascnt ==========")
    ascnt = edx_ascnt(course_id)
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
    test_google_calendar(course_id)
    print("test_google_calendar completed")    
    #print("====== test case 13 edx_import ==========")    
    #edx_import(course_id,  client_name)
    print( f"edx_import {course_id},  {client_name}")
    #print("====== test case 14 generate_mcq_as ==========")
    #generate_mcq_as(client_name)
    #print("generate_mcq_as completed")    
    #print("====== test case 15 mass_update_usermaster ==========")
    #mass_update_usermaster(client_name)
    #print("mass_update_usermaster completed")
    #print("====== test case 16 edx_alluserdata ==========")
    #df = edx_alluserdata()
    #print( df[:5] )
    #print("edx_alluserdata completed")
    print("end of unit tests 1 - 12")
    return

if __name__ == "__main__":    
    global use_edxapi, edx_api_header, edx_api_url
    with open("vmbot.json") as json_file:  
        bot_info = json.load(json_file)
    client_name = bot_info['client_name']
    #edx_api_url = "https://om.sambaash.com/edx/v1"
    edx_api_url = "https://omnimentor.lithan.com/edx/v1"
    edx_api_header = {'Authorization': 'Basic ZWR4YXBpOlVzM3VhRUxJVXZENUU4azNXdG9E', 'Content-Type': 'text/plain'}
    client_name = "Sambaash"    
    #client_name = "Lithan"    
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    course_id = "course-v1:Lithan+FOS-0620A+17Jun2020" # 6116
    #course_id = "course-v1:Lithan+ICO-0520A+15Jul2020"
    #course_id = "course-v1:Lithan+FOS-0520A+06May2020"  # 5709
    sid = 6464
    attendance_dates = sms_attendance(course_id, sid)
    print(list(attendance_dates))
    missing_dates = sms_missingdates(client_name, course_id, sid)
    print(missing_dates)
    #print(course_id, sid, f2f)
    #edx_daystart = edx_day0(course_id)
    #print( edx_daystart ) #2020-05-05
    #test_google_calendar(course_id)
    #update_schedule(course_id, client_name)
    #query = f"SELECT * FROM stages WHERE courseid = '{course_id}' AND client_name = '{client_name}';"
    #df = rds_df(query)
    #if df is not None:
    #    df.columns = get_columns("stages")
    #    df1 = df[['id' , 'name' , 'desc', 'startdate', 'stagedate', 'days']]
    #    print(df1)
    #
    #=====================================
    #mass_update_schedule(client_name)
    #perform_unit_tests(client_name, course_id, sid)
    #=====================================
    #mass_update_usermaster(client_name)
    #
    #print("check user_master")
    print("This is vmedxlib.py")
