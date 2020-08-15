#
#  ______                        __ __       __                     __
# /      \                      |  \  \     /  \                   |  \
#|  ▓▓▓▓▓▓\______ ____  _______  \▓▓ ▓▓\   /  ▓▓ ______  _______  _| ▓▓_    ______   ______
#| ▓▓  | ▓▓      \    \|       \|  \ ▓▓▓\ /  ▓▓▓/      \|       \|   ▓▓ \  /      \ /      \
#| ▓▓  | ▓▓ ▓▓▓▓▓▓\▓▓▓▓\ ▓▓▓▓▓▓▓\ ▓▓ ▓▓▓▓\  ▓▓▓▓  ▓▓▓▓▓▓\ ▓▓▓▓▓▓▓\\▓▓▓▓▓▓ |  ▓▓▓▓▓▓\  ▓▓▓▓▓▓\
#| ▓▓  | ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓  | ▓▓ ▓▓ ▓▓\▓▓ ▓▓ ▓▓ ▓▓    ▓▓ ▓▓  | ▓▓ | ▓▓ __| ▓▓  | ▓▓ ▓▓   \▓▓
#| ▓▓__/ ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓  | ▓▓ ▓▓ ▓▓ \▓▓▓| ▓▓ ▓▓▓▓▓▓▓▓ ▓▓  | ▓▓ | ▓▓|  \ ▓▓__/ ▓▓ ▓▓
# \▓▓    ▓▓ ▓▓ | ▓▓ | ▓▓ ▓▓  | ▓▓ ▓▓ ▓▓  \▓ | ▓▓\▓▓     \ ▓▓  | ▓▓  \▓▓  ▓▓\▓▓    ▓▓ ▓▓
#  \▓▓▓▓▓▓ \▓▓  \▓▓  \▓▓\▓▓   \▓▓\▓▓\▓▓      \▓▓ \▓▓▓▓▓▓▓\▓▓   \▓▓   \▓▓▓▓  \▓▓▓▓▓▓ \▓▓
#
# Library functions by KH
#------------------------------------------------------------------------------------------------------
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore")

import pandas as pd
import pandas.io.formats.style

import os, re, sys, time, datetime, string, random
import subprocess
import matplotlib.pyplot as plt
import matplotlib.ticker as mtk

import requests

import telepot
import asyncio
import telepot.aio
from telepot.aio.loop import MessageLoop
from telepot.aio.delegate import pave_event_space, per_chat_id, create_open

import wget
import json

import vmnlplib
import vmaiglib
#import vmffnnlib
import vmmcqdlib
import vmsvclib
import vmedxlib
from vmsvclib import *

global bot_intance, vmbot, ft_model, dt_model, nn_model, mcq_analysis
global rdscon, rds_connstr, edx_api_header, edx_api_url

# following will be replaced by generic config without hardcoded.
debug_mode = False
use_mailapi = False  # only available when using aws host
btn_hellobot = "Hello OmniMentor 👩‍🎓 👨‍🎓🤖"
option_back = "◀️"
option_import = "Import 📦"
option_export = "Export 💾"
option_mainmenu = "mainmenu"
option_learners = "Learners 👩"
option_faculty = "Faculty"
mainmenu = [[option_learners, option_faculty , option_back]]
option_mycourse = "My courses"
option_updateprogress = "Update progress"
option_faq = "FAQ"
option_mychat = "LiveChat"
option_mychart = "Chart"
option_binduser = "Auto Sign-in"
option_bind = "Binding"
option_gethelp = "Contact me"
option_info = "Info"
lrn_start = "Learner Started"
lrn_student = "Learner Verified"
learners_menu = [[option_mycourse, option_updateprogress, option_mychart],\
    [option_gethelp, option_mychat, option_faq], [option_binduser, option_info, option_back]]
option_fct = "Faculty Admin 📚"
option_pb = "Playbooks 📗📘📙"
option_analysis = "Analysis 📊"
option_chat = "Chat 💬"
option_sos = "Help 🆘"
option_chart = "Chart 📊"
option_chatlist = "Chat List"
option_chatempty = "Chat Empty"
option_bindadm = "Auto Sign-in 🔐"
option_usermgmt = "Manage Users 👥"
mentor_menu = [[option_fct, option_pb, option_analysis], [option_chat, option_usermgmt, option_back]]
option_searchbyname = "Name Search"
option_searchbyemail = "Email Search"
option_resetuser = "Reset User"
option_admin_users = "Admin Users"
option_active_users = "Active Users"
option_blocked_users = "Blocked Users"
option_binded_users = "Binded Users"
users_menu = [[option_searchbyname, option_searchbyemail, option_resetuser, option_active_users], \
    [option_admin_users, option_binded_users, option_blocked_users, option_back]]
opt_blockuser = 'Block this user'
opt_setadmin = 'Set as Admin'
opt_setlearner = 'Set as Learner'
opt_resetemail = 'Change Email'
opt_unbind = 'Reset Binding'
useraction_menu = [[opt_blockuser, opt_setadmin , opt_setlearner],[opt_resetemail, opt_unbind, option_back]]
fc_userimport = "User Import"
fc_edxupdate = "LMS Import"
fc_schedule = "Schedule Update"
fc_intv = "Intervention Msg"
fc_notf = "Reminder Msg"
# this is for UAT
faculty_menu = [[fc_schedule, fc_userimport, fc_edxupdate], [fc_intv, fc_notf, option_back]]
# this is for production
#faculty_menu = [[fc_schedule, fc_userimport, fc_edxupdate, option_back]]
opt_stage = "Edit Stage Cohorts"
opt_updstage = "Stage Update Cohorts"
pb_config = "Configurator Playbook 📗"
pb_userdata = "Persona Playbook 📙"
pb_riskuser = "Learners at risk"
playbook_menu= [[pb_config, pb_userdata, pb_riskuser, option_back]]
ps_userdata = "Userdata"
ps_schedule = "Schedule 📅"
ps_stage = "Unit Guides"
ps_mcqzero = "MCQ Pending"
ps_mcqfailed = "MCQ Failed"
ps_aszero = "Assignment Pending"
ps_asfailed = "Assignment Failed"
course_menu = [[ps_userdata, ps_schedule, ps_stage, ps_mcqzero],[ps_mcqfailed, ps_aszero, ps_asfailed, option_back]]
an_mcq = "MCQ Analysis"
an_chart = "Graph"
an_mcqd = "MCQ Diff. Analysis"
ml_grading = "AI Grading"
opt_aig = "AI Grad Cohorts"
analysis_menu = [[ml_grading, an_mcq, an_mcqd, an_chart, option_back]]
an_mcqavg = "By MCQ Average"
an_avgatt = "By MCQ Attempts"
an_avgscore = "By MCQ Scores"
opt_mcqd = "MCQ Diff Cohorts"
opt_pbusr = "Playbook Cohorts"
opt_mcqavg = "MCQ Avg Cohorts"
opt_analysis = "Analysis Cohorts"
mcqdiff_menu = [[an_avgatt,an_avgscore,an_mcqavg,option_back]]

#gen2fa = lambda : (''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(32))).upper()
string2date = lambda x,y : datetime.datetime.strptime(x,y).date()
piece = lambda txtstr,seperator,pos : txtstr.split(seperator)[pos]
module_code = lambda x : piece(piece(piece(x,':',1),'+',1),'-',0)
sorted_numlist = lambda y : list(set([int(x) for x in ''.join([z for z in y if z.isnumeric() or z==',']).split(',') if x!='']))
iu_reading = lambda z: ','.join([ str(x+1) for x in range(max(z))])
#callgraph = lambda x : vmsvclib.callgraph(x)
#debug = lambda x : vmsvclib.debug(x)

def do_main():
    global vmbot, bot_intance, dt_model, nn_model
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    if not loadconfig():
        print("error loading config")
        return
    if dt_model.model_name=="" :
        txt = "AI grading model data file dt_model.bin is missing"
        syslog('system', txt)
    #elif nn_model.model_name=="" :
    #    txt = "AI grading model data file ffnn_model.hdf5 is missing"
    #    syslog('system', txt)
    bot_intance = BotInstance()    
    loop = asyncio.get_event_loop()
    loop.create_task(MessageLoop(bot_intance.bot).run_forever())
    loop.create_task(job_scheduler())
    loop.run_forever()
    return 

async def job_scheduler():
    global bot_intance,vmbot
    client_name = bot_intance.client_name
    resp_dict = bot_intance.resp_dict
    pass_rate = bot_intance.pass_rate
    adminchatid = bot_intance.adminchatid
    edx_cnt = 0
    edx_time = bot_intance.edx_time  # currently  2200 Sgp time    
    automsg = bot_intance.automsg   # currently 0900 Sgp time
    gmt = bot_intance.gmt  # azure version : gmt = 8
    while True :
    #        zz = """
        checkjoblist()
        timenow = time_hhmm(gmt)
        if (timenow==automsg) and (automsg>0):
            print("running auto_notify")
            await auto_notify(client_name, resp_dict, pass_rate, adminchatid)
            print("completed auto_notify")
            print("running auto_intervent")
            await auto_intervent(client_name, resp_dict, pass_rate, adminchatid)
            print("completed auto_intervent")
            await asyncio.sleep(60)
        if (edx_time > 0) and (timenow==edx_time) and (edx_cnt==0) :
            edx_cnt = 1
            print("running load_edxdata")
            load_edxdata()
            await asyncio.sleep(60)
            print("completed with load_edxdata")
        if (edx_time > 0) and (timenow > edx_time) and (edx_cnt==1):
            edx_cnt = 0
        #"""
        await asyncio.sleep(1)
    return

def load_edxdata():
    #async def load_edxdata():
    #async asyncio.sleep(1)
    global vmbot, bot_intance
    client_name = bot_intance.client_name
    syslog(client_name,"load_edxdata started")
    date_today = datetime.datetime.now().date()
    sub_str  = bot_intance.sub_str
    yrnow = str(date_today.strftime('%Y'))
    #query = f"SELECT DISTINCT courseid FROM userdata WHERE client_name = '{client_name}' "
    #query += f" AND {sub_str}(courseid,-4)='{yrnow}' ORDER BY courseid;"
    query = f"SELECT DISTINCT u.courseid FROM userdata u INNER JOIN playbooks p "
    query += f" ON u.client_name=p.client_name AND u.courseid=p.course_id "
    query += f"WHERE u.client_name = '{client_name}' AND p.eoc=0 AND {sub_str}(u.courseid,-4)='{yrnow}';"
    df = rds_df(query)
    if df is None:
        course_list = []
    else:
        df.columns = ['courseid']
        course_list= [x for x in df.courseid]

    # update existing courses on mcq,attempts,stage schedule
    updated_courses = []
    vars = dict()
    txt =  ""
    efilter = bot_intance.efilter
    for course_id in course_list:
        eoc = vmedxlib.edx_endofcourse(client_name, course_id)
        eoc_gap = vmedxlib.edx_eocgap(client_name, course_id, 7)
        if (eoc == 0) or (eoc_gap==1):
            vmedxlib.update_mcq(course_id, client_name)
            vmedxlib.update_assignment(course_id, client_name)
            vmedxlib.update_schedule(course_id, client_name)
            updated_courses.append(course_id)
        if eoc == 1:
            query = "update playbooks set eoc=1 where client_name='{client_name}' AND course_id='{course_id}';"
            rds_update(query)

    course_list = vmedxlib.search_course_list(yrnow)
    #print("edx_import")
    for course_id in [ x for x in course_list if x not in updated_courses]:
        #print(course_id)
        vmedxlib.edx_import(course_id, client_name)
        updated_courses.append(course_id)

    #print("mass_update_usermaster")
    vmedxlib.mass_update_usermaster(client_name)
    #print("load_edxdata completed")
    syslog(client_name,"load_edxdata completed")
    return

async def auto_notify(client_name, resp_dict, pass_rate, adm_chatid):
    global vmbot,bot_intance
    await bot_intance.bot.sendMessage(adm_chatid, "auto_notify started")
    # T -1 days currently, to be generic next time
    qry = "SELECT a.* FROM stages a INNER JOIN playbooks b ON a.client_name=b.client_name AND a.courseid=b.course_id "
    qry += "INNER JOIN course_module c ON b.client_name=c.client_name AND b.module_code=c.module_code WHERE c.enabled=1 and "
    qry += f"a.client_name = '{client_name}' AND "
    if ('.db' in vmsvclib.rds_connstr):
        qry += f"strftime(substr(a.startdate,7,4)||'-'||substr(a.startdate,4,2)||'-'||substr(a.startdate,1,2))=strftime(date('now','+1 day'));"
    else:
        qry += f"STR_TO_DATE(a.startdate,'%d/%m/%Y') = DATE_ADD(CURDATE() , INTERVAL 1 DAY);"
    stagedf = rds_df(qry)
    if stagedf is None:        
        return
    mdf = rds_df(f"SELECT * FROM user_master where client_name='{client_name}';")
    if mdf is None:        
        return
    s_cols = bot_intance.stages_cols
    u_cols = bot_intance.userdata_cols
    mdf.columns = bot_intance.usermaster_cols
    stagedf.columns = s_cols
    course_list = [x for x in stagedf.courseid]
    stg_list = [x for x in stagedf.stage]
    stgname_list = [x for x in stagedf.name]
    stgdesc_list = [x for x in stagedf.desc]
    mcqvars_list = [x for x in stagedf.mcq]
    asvars_list = [x for x in stagedf.assignment]
    iunum_list = [x for x in stagedf.IU]
    duedate_list = [x for x in stagedf.stagedate]
    count = len(course_list)
    if count == 0:        
        return
    syslog(client_name,"auto_notify started")
    efilter = bot_intance.efilter
    notif0 = resp_dict['notif0']
    notif1 = resp_dict['notif1']
    notif2 = resp_dict['notif2']
    for n in range(count):
        course_id = course_list[n]        
        stagecode = stg_list[n]
        stagename = stgname_list[n]
        stagedesc = stgdesc_list[n]
        stagedesc = stagedesc.replace('?','')

        due_date = duedate_list[n]
        mcqvars = mcqvars_list[n]
        asvars = asvars_list[n]
        query = f"select * from stages where client_name='{client_name}' and courseid='{course_id}';"
        sdf = rds_df(query)
        if sdf is None:
            continue
        sdf.columns = s_cols

        query = f"select * from userdata where client_name='{client_name}' and courseid='{course_id}';"
        udf = rds_df(query)
        if udf is None:
            continue
        udf.columns = u_cols

        ulist = [x for x in udf.studentid]
        uname_list = [x for x in udf.username]
        udict = dict(zip(ulist,uname_list))
        err_msg = ""
        err_list = []
        sent_list = []
        for sid in ulist:
            uname = udict[sid]
            df1 = udf[ udf.studentid == sid ].copy()
            (tt, vars) = verify_student(client_name, df1, sid, course_id, sdf)
            df2 = mdf[ mdf.studentid == sid ].copy()
            email = df2.email.values[0]
            if email.lower().split('@')[1] in efilter:
                del df1, df2
                continue
            usrtyp = df2.usertype.values[0]
            if usrtyp != 1:
                del df1, df2
                continue
            tid = df2.chat_id.values[0]
            binded = df2.binded.values[0]
            tid = 0 if binded==0 else tid
            amt = vars['amt']
            (pass_stage, has_score, avg_score, mcqas_list, max_attempts, list_attempts, mcq_avg, mcq_zero, mcq_pass, mcq_failed, mcq_attempts, mcnt, \
            mcq_att_balance, as_avg, as_zero, as_pass, as_failed, as_attempts, acnt, as_att_balance, mcqas_complete, risk_level, tt) \
                = get_stageinfo(vars, pass_rate, amt, stagecode, mcqvars, asvars)
            mcq_iu_list = '' if mcq_zero == [] else iu_reading(mcq_zero)
            as_iu_list = '' if as_zero == [] else iu_reading(as_zero)
            txt = notif0
            if len(mcq_zero) > 0:
                txt += notif1
            if len(as_zero) > 0:
                txt += notif2
            txt += "\n"
            if '{uname}' in txt:
                txt = txt.replace('{uname}', uname)
            if '{stagename}' in txt:
                txt = txt.replace('{stagename}', stagename)
            if '{stagedesc}' in txt:
                txt = txt.replace('{stagedesc}', stagedesc)
            if '{mcq_zero}' in txt:
                txt = txt.replace('{mcq_zero}', str(mcq_zero))
            if '{mcq_failed}' in txt:
                txt = txt.replace('{mcq_failed}', str(mcq_failed))
            if '{as_zero}' in txt:
                txt = txt.replace('{as_zero}', str(as_zero))
            if '{as_failed}' in txt:
                txt = txt.replace('{as_failed}', str(as_failed))
            if '{mcq_iu_list}' in txt:
                txt = txt.replace('{mcq_iu_list}', str(mcq_iu_list))
            if '{as_iu_list}' in txt:
                txt = txt.replace('{as_iu_list}', str(as_iu_list))
            if '{due_date}' in txt:
                txt = txt.replace('{due_date}', str(due_date))
            if '{lf}' in txt:
                txt = txt.replace('{lf}', "\n")

            if debug_mode:
                txt = course_id + '\t' + str(sid) + '\n' + txt
                await bot_intance.bot.sendMessage(adm_chatid, txt)
            else:
                #zz="""
                if tid <= 0:
                    err_list.append(sid)
                else:
                    try:
                        await bot_intance.bot.sendMessage(int(tid), txt)
                        sent_list.append(sid)
                    except:
                        err_list.append(sid)
                #"""
            del df1, df2

        msg = ""
        if len(sent_list) > 0:
            msg += f"Succesfully sent reminder to learners from {course_id} :\n{str(sent_list)} \n"
        if len(err_list) > 0:
            msg += f"Unable to send reminder to learners from {course_id} :\n{str(err_list)} \n"
        if len(msg) > 0:
            try:
                await bot_intance.bot.sendMessage(adm_chatid, msg)
            except:
                print(msg)
        syslog(client_name,msg)
    syslog(client_name, "auto_notify completed")
    await bot_intance.bot.sendMessage(adm_chatid, "auto_notify completed")
    return

async def auto_intervent(client_name, resp_dict, pass_rate, adm_chatid):
    global vmbot, bot_intance
    sub_str  = bot_intance.sub_str
    date_today = datetime.datetime.now().date()
    yrnow = str(date_today.strftime('%Y'))
    await bot_intance.bot.sendMessage(adm_chatid, "auto_intervent started")
    if debug_mode:
        # course_list = [ 'course-v1:Lithan+ITC-0320B+15Jul2020' ] # 5188, 5297 , 6866
        # course_list = [ 'course-v1:Lithan+FOS-0720A+08Jul2020' ] # 6686 6651
        # course_list = [ 'course-v1:Lithan+ICO-0620A+24Jul2020' ] # 6161 6464
        course_list = [ 'course-v1:Lithan+ICO-0520B+26Jun2020' ] # 1716
    else:
        #zz = """
        query = "SELECT DISTINCT a.courseid FROM userdata a "
        query += " INNER JOIN playbooks b ON a.client_name=b.client_name AND a.courseid=b.course_id "
        query += " INNER JOIN course_module c ON b.client_name=c.client_name AND b.module_code=c.module_code "
        query += f" WHERE c.enabled=1 and a.client_name = '{client_name}' "
        query += f" AND {sub_str}(courseid,-4)='{yrnow}' ORDER BY courseid;"
        df = rds_df(query)
        if df is None:
            course_list = []
        else:
            df.columns = ['courseid']
            course_list= [x for x in df.courseid]
        #"""

    mdf = rds_df(f"SELECT * FROM user_master where client_name='{client_name}';")
    if mdf is None:
        return
    mdf.columns = bot_intance.usermaster_cols
    syslog(client_name,"auto_intervent started")
    vars = dict()
    txt =  ""
    efilter = bot_intance.efilter
    s_cols = bot_intance.stages_cols
    u_cols = bot_intance.userdata_cols
    risk_level = 0
    notif3 = resp_dict['notif3']
    notif4 = resp_dict['notif4']
    notif5 = resp_dict['notif5']
    notif6 = resp_dict['notif6']
    notif7 = resp_dict['notif7']
    notif8 = resp_dict['notif8']
    notif9 = resp_dict['notif9']
    notif10 = resp_dict['notif10']
    notif11 = resp_dict['notif11']
    notif12 = resp_dict['notif12']
    resp8 = resp_dict['resp8']
    eoc7_maildict = dict()
    eoc7_userdict = dict()
    eoc7_mailcnt = 0
    for course_id in course_list:
        eoc = vmedxlib.edx_endofcourse(client_name, course_id) # ended=1 else 0
        soc = vmedxlib.edx_course_started(client_name, course_id)  # started=1 else 0
        eoc_gap = vmedxlib.edx_eocgap(client_name, course_id, 7)
        cohort_id = piece(piece(course_id,':',1),'+',1)
        module_code = piece(cohort_id,'-',0)
        #qry = f"select pillar from course_module where client_name = '{client_name}' and module_code = '{module_code}';"
        #pillar_code = rds_param(qry)
        qry = f"select enquiry_email from course_module where client_name = '{client_name}' and module_code = '{module_code}';"
        enquiry_email = rds_param(qry)
        if (soc == 1) and ((eoc == 0) or (eoc_gap == 1)):
            if debug_mode:
                print(f"processing {course_id}")
            query = f"select * from stages where client_name='{client_name}' and courseid='{course_id}';"
            sdf = rds_df(query)
            if sdf is None:
                continue
            sdf.columns = s_cols
            query = f"select * from userdata where client_name='{client_name}' and courseid='{course_id}';"
            udf = rds_df(query)
            if udf is None:
                continue
            udf.columns = u_cols
            err_list = []
            sent_list = []
            ulist = [x for x in udf.studentid]
            uname_list = [x for x in udf.username]
            udict = dict(zip(ulist,uname_list))
            if debug_mode:
                ulist = [ 1716 ]
                #eoc_gap = 1
            for sid in ulist:
                uname = udict[sid]                
                if debug_mode:
                    print(f"processing {sid} {uname}")
                df1 = udf[ udf.studentid == sid ].copy()
                df2 = mdf[ mdf.studentid == sid ].copy()
                email = df2.email.values[0]
                if email.lower().split('@')[1] in efilter:
                    del df1, df2
                    continue
                usrtyp = df2.usertype.values[0]
                if usrtyp != 1:
                    del df1, df2
                    continue
                tid = df2.chat_id.values[0]
                binded = df2.binded.values[0]
                tid = 0 if binded==0 else tid

                (tt, vars) = verify_student(client_name, df1, sid, course_id, sdf)
                (txt1, txt2, vars) = load_progress(df1, sid, vars, client_name, resp_dict, pass_rate, sdf)
                if txt1 in ["_soc_","_eoc_"]:
                    if debug_mode:
                        print(txt1, course_id, sid, uname)
                    continue
                stagecode = vars['stagecode']
                f2f_stage = vars['f2f_stage']
                f2f_error = 0 if f2f_stage=="" else 1
                stage = vars['stage']
                amt = vars['amt']
                mcq_zero = vars['mcq_zero']
                mcq_failed = vars['mcq_failed']
                as_zero = vars['as_zero']
                as_failed = vars['as_failed']
                asdate =  vars['asdate']
                risk_level = vars['risk_level']
                att_rate = vars['att_rate']
                pm_stage = vars['pm_stage']
                if (pm_stage == 1) and (att_rate < 0.75):
                    f2f_error = 1
                df = sdf[ sdf.stage == stagecode ].copy()
                m = len(df)
                if m == 0:
                    print(f"no matching record in stages table for {stagecode} {course_id} {sid}")
                    continue
                else:
                    stageid = df.id.values[0]
                    stagecode = df.stage.values[0]
                    stagename = df.name.values[0]
                    stagedesc = df.desc.values[0]
                    mcqvars = df.mcq.values[0]
                    asvars = df.assignment.values[0]
                    f2fvars = df.f2f.values[0]
                    iu_list = df.IU.values[0]
                due_date = vars['duedate']
                stagedesc = stagedesc.replace('?','')
                iu_list = [int(x) for x in str(iu_list).split(',') if x!='0' and x.isnumeric() ]
                mcq_zero_iu = [x for x in iu_list if x in mcq_zero]
                mcq_failed_iu = [x for x in iu_list if x in mcq_failed]
                eoc7 = 0
                if eoc_gap == 1:
                    try:
                        eoc7 = vmedxlib.edx_eocend(client_name, course_id, 7)
                    except:
                        pass
                    if eoc7==1:
                        if enquiry_email not in list(eoc7_maildict):
                            eoc7_maildict[enquiry_email]=''
                            eoc7_userdict[enquiry_email]=''
                        #txt = notif11
                        txt = notif12
                    else:
                        txt = notif10
                else:
                    txt = notif3
                    if (len(mcq_zero) + len(as_zero) == 0) and (f2f_error==0):
                        if debug_mode:
                            txt += resp8
                        continue
                    if len(mcq_zero) > 0:
                        txt += notif4
                    if len(mcq_failed) > 0:
                        txt += notif5
                    if len(as_zero) > 0:
                        txt += notif6
                    if len(as_failed) > 0:
                        txt += notif7
                    if f2f_error == 1:
                        if pm_stage==0:
                            resp11 = resp_dict['resp11']
                            txt += notif8
                        else:
                            if att_rate >= 0.75:
                                txt += notif8
                            else:
                                txt += notif9
                txt += "\n"
                if '{uname}' in txt:
                    txt = txt.replace('{uname}', uname)
                if '{course_id}' in txt:
                    txt = txt.replace('{course_id}', course_id)
                if '{email}' in txt:
                    txt = txt.replace('{email}', email)
                if '{sid}' in txt:
                    txt = txt.replace('{sid}', str(sid))
                if '{stagename}' in txt:
                    txt = txt.replace('{stagename}', stagename)
                if '{stagedesc}' in txt:
                    txt = txt.replace('{stagedesc}', stagedesc)
                if '{mcq_zero}' in txt:
                    txt = txt.replace('{mcq_zero}', str(mcq_zero))
                if '{mcq_failed}' in txt:
                    txt = txt.replace('{mcq_failed}', str(mcq_failed))
                if '{as_zero}' in txt:
                    txt = txt.replace('{as_zero}', str(as_zero))
                if '{as_failed}' in txt:
                    txt = txt.replace('{as_failed}', str(as_failed))
                if '{iu_list}' in txt:
                    txt = txt.replace('{iu_list}', str(iu_list))
                if '{mcq_zero_iu}' in txt:
                    txt = txt.replace('{mcq_zero_iu}', str(mcq_zero_iu))
                if '{mcq_failed_iu}' in txt:
                    txt = txt.replace('{mcq_failed_iu}', str(mcq_failed_iu))
                if '{due_date}' in txt:
                    txt = txt.replace('{due_date}', str(due_date))
                if '{asdate}' in txt:
                    txt = txt.replace('{asdate}' ,  str(asdate) )
                if '{f2f_stage}' in txt:
                    txt = txt.replace('{f2f_stage}' , f2f_stage)
                if '{enquiry_email}' in txt:
                    if debug_mode:
                        txt = txt.replace('{enquiry_email}' , 'kimhuat.lim@sambaash.com')
                    else:
                        txt = txt.replace('{enquiry_email}' , enquiry_email)
                if '{lf}' in txt:
                    txt = txt.replace('{lf}', "\n")
                if eoc7==1:
                    eoc7_maildict[enquiry_email] += txt + "\n"
                    eoc7_userdict[enquiry_email] += ',' + uname
                    eoc7_mailcnt += 1
                else:
                    if tid <= 0:
                        err_list.append(sid)
                    else:
                        try:
                            bot_intance.bot.sendMessage(int(tid), txt)
                            sent_list.append(sid)
                        except:
                            err_list.append(sid)
                # save f2f_error and f2f_stage into userdata table ?
                query = f"update userdata set risk_level = {risk_level}, mcq_zero = '{mcq_zero}' "
                query += f" ,mcq_failed = '{mcq_failed}', as_zero = '{as_zero}', as_failed = '{as_failed}' "
                query += f" where client_name='{client_name}' and courseid='{course_id}' and studentid={sid};"
                try:
                    rds_update(query)
                except:
                    pass
                del df1, df2
            del udf
            msg = ""
            if len(sent_list) > 0:
                msg += f"Succesfully sent intervention to learners from {course_id} :\n{str(sent_list)} \n"
            if len(err_list) > 0:
                msg += f"Unable to send intervention to learners from {course_id} :\n{str(err_list)} \n"
            if len(msg) > 0:
                try:
                    await bot_intance.bot.sendMessage(adm_chatid, msg)
                except:
                    #print(msg)
                    pass
            syslog(client_name,msg)
        await asyncio.sleep(1)
    # mess email to pillar owner
    ulist = ''
    for enquiry_email in list(eoc7_maildict):
        # ulist = list of username(s) ??
        ulist = eoc7_userdict[enquiry_email]
        txt = f"From: ombot@lithan.com\nTo: {enquiry_email}\nSubject : List of learners who reached EOC grace period.\n\n"
        txt += eoc7_maildict[enquiry_email]
        txt += "\nRegards,\nOmniMentorBot"
        #if use_mailapi:
        #    fout = open("mail.txt", "wt")
        #    fout.write(txt)
        #    fout.close()
        #    txt = shellcmd('/bin/sh txt2mail')  # to be replaced with rest apis
        #else:
        await bot_intance.bot.sendMessage(adm_chatid, txt) # temporary use telegram
    del mdf
    syslog(client_name,"auto_intervent completed")
    await bot_intance.bot.sendMessage(adm_chatid, "auto_intervent completed")
    return

def loadconfig():
    global ft_model, dt_model, nn_model, mcq_analysis, vmbot
    ok = True
    try:
        dt = "dt_model.bin"
        ft = "ft_model.bin"
        ffnn = "ffnn_model.hdf5"
        with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)

        client_name = bot_info['client_name']
        ft_model = vmnlplib.NLP_Parser()
        ft_model.load_modelfile(ft, client_name)
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model(dt)
        if dt_model.model_name == "":
            print("Error loading dt_model")
            ok = False
        #nn_model = vmffnnlib.NNGrader()
        #nn_model.model_loader(ffnn)
        #if nn_model.model_name == "":
        #    print("Error loading nn_model")
        #    ok = False
        mcq_analysis = vmmcqdlib.MCQ_Diff()
    except:
        ok = False
    return ok    

def load_respdict():
    mydict = dict()
    df = rds_df("select * from progress;")
    if df is not None:
        df.columns = ['key','response']
        keys = [ x for x in df.key]
        resp = [ x for x in df.response]
        mydict=dict(zip(keys,resp))
    return mydict

class BotInstance():
    def __init__(self):
        global vmbot, bot_intance
        bot_intance = self
        self.Token = ""
        self.bot_name = ""
        self.bot_id = ""
        self.client_name = ""
        self.bot_running = False
        self.bot = None
        self.adm_list = []
        self.user_list = {}
        self.chat_list = {}
        self.job_items = {}
        self.vars = dict()
        self.cmd_dict = dict()
        self.keys_dict = dict()
        self.keys_dict[option_mainmenu] = 1
        # /home=learner+admin+sys
        self.define_keys( mainmenu, self.keys_dict[ option_mainmenu ])
        # /Hello OmniMentor
        self.define_keys( learners_menu, self.keys_dict[ option_learners ])
        #
        self.keys_dict[ lrn_start ] = (self.keys_dict[ option_learners ]*10) + 1
        self.keys_dict[ lrn_student ] = (self.keys_dict[ option_learners ]*10) + 2
        self.keys_dict[ option_bind ] = (self.keys_dict[ option_learners ]*10) + 3
        # /admin
        self.define_keys( mentor_menu, self.keys_dict[ option_faculty ])
        self.define_keys( faculty_menu, self.keys_dict[ option_fct ])
        self.define_keys( playbook_menu, self.keys_dict[ option_pb ])
        self.define_keys( course_menu, self.keys_dict[ pb_userdata ])
        self.define_keys( analysis_menu, self.keys_dict[ option_analysis ])
        self.define_keys( mcqdiff_menu, self.keys_dict[ an_mcqd ])
        self.define_keys( users_menu, self.keys_dict[ option_usermgmt ])
        self.define_keys( useraction_menu, self.keys_dict[ option_resetuser ])
        self.keys_dict[ option_chatlist ] = (self.keys_dict[ option_chat ]*10) + 1
        self.keys_dict[ option_chatempty ] = (self.keys_dict[ option_chat ]*10) + 2
        self.keys_dict[ opt_pbusr ] = (self.keys_dict[ pb_userdata ]*10) + 1
        self.keys_dict[ opt_stage ] = (self.keys_dict[ ps_stage ]*10) + 1
        self.keys_dict[ opt_analysis ] = (self.keys_dict[ option_analysis ]*10) + 1
        self.keys_dict[ opt_mcqd ] = (self.keys_dict[ an_mcqd ]*10) + 1
        self.keys_dict[ opt_mcqavg ] = (self.keys_dict[ an_mcqavg ]*10) + 1
        self.keys_dict[ opt_aig ] = (self.keys_dict[ ml_grading ]*10) + 1
        # printdict(self.keys_dict)
        # printdict(self.cmd_dict)
        with open("vmbot.json") as json_file:
            bot_info = json.load(json_file)
        #printdict(bot_info)
        Token = bot_info['BotToken']
        client_name = bot_info['client_name']
        df = rds_df("select * from params where client_name = 'System';")
        if df is None:
            print("Unable to access params table from RDS")
            return
        df.columns = ['client_name','key', 'value']
        par_val = ['' + str(x) for x in df.value]
        par_key = [x for x in df.key]
        par_dict = dict(zip(par_key, par_val))
        self.adminchatid = int(par_dict['adminchatid'])
        max_duration = int(par_dict['max_duration'])
        #max_duration = 3600
        self.match_score = eval(par_dict['match_score'])
        self.use_regexpr = int(par_dict['regexpr'])
        self.pass_rate = float(par_dict['pass_rate'])
        self.gmt = int(par_dict['GMT'])
        self.resp_dict = load_respdict()
        if client_name=="":
            client_name = par_dict['client_name']
        self.client_name = client_name
        df = rds_df(f"select * from params where client_name = '{self.client_name}';")
        if df is None:
            print("Unable to access params table from RDS")
            return
        df.columns = ['client_name','key', 'value']
        par_val = ['' + str(x) for x in df.value]
        par_key = [x for x in df.key]
        par_dict = dict(zip(par_key, par_val))
        email_filter = par_dict['email_filter']
        self.efilter = email_filter.split(',')
        self.edx_time = int(par_dict['edx_import']) if 'edx_import' in par_key else 0
        self.automsg = int(par_dict['automsg']) if 'automsg' in par_key else 0
        self.stages_cols = get_columns("stages")
        self.userdata_cols = get_columns("userdata")
        self.usermaster_cols = get_columns("user_master")
        hdr = par_dict['edx_api_header']
        self.edx_api_header = eval(hdr)
        self.edx_api_url = par_dict['edx_api_url']
        vmedxlib.edx_api_url = self.edx_api_url
        vmedxlib.edx_api_header = self.edx_api_header
        self.sub_str  = "SUBSTRING" if ':' in vmsvclib.rds_connstr else "SUBSTR"
        if Token == "":
            Token = par_dict['BotToken']
        try:        
            #self.bot = telepot.DelegatorBot(Token, [
            #    pave_event_space()( [per_chat_id(), per_callback_query_chat_id()],
            #    create_open, MessageCounter, timeout=max_duration, include_callback_query=True),
            #])
            print(Token) # @OmniMentorBot
            self.bot = telepot.aio.DelegatorBot(Token, [
                pave_event_space()( per_chat_id(),
                create_open, MessageCounter, timeout=max_duration),            
            ])
            vmbot = self.bot
            self.Token = Token
            #loop = asyncio.get_event_loop()
            #self.loop = loop
            #loop.create_task(MessageLoop(self.bot).run_forever())
            #asyncio.ensure_future(job_scheduler())
            #loop.run_forever()
            #loop.close()
        except:
            pass
        del par_dict, df, client_name
        return

    def __str__(self):
        return "Telegram chatbot service class"

    def __repr__(self):
        return 'BotInstance()'

    def define_keys(self, telegram_menu, start_key):
        button_list = lambda x : str(x).replace('[','').replace(']','').replace(", ",",").replace("'","").split(',')
        menu_keys = start_key*100 + 1
        self.cmd_dict[start_key] = button_list(telegram_menu)
        for menu_item in self.cmd_dict[start_key] :
            if (menu_item != option_back) and (menu_item != ''):
                self.keys_dict[menu_item] = menu_keys
                menu_keys += 1
        return

    def get_menukey(self, val):
        menukey = ""
        for key, value in (self.keys_dict).items():
             if val == value:
                 menukey = key
        return menukey

class MessageCounter(telepot.aio.helper.ChatHandler):
    #class MessageCounter(telepot.helper.ChatHandler):
    def __init__(self, *args, **kwargs):
        super(MessageCounter, self).__init__(*args, **kwargs)
        self.new_session = True
        self.is_admin = False
        self.client_name = ""
        self.chatid = 0
        self.chatname = ""
        self.student_id = 0
        self.email = ''
        self.username = ''
        self.userdata = ''
        self.courseid = ''
        self.coursename = ''
        self.stagetable = ''
        self.stage_name = ''
        self.stagedate = ''
        self.stage_list = []
        self.stage_days = []
        self.tablefields = ''
        self.edited = 0
        self.tableindex = 0
        self.tablerows = []
        self.list_courseids = []
        self.list_coursename = []
        self.course_selection = []
        self.mentor_email = ""
        self.asst_email = ""
        self.records = dict()
        self.menu_id = 0
        self.menu_home = []

    def reset(self):
        self.__init__()
        return

    async def logoff(self, txt = "Have a great day!"):
        global vmbot,bot_intance
        try:
            if self.chatid in [d for d in bot_intance.user_list]:
                bot_intance.user_list.pop(self.chatid)
            if self.chatid in [d for d in bot_intance.adm_list]:
                bot_intance.adm_list.pop(self.chatid)
        except:
            pass
        #bot_prompt(self.bot, self.chatid, txt, [[btn_hellobot]])
        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[btn_hellobot]]))        
        syslog('system',f"telegram user {self.chatid} logged out.")
        self.new_session = True
        self.chatid = 0
        self.student_id = 0
        self.reset
        self.menu_id = 0
        return

    async def on_close(self, exception):
        await self.logoff()
        await self.sender.sendMessage('session time out.\nPress /start a few times to awake this bot.')
        return

    def mcqas_chart(self, groupcht = False ):
        if self.userdata is None:
            return (None,None)
        avg_list = dict()
        if groupcht:
            cohort_id = piece(piece(self.courseid,':',1),'+',1)
            fn = 'chart_' + cohort_id + '.png'
            title = f"MCQ and Assignment scores for cohort {cohort_id}"
            df = self.userdata
        else:
            sid = self.student_id
            if sid == 0:
                return (None,None)
            df = self.userdata[self.userdata.studentid==sid]
            fn = 'chart_' + str(sid) + '.png'
            title = f"MCQ and Assignment scores for student #{sid}"
        avg_list = dict()
        for avgopt in ['mcq_avg','as_avg']:
            avg_list[avgopt] = [ df[ avgopt + str(x) ].mean() for x in range(1,14)]

        df = pd.DataFrame({
            'Test/IU' : [ '#' + str(n) for n in range(1,14) ],
            'mcq test' : [x * 100 for x in avg_list['mcq_avg']],
            'assignment test' : [x * 100 for x in avg_list['as_avg']]
        })
        df.plot(kind='bar',figsize=(10,4), rot = 90)
        plt.title(title)
        ax = plt.gca()
        cols = [c for c in  df.columns]
        label_col = cols[0]
        xcol = cols[1]
        label_list = [ x for x in df[label_col] ]
        width = 0.8
        plt.xlim([-width, len(df[xcol])-width])
        ax.set_xticklabels((label_list))
        ax.yaxis.set_major_formatter(mtk.PercentFormatter())
        plt.draw()
        plt.savefig(fn, dpi=100)
        plt.clf()
        f = open(fn, 'rb')
        #self.bot.sendPhoto(self.chatid, f)
        if groupcht:
            df = pd.DataFrame({
                'Test/IU' : [ '#' + str(n) for n in range(1,14) ],
                'mcq test' : [ "{:.2%}".format(x) for x in avg_list['mcq_avg'] ],
                'assignment test' : [ "{:.2%}".format(x) for x in avg_list['as_avg'] ]
            })            
            return (df,f)
        del df
        return (None, f)

    def session_info(self):
        global vmbot,bot_intance
        txt = "Summary:\n"
        if self.is_admin :
            txt += '\nYou are the faculty adm\n'
            txt += 'Student id : ' + str(self.student_id) + "\n\n"
            if len(bot_intance.user_list)==0:
                txt += 'No students online\n'
            else:
                cnt = len(bot_intance.user_list)
                txt += f"{cnt} students online:\n"
        else:
            if self.new_session:
                txt += '\nsession already logged out.'
            else:
                txt += '\nStudent id : ' + str(self.student_id)
                txt += '\nCourse ID : ' + self.courseid
                txt += '\nCourse Name : ' + self.coursename
                cc = [x for x in self.list_courseids if self.courseid != x]
                if len(cc)>0:
                    txt += "\nOther courses (cohort-id):\n"
                    for x in cc:
                        txt += "\t" + x + "\n"
                txt += '\nLearning Stage : ' + self.stage_name
                txt += '\nOutstanding Amount : ' + str(self.records['amt']) + "\n"
                txt += '\nMentor email : ' + self.mentor_email
                txt += '\nLA email : ' + self.asst_email
        txt += f"\nYour username is {self.username}"
        txt += f"\nYour email is {self.email}"
        txt += f"\nYour telegram chat_id is {self.chatid}"
        txt += f"\nYour telegram chat name is {self.chatname}"
        txt += f"\nSystem client name is {self.client_name}"
        return txt

    def mentor_chatid(self):
        # read from mentor_email of course_master table ?
        query = "select u.chat_id from playbooks p inner join user_master u on p.client_name=u.client_name "
        query += f"and p.mentor=u.email where p.course_id='{self.courseid}' and p.client_name = '{self.client_name}';"
        mentorchatid = rds_param(query)
        if mentorchatid == '':
            return 0
        return int(mentorchatid)

    def livechat(self, sid=0, telegram_id=0):
        global vmbot,bot_intance
        list_learners = []
        if len(bot_intance.user_list)==0:
            txt = "There is no online learners at the moment"
            #self.sender.sendMessage(txt)
            return (0,txt)
        elif sid>0:
            tlist = [x for x in list(bot_intance.user_list) if bot_intance.user_list[x][1] == sid]
            if tlist == []:
                txt = f"student #{sid} is not online at the moment"
                #self.sender.sendMessage(txt)
                return (0,txt)
            else:
                if self.chatid in bot_intance.user_list:
                    user_from = bot_intance.user_list[self.chatid][2]
                else:
                    user_from = self.username
                if telegram_id==0:
                    tid = tlist[0]
                else:
                    tid = telegram_id
                tname = bot_intance.user_list[tid][2]
                #txt = '❚█══ Live Chat ══█❚\nHi ' + tname + ', you are in the live chat with : <a href=\"tg://user?id=' + str(self.chatid) + '">' + user_from + '</a>'
                #self.bot.sendMessage(tid, txt, parse_mode='HTML')
                bot_intance.chat_list[tid] = self.chatid
                bot_intance.chat_list[self.chatid] = tid
                #txt = '❚█══ Live Chat ══█❚\nHi, you are in the live chat with : <a href=\"tg://user?id=' + str(tid) + '">' + tname + '</a>'
                #self.bot.sendMessage(self.chatid, txt, parse_mode='HTML')
                #self.menu_id = bot_intance.keys_dict[option_chat]
                return (1, tname)
        else:
            if self.is_admin:
                list_learners = [ ['     '.join([str(d) for d in bot_intance.user_list[r]])] for r in bot_intance.user_list ]
            else:
                list_learners = [ ['     '.join([str(d) for d in bot_intance.user_list[r]])] for r in bot_intance.user_list \
                    if (bot_intance.user_list[r][0] == self.courseid) and (bot_intance.user_list[r][1] != self.student_id) ]
            if len(list_learners) > 0:
                txt = 'Chat with online learners 🗣'
                list_learners = list_learners + [ [option_back] ]
                #bot_prompt(self.bot, self.chatid, txt, list_learners)
                #self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(list_learners))
                #self.menu_id = bot_intance.keys_dict[option_chatlist]
                return(2, self.reply_markup(list_learners) )
            else:
                txt = "There is no online learners at the moment"
                #self.sender.sendMessage(txt)
                return (0,txt)
        return

    def endchat(self):
        global vmbot,bot_intance
        chat_id = self.chatid
        chat_found = False
        tid = 0
        if chat_id in bot_intance.chat_list:
            chat_found = True
            tid = bot_intance.chat_list[chat_id]
            #txt = "Live chat session disconnected. 👋"
            #self.bot.sendMessage(chat_id, txt)
            #self.bot.sendMessage(tid, txt)
            try:
                bot_intance.chat_list.pop(chat_id)
                if tid != chat_id:
                    bot_intance.chat_list.pop(tid)
            except:
                pass
            for c in [chat_id , tid] :
                if c in list(bot_intance.user_list) :
                    bot_intance.user_list[ c ][4] = ""
        #return chat_found
        return tid

    def runfaq(self, resp):
        global ft_model, vmbot, bot_intance
        accuracy = 0
        match_score = bot_intance.match_score
        use_regexpr = bot_intance.use_regexpr
        user_resp = resp.lower()
        txt = ""
        if use_regexpr == 1 :
            txt = ft_model.match_resp(resp)
            syslog( "REG" , txt )

        if txt == '':
            (result, accuracy) = ft_model.get_response(resp)
            if accuracy >= match_score:
                txt = result
                syslog( "NLP" , txt )

        if txt == '':
            ( result, accuracy ) = ft_model.find_matching( resp )
            if accuracy > 0:
                if accuracy < match_score:
                    txt = 'do you mean this ? =>\n' + result
                    user_resp = result.lower()
                    self.sender.sendMessage(txt)
                txt = ft_model.match_resp(user_resp)
                syslog( "REG" , txt )

        if (txt != '') and re.search('.*\{.*', txt):
            stagedate = str(self.stagedate)
            mcqlist = str(self.records['mcqlist']) if 'mcqlist' in list(self.records) else ''
            aslist = str(self.records['aslist']) if 'aslist' in list(self.records) else ''
            amt = str( self.records['amt'] )
            mcqas_chart = ""
            mcqdate = stagedate
            asdate = stagedate
            eldate = stagedate
            fcdate = stagedate
            if '{lf}' in txt:
                txt = txt.replace('{lf}' , '\n')
            if '{sos}' in txt:
                bot_intance.user_list[ self.chatid ][4] = "👋"
                txt = txt.replace('{sos}' , '')
            if '{mcqas_chart}' in txt:
                self.mcqas_chart()
                txt = txt.replace('{mcqas_chart}' , '')
            if '{mcq_att_balance}' in txt:
                result = self.track_attempts()
                txt = txt.replace('{mcq_att_balance}' , result)
            if '{mentor_email}' in txt:
                txt = txt.replace('{mentor_email}' , self.mentor_email)
            if '{assistance_email}' in txt:
                txt = txt.replace('{assistance_email}' , self.asst_email)
            try:
                txt = eval("f'@'".replace('@', txt.replace('\n','~~~'))).replace('~~~','\n')
            except:
                # formatted string does not work due to old python versions 2.7
                tt = txt.replace('\n','~~~')
                arr_parts = [t.split('{')[1] for t in tt.split('}') if '{' in t]
                for w in arr_parts:
                    p = '{' + w + '}'
                    v = eval(w)
                    tt = tt.replace( p, str(v) )
                txt = tt.replace('~~~','\n')

        if txt == '':
            txt = "I'm sorry, I do not understand you but could you be more specific about your question related to "
            txt += "'" + resp + "' ?"

        recommendation = ft_model.recommend_list(resp)
        #if len(recommendation) > 0:
        #    if txt != '':
        #        await self.sender.sendMessage(txt)
        #        rec_menu = build_menu(recommendation,1,option_back,[])
        #        #bot_prompt(self.bot, self.chatid, "You might want to ask :", rec_menu)
        #        await self.bot.sendMessage(self.chatid, "You might want to ask :", reply_markup=self.reply_markup(rec_menu))
        #        txt = ""
        #        self.menu_id = bot_intance.keys_dict[option_faq]
        #return txt
        return (txt, recommendation)

    def load_tables(self):
        global vmbot,bot_intance
        client_name = bot_intance.client_name
        self.sender.sendMessage("Please wait for a while.")
        try:
            if self.is_admin:
                vmedxlib.update_mcq(self.courseid, client_name, 0)
                vmedxlib.update_assignment(self.courseid, client_name, 0)
            else:
                vmedxlib.update_mcq(self.courseid, client_name, self.student_id)
                vmedxlib.update_assignment(self.courseid, client_name, self.student_id)
        except:
            pass
        qry = "SELECT u.* FROM userdata u INNER JOIN user_master m ON u.client_name=m.client_name "
        qry += f" AND u.studentid=m.studentid WHERE u.client_name='{client_name}' AND u.courseid = '{self.courseid}' "
        qry += ''.join([ " and lower(m.email) not like '%" + x + "'"  for x in bot_intance.efilter])
        df = rds_df( qry )
        if df is None:
            self.userdata = None
            vmedxlib.edx_import(self.courseid, self.client_name)
            df = rds_df( qry )
        if df is not None:
            df.columns = bot_intance.userdata_cols
            self.userdata = df
        qry = "select * from stages where client_name = '_c_' and courseid = '_x_';"
        qry = qry.replace('_c_', self.client_name)
        qry = qry.replace('_x_', self.courseid)
        df = rds_df( qry)
        if df is None:
            self.stagetable = None
        else:
            df.columns = bot_intance.stages_cols
            self.stagetable = df
        return

    def load_courseinfo(self, resp):
        if len(self.list_courseids)==0:
            return 0
        self.courseid = ""
        self.userdata = ""
        self.coursename = ""
        ok = 0
        if resp in self.list_courseids:
            n = self.list_courseids.index(resp)
            self.coursename = self.list_coursename[n]
            self.courseid = resp
            self.load_tables()
            ok = 1
        return ok

    def check_student(self, sid, chat_id):
        global bot_intance,vmbot
        self.student_id = 0
        txt = ''
        if self.new_session == False :
            return ('', [])
        if self.userdata is None:
            return ('', [])
        (txt, self.records ) = verify_student(self.client_name, self.userdata, sid, self.courseid, None)
        self.mentor_email = self.records['mentor_email']
        self.asst_email = self.records['asst_email']
        if sid > 0:
            binded = rds_param(f"select binded FROM user_master where client_name='{self.client_name}' and studentid={sid};")
            if binded==0:
                query = f"update user_master set chat_id={chat_id} where client_name='{self.client_name}' and studentid={sid};"
                rds_update(query)
        err = 0
        if (self.records=={}) :
            txt = "Hi, there is incomplete information at the moment, the session is not ready yet.\n\n"
            txt += "Please select the course from below list."
            btn_course_list = build_menu(self.list_courseids,1)
            #bot_prompt(self.bot, self.chatid, txt, btn_course_list)
            #self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
            self.menu_id = bot_intance.keys_dict[option_learners]
            return (txt, btn_course_list)
        else:
            self.is_admin = False
            self.student_id = sid
            self.new_session = False
            bot_intance.user_list[chat_id]=[self.courseid, self.student_id, self.username, chat_id, ""]
            self.records = load_vars(self.userdata, sid)
            vars = display_progress(self.userdata, self.stagetable, sid, self.records, self.client_name, bot_intance.resp_dict, bot_intance.pass_rate)
            for v in list(vars):
                self.records[v] = vars[v]
            txt  = vars['notification']
            self.stage_name = vars['stage']
            if vars['course_alive'] == 1:
                txt += grad_pred_text(vars, self.client_name)
            if txt == "":
                txt = "Welcome back."
            #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            #vmbot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(self.menu_home))
            self.menu_id = bot_intance.keys_dict[lrn_student]
            return (txt, self.menu_home)
        return ('', [])

    def getdoc(self, bot, msg):
        fname = msg['document']['file_name']
        fid = msg['document']['file_id']
        fpath = bot.getFile(fid)['file_path']
        fn = "https://api.telegram.org/file/bot" + bot._token + "/"  + fpath
        return (fname, fn)

    def track_attempts(self):
        global vmbot,bot_intance
        vars = load_vars(self.userdata, self.student_id)
        (t0, t1, vars) = load_progress(self.userdata, self.student_id, vars, self.client_name, bot_intance.resp_dict, bot_intance.pass_rate, self.stagetable)
        if vars['mcq_att_balance'] == "":
            txt = "There is no outstand MCQs for futher attempts."
        else:
            txt = vars['mcq_att_balance']
        return txt

    def grad_prediction(self):
        global nn_model, dt_model
        if dt_model.model_name == "" :
            print("please load the model first")
            return ( [] , None )
        #if nn_model.model_name == "":
        #    print("please load the model first")
        #    return []
        txt = ""
        df = self.userdata
        if self.userdata is None:
            print("there is no data")
        list_sid = [str(x) for x in df.studentid]
        if len(list_sid)==0:
            return ( [] , None )
        client_name = list(df['client_name'])[0]
        courseid = list(df['courseid'])[0]
        progress_df = dict()
        progress_tt = dict()
        tbl = []
        new_sidlist = []
        for vv in list_sid:
            sid = int(vv)
            vars = load_vars(self.userdata, sid)
            uu = vars['username']
            fw = lambda u,v : f"\nTest Results for Student #{v} {u}\n    MCQ Tests\t\tAssignment Tests\n"
            fx = lambda n : vars["mcq_avg"+str(n)]
            fy = lambda n : vars["as_avg"+str(n)]
            gx = lambda n : vars["mcq_attempts"+str(n)]
            gy = lambda n : vars["as_attempts"+str(n)]
            gz = lambda n : gx(n) + gy(n) if gx(n) is not None and  gy(n) is not None else 0
            fz = lambda n : [ "#" + str(n) , "{:.2%}".format(fx(n)), str(gx(n)), "{:.2%}".format(fy(n)) , str(gy(n)) ]
            mscores = [fx(n) for n in range(1,14) if gx(n) > 0]
            ascores = [fy(n) for n in range(1,14) if gy(n) > 0]
            mcnt = len(mscores)
            acnt = len(ascores)
            if mcnt+acnt==0:
                continue
            mavg = 0 if mcnt == 0 else sum(mscores) / mcnt
            aavg = 0 if acnt == 0 else sum(ascores) / acnt
            # current not working
            #use_neural_network = False   # True or False
            #if use_neural_network:
            #    #mavgatt = sum([gx(n) for n in range(1,14)]) / 13
            #    #mmaxatt = max([gx(n) for n in range(1,14)])
            #    #as_avgatt = sum([gy(n) for n in range(1,14)]) / 13
            #    #as_maxatt = max([gy(n) for n in range(1,14)])
            #    #grad_pred = nn_model.pred(client_name,mavg,mavgatt,mmaxatt,aavg,as_avgatt,as_maxatt,acnt)
            #    #if grad_pred is None:
            #    #    continue
            #    pass
            #else:
            grad_pred = dt_model.predict(mavg , aavg, mcnt)
            tbl.append( [vv , uu , "{:.2%}".format(grad_pred[0])] )
            progress_list = [ fz(n) for n in range(1,14) if gz(n) > 0]
            progress_list.append(['Avg', "{:.2%}".format(mavg) , " ",  "{:.2%}".format(aavg) , " "])

            df =  pd.DataFrame( progress_list )
            df.columns = ['Test #', 'MCQ', '#Attempts', 'Assignment', '# Attempts']
            progress_df[sid] = df

            progress_tt[sid] = f"\nTest Results for Student #{vv} {uu}"
            new_sidlist.append(str(sid))
        self.records['progress_df'] = progress_df
        self.records['progress_tt'] = progress_tt
        df1 =  pd.DataFrame( tbl )
        if len(df1) == 0:
            self.sender.sendMessage("There is no data for this course id")
            return ( [] , None )
        df1.columns = ['Student ID#','Name', 'Prediction']
        tt = "AI Grading for " + self.courseid
        df1= df1.sort_values(by ='Prediction')
        return ( new_sidlist , df1 )        

    def find_course(self, stud):
        client_name =  self.client_name
        course_id_list = []
        course_name_list = []
        logon_list = []
        date_today = datetime.datetime.now().date()
        yrnow = str(date_today.strftime('%Y'))
        if client_name == "":
            return (course_id_list, course_name_list, logon_list)
        if stud==0:
            sub_str  = bot_intance.sub_str
            qry = f"SELECT DISTINCT a.course_id, a.course_name FROM playbooks a "
            qry += f"INNER JOIN course_module c ON a.client_name=c.client_name and a.module_code=c.module_code "
            #qry += f"INNER JOIN course_module c ON a.client_name=c.client_name and a.module_code=c.module_code and a.course_code=c.course_code "
            qry += f"where c.enabled=1 AND a.client_name='{client_name}' and {sub_str}(course_id,-4)='{yrnow}' ORDER BY a.course_id;"
            df = rds_df(qry)
            if df is not None:
                df.columns = ['course_id','course_name']
                course_id_list = [x for x in df.course_id]
                course_name_list = [x for x in df.course_name]
            return (course_id_list, course_name_list, course_id_list)
        #userinfo = {}
        #api_url = bot_intance.edx_api_url
        #if api_url != '':
        #    url = f"{api_url}/user/fetch/{stud}"
        #    headers = bot_intance.edx_api_header
        #    userinfo = edxapi_getuser(headers, url)
        #if userinfo == {} :
        (course_id_list, course_name_list, logon_list) = rds_loadcourse(client_name, stud)
        return (course_id_list, course_name_list, logon_list)

    def reply_markup(self, buttons=[], opt_resize = True):        
        mark_up = None
        if buttons == []:
            mark_up = {'hide_keyboard': True}
        else:
            mark_up = ReplyKeyboardMarkup(keyboard=buttons,one_time_keyboard=True,resize_keyboard=opt_resize)
        return mark_up

    async def on_chat_message(self, msg):
        global ft_model, dt_model, mcq_analysis, vmbot, bot_intance
        try:
            content_type, chat_type, chat_id = telepot.glance(msg)
            bot = self.bot
            self.chatid = chat_id
            keys_dict = bot_intance.keys_dict
            adminchatid = bot_intance.adminchatid        
        except:        
            return

        resptxt = ""
        resp = ""
        txt = ""
        retmsg = ""
        title = ""
        html_msg_dict = dict()
        if content_type == 'text':
            resp = msg['text'].strip()
            resptxt = resp.lower()
            if 'from' in list(msg):                
                username = msg['from']['first_name']
                self.records['username'] = username
                self.chatname = username
                if self.username=="":
                    self.username = username
        elif content_type != "text":
            txt = "Thanks for the " + content_type + " but I do not need it for now."
            await self.sender.sendMessage(txt)
            return
        else:
            syslog( content_type , str(msg) )

        if chat_id <0 :
            await self.bot.sendMessage(71354936, resp + " from " + str(chat_id))
            return

        if (resp not in list(keys_dict)) and (resp != [option_back]) and (resp[0]!='/'):       
            syslog(str(self.chatid), f"response = {resp}")
            
        if resp=='/?':
            retmsg = self.session_info()            
            retmsg += "\nmenu id = " + str(self.menu_id) 
            k = bot_intance.get_menukey(self.menu_id) 
            retmsg += "\nmenu key : " + str(k)

        #elif resp=='/z':
            #pass
                
        elif resp=='/end':
            tid = self.endchat()
            if tid > 0:
                txt = "Live chat session disconnected. 👋"
                await self.bot.sendMessage(tid, txt)
                await self.bot.sendMessage(chat_id, txt)
            txt = "sesson closed."
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([['/start']]))
            self.is_admin = (chat_id == adminchatid)
            syslog("system", "telegram user " + str(chat_id) + " offine.")            
            await self.logoff()
        
        elif resp=='/stop' and (chat_id in [adminchatid, 71354936]):
            for d in bot_intance.user_list:
                await bot.sendMessage(d, 'System shutting down.')        
            #bot_intance.bot_running = False
            await self.logoff()
            result ='<pre> ▀▄▀▄▀▄ OmniMentor ▄▀▄▀▄▀\n Powered by Sambaash</pre>Contact <a href=\"tg://user?id=1064466049">@OmniMentor</a>'
            await bot.sendMessage(chat_id,result,parse_mode='HTML')
            syslog('system', txt)
            
                
        elif resp == '/start' or resp == '/hellobot' or resp == btn_hellobot:            
            self.reset
            self.new_session = True
            self.is_admin = (chat_id == adminchatid)
            self.menu_home = learners_menu
            self.edited = 0
            self.client_name = bot_intance.client_name
            if chat_id > 0 :
                tid = self.endchat()
                if tid > 0:
                    txt = "Live chat session disconnected. 👋"
                    await self.bot.sendMessage(tid, txt)
                    await self.bot.sendMessage(chat_id, txt)
                syslog("system", "telegram user " + str(chat_id) + " online.")
                query = "select * from user_master where binded=1 and chat_id =" + str(chat_id) + " and client_name = '" + self.client_name + "';"
                df = rds_df(query)
                if df is None:
                    sid = 0
                    self.student_id = 0
                    usertype = 0
                    courseid = ""
                    binded = 0
                else:
                    df.columns = bot_intance.usermaster_cols
                    sid = int(df.studentid.values[0])
                    self.student_id = sid
                    self.username = df.username.values[0]
                    usertype = df.usertype.values[0]
                    courseid = df.courseid.values[0]
                    binded =  df.binded.values[0]
                    self.email = df.email.values[0]
                if (binded==1) and (sid > 0) and (courseid != ""):
                    if usertype == 1:
                        (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(sid)
                        query = f"select course_name from playbooks where course_id ='{courseid}' and client_name='{self.client_name}';"
                        coursename = rds_param(query)
                        msg = "You are in course:\n" + courseid
                        self.courseid = courseid
                        self.load_tables()
                        await bot.sendMessage(chat_id, msg)
                        (txt, menu_item) = self.check_student(self.student_id, self.chatid)
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))
                        return
                    elif usertype == 11:
                        self.is_admin = True
                        self.menu_id = 1
                        bot_intance.adm_list.append(sid)
                        txt = banner_msg("Welcome " + self.username,"You are now connected to Mentor mode.")
                        self.menu_home = mentor_menu                            
                        (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(0)
                        #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                        await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                        return
                    else:
                        await self.sender.sendMessage("Sorry your account is blocked, please contact the admin.")
                        await self.logoff()
                        return
                else:
                    self.list_courseids = []
                    txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + self.client_name + '.\n'
                    txt += "\nplease enter your student id or email address :"
                    #bot_prompt(self.bot, self.chatid, txt, [])
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup([]))
                    self.menu_id = keys_dict[option_learners]

        elif self.menu_id == keys_dict[option_mainmenu]:
            if resp == option_fct :
                txt = 'You are in faculty admin mode.'
                #bot_prompt(self.bot, self.chatid, txt, faculty_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(faculty_menu))
                self.menu_id = keys_dict[option_fct]
            elif resp == option_pb :
                txt = 'You are in playbooks maintainence mode.'
                #bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbook_menu))
                self.menu_id = keys_dict[option_pb]
            elif resp == option_chat :
                (status, info) = self.livechat()
                if status == 2:
                    txt = 'Chat with online learners 🗣'
                    await self.bot.sendMessage(self.chatid, txt, reply_markup = info )
                    self.menu_id = keys_dict[option_chatlist]
                else:
                    await self.sender.sendMessage(info)
            elif resp == option_analysis :
                txt = "Let's take a look on the following courses."
                courseid_menu = build_menu([x for x in self.list_courseids],1)
                #bot_prompt(self.bot, self.chatid, txt, courseid_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(courseid_menu))
                self.menu_id = keys_dict[opt_analysis]
            elif resp == option_bindadm:
                txt += "\nDo you want me to activate auto-login without entering admin id each time ?"
                opt_yes = "Yes, enable auto-login"
                opt_no = "No, I would like to login manually each time"
                #yesno_menu = build_menu([opt_yes,opt_no],1,option_back)
                #bot_prompt(self.bot, self.chatid, txt, yesno_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(yesno_menu))
                txt = ""
                self.menu_id = bot_intance.keys_dict[option_bind]            
            elif resp == option_usermgmt:                
                txt = 'To search for student-ID or reset User by student-ID.'
                #bot_prompt(self.bot, self.chatid, txt, users_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(users_menu))
                self.menu_id = keys_dict[option_usermgmt]
            elif (resp == option_back) or (resp == "0"):
                tid = self.endchat()
                if tid > 0:
                    txt = "Live chat session disconnected. 👋"
                    await self.bot.sendMessage(tid, txt)
                    await self.bot.sendMessage(chat_id, txt)
                if chat_id in bot_intance.adm_list:
                    bot_intance.adm_list.pop(chat_id)
                syslog("system", "telegram user " + str(chat_id) + " offine.")
                await self.logoff()
                await self.bot.sendMessage(chat_id, "Session closed.", reply_markup=self.reply_markup([[btn_hellobot]]))

        elif self.menu_id == keys_dict[option_learners] :
            if (resp == option_back) or (resp == "0"):
                await self.logoff()
                #bot_prompt(self.bot, self.chatid, "End of session.", [[btn_hellobot]])
                await self.bot.sendMessage(self.chatid, "End of session.", reply_markup=self.reply_markup([[btn_hellobot]]))
                return
            if '@' in resp :
                if self.edited == 0:
                    df = rds_df( "select distinct studentid, email from user_master order by email")
                    if df is not None:
                        df.columns = ['studentid','email']
                        result = email_lookup(df, resptxt)
                        if result != "":
                            resp = result
                            self.edited = int("0" + str(result))
                if self.edited > 0:
                    sid = self.edited
                    condqry = f" WHERE studentid={str(sid)} and lower(email)='{resptxt}' and client_name = '{self.client_name}';"
                    result = rds_param("SELECT email FROM user_master " + condqry)
                    if result == "":
                        await self.sender.sendMessage("Sorry your account is not found, please contact the admin.")
                        await self.logoff()
                        return
                    uname = rds_param("SELECT username FROM user_master " + condqry)
                    self.username = uname
            if resp.isnumeric():
                sid = int(resp)
                query=f"SELECT COUNT(*) AS cnt FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';"
                result = rds_param(query)
                cnt = int("0" + str(result))
                if cnt == 0:
                    await self.sender.sendMessage("Sorry your account is not found, please contact the admin.")
                    await self.logoff()
                    return
                query = "select * from user_master where studentid = " + resp + " and client_name = '" + self.client_name + "';"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry your account is not found, please contact the admin.")
                    await self.logoff()
                    return
                df.columns = bot_intance.usermaster_cols
                self.email = df.email.values[0]
                usertype = df.usertype.values[0]
                self.username = df.username.values[0]
                #usertype = rds_param(f"SELECT usertype FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';")
                #query = "select username from user_master where studentid =" + resp + \
                #    " and client_name = '" + self.client_name + "';"
                #self.username = rds_param(query)                
                if usertype == 11:
                    self.is_admin = True
                    self.menu_id = 1
                    self.student_id = sid
                    bot_intance.adm_list.append(sid)
                    txt = banner_msg("Welcome " + self.username,"You are now connected to Mentor mode.")
                    self.menu_home = mentor_menu
                    (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(0)
                    #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                    return
                elif usertype not in [1,2]:
                    await self.sender.sendMessage("Sorry your account is blocked, please contact the admin.")
                    await self.logoff()
                    return                
                if sid in bot_intance.user_list:
                    txt = "Sorry you can't logon using another telegram account\nPlease try again later."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return                
                (self.list_courseids, self.list_coursename,self.course_selection) = self.find_course(sid)
                stud_courselist = self.list_courseids
                #slen = len(stud_courselist)
                slen = len(self.course_selection)
                self.student_id = sid
                if (slen == 0) :
                    txt = "Sorry, we are not unable to find your record.\nPlease contact the course admin."
                    await self.sender.sendMessage(txt)
                    await self.logoff()
                    return                
                if slen == 1:
                    self.chatid = chat_id
                    self.courseid = self.course_selection[0]
                    n = self.list_courseids.index(self.courseid)
                    self.coursename = self.list_coursename[n]
                    self.load_tables()
                    if self.userdata is None:
                        await self.sender.sendMessage(f"we do not have any data for course id\n{self.courseid}")
                        return
                    else:
                        #self.check_student(self.student_id, chat_id)
                        (txt, menu_item) = self.check_student(self.student_id, chat_id)
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))                        
                elif slen < 20:
                    btn_course_list = build_menu(stud_courselist, 1) 
                    txt = "Please select the course id from below:"
                    #bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                    self.menu_id = keys_dict[option_learners]
                else:
                    #btn_course_list = build_menu(stud_courselist, 1)
                    date_today = datetime.datetime.now().date()
                    yrnow = str(date_today.strftime('%Y'))
                    course_list = [x for x in stud_courselist if x[-4:]==yrnow]
                    btn_course_list = build_menu(course_list, 1) 
                    txt = "Please select the course id from below:"
                    #bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                    self.menu_id = keys_dict[option_learners]
            else:                
                if self.load_courseinfo(resp) == 0:
                    txt = 'Your selection is not available !\n'
                    txt += 'Please select the course from below list'
                    date_today = datetime.datetime.now().date()
                    yrnow = str(date_today.strftime('%Y'))
                    course_list = [x for x in self.list_courseids if resptxt in x.lower()]
                    btn_course_list  = build_menu(course_list, 1)
                    #bot_prompt(self.bot, self.chatid,  txt, btn_course_list )
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                else:
                    self.chatid = chat_id                    
                    if self.student_id == 0 or self.chatid == 0 :                     
                        txt = 'My name is OmniMentor, I am your friendly learning facilitator at ' + self.client_name + '.\n'
                        txt += "\nplease enter your student id or email address :"
                        #bot_prompt(self.bot, self.chatid, txt, [])
                        await self.bot.sendMessage(self.chatid, txt, [])
                        txt = ''
                        self.menu_id = keys_dict[lrn_start]
                    else:                        
                        sid = self.student_id
                        ch_id = self.chatid   
                        #self.load_tables()  
                        #self.update_stage(sid)                        
                        (txt, menu_item) = self.check_student(sid, ch_id)                        
                        await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))

        elif self.menu_id == keys_dict[lrn_start] :
            userdata = self.userdata
            if self.new_session and '@' in resp :
                resptxt = email_lookup(self.userdata, resptxt)
                resp = resp if resptxt=="" else resptxt
            if resp.isnumeric() and self.new_session :
                sid = int(resp)                
                usertype = rds_param(f"SELECT usertype FROM user_master WHERE studentid={resp} and client_name ='{self.client_name}';")
                if usertype==3:
                    await self.sender.sendMessage("Sorry your account is blocked, please contact the admin.")
                    await self.logoff()
                else:
                    #self.check_student(sid, chat_id)
                    (txt, menu_item) = self.check_student(sid, chat_id)                        
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))                    
                    self.menu_id = keys_dict[lrn_student]
            else:
                retmsg = "please enter your student id or email address :"

        elif self.menu_id == keys_dict[lrn_student] and resp in [option_mycourse, option_updateprogress, \
            option_faq, option_mychat, option_mychart, option_binduser, option_gethelp, option_info, option_back]:
            if (resp == option_back) or (resp == "0"):
                await self.logoff()
            elif resp == option_mycourse:
                date_today = datetime.datetime.now().date()
                yrnow = str(date_today.strftime('%Y'))
                course_list = [x for x in self.list_courseids if x[-4:]==yrnow]
                btn_course_list = build_menu(course_list, 1)
                txt = "Please select the course id from below:"
                #bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                self.menu_id = keys_dict[option_mycourse]

            elif resp == option_updateprogress:
                sid = self.student_id
                courseid = self.courseid
                cname = self.client_name
                query = f"select stage from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"
                stg = rds_param(query)
                query = f"select * from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"
                df = rds_df(query)
                if df is not None:
                    df.columns = bot_intance.userdata_cols
                    self.userdata = df
                self.stage_name = stg
                self.student_id = sid
                self.courseid = courseid
                self.load_tables()
                (txt, self.records ) = verify_student(self.client_name, self.userdata, sid, self.courseid, None)
                vars = display_progress(self.userdata, self.stagetable, sid, self.records, self.client_name, bot_intance.resp_dict, bot_intance.pass_rate)                
                retmsg  = vars['notification']
                retmsg += grad_pred_text(vars, self.client_name)
                self.menu_id = keys_dict[lrn_student]
            elif resp == option_faq:
                txt = 'These are the FAQs :'
                faq_menu = build_menu(ft_model.faq_list.copy(),1,option_back,[])
                #bot_prompt(self.bot, self.chatid, txt, faq_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(faq_menu))
                self.menu_id = keys_dict[option_faq]
            elif resp == option_gethelp:
                mentor_id = self.mentor_chatid()
                bot_intance.user_list[ self.chatid ][4] = "👋"
                if mentor_id <= 0:
                    mentor_id = adminchatid
                result = '<b>Learner needs assistance 👋</b>'
                result +='\n<pre>Course id    : ' +  self.courseid
                result += '\nStudent ID   : ' + str(self.student_id) 
                result += '\nStudent Name : ' + self.username + '</pre>'
                result += '\nContact : <a href=\"tg://user?id=' + str(self.chatid) + '">@' + self.chatname + '</a>'
                await bot.sendMessage(mentor_id,result,parse_mode='HTML')
                retmsg = "Please wait, our faculty admin will connect with you on a live chat"                
            elif resp == option_mychat:
                (status, info) = self.livechat()
                if status == 2:
                    txt = 'Chat with online learners 🗣'
                    await self.bot.sendMessage(self.chatid, txt, reply_markup = info )
                    self.menu_id = keys_dict[option_chatlist]
                else:
                    await self.sender.sendMessage(info)
            elif resp == option_mychart:            
                self.menu_id = keys_dict[lrn_student]
                if self.student_id > 0:
                    (df,f) = self.mcqas_chart()                    
                    await self.bot.sendPhoto(chat_id, f)
                    txt = 'You are at the main menu.'
                    #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
            elif resp == option_binduser:
                txt += "\nDo you want me to activate auto-login without entering student id each time ?"
                opt_yes = "Yes, enable auto-login"
                opt_no = "No, I would like to login manually each time"
                yesno_menu = build_menu([opt_yes,opt_no],1,option_back)
                #bot_prompt(self.bot, self.chatid, txt, yesno_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(yesno_menu))
                txt = ""
                self.menu_id = bot_intance.keys_dict[option_bind]
            elif resp == option_info:
                retmsg = self.session_info()

        elif self.menu_id == keys_dict[option_mycourse] :
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to this cohort : " + self.courseid
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = keys_dict[lrn_student]
            elif resp in self.list_courseids:
                n = self.list_courseids.index( resp )
                self.courseid = resp
                self.coursename = self.list_coursename[n]
                self.load_tables()
                sid = self.student_id
                #self.check_student(self.student_id, chat_id)                
                (txt, menu_item) = self.check_student(self.student_id, chat_id)
                if len(txt)>0:
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(menu_item))                
                txt = "You are now with this cohort : " + self.courseid
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = keys_dict[lrn_student]
                #self.new_session = True
                self.student_id = sid
            else:
                btn_course_list = build_menu(self.list_courseids, 1)
                txt = "Please select the course id from below:"
                #bot_prompt(self.bot, self.chatid, txt, btn_course_list)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_course_list))
                self.menu_id = keys_dict[option_mycourse]
                self.new_session = True

        elif self.menu_id == keys_dict[option_bind] :
            sid = str(self.student_id)            
            cid = self.courseid            
            if "yes," in resptxt:
                query = f"select * from user_master where binded=1 and chat_id={str(chat_id)} and client_name = '{self.client_name}';"
                df = rds_df(query)
                if df is None:
                    ulist = []
                else:
                    df.columns = bot_intance.usermaster_cols
                    ulist = [x for x in df.studentid if x != self.student_id]
                if ulist == []:
                    updqry = f"update user_master set binded=1, chat_id={str(chat_id)}, courseid='{cid}' where client_name = '{self.client_name}' and studentid={sid};"
                    rds_update(updqry)
                    txt = "Auto-Login option enabled"
                else:
                    txt = f"Sorry unable to bind due to more than one Student ID found under this telegram account : {ulist}"
            elif "no," in resptxt:
                updqry = f"update user_master set binded=0 where client_name = '{self.client_name}' and studentid={sid};"
                rds_update(updqry)
                txt = "Auto-Login option disabled"
            elif (resp == option_back) or (resp == "0"):
                txt = "you are back to main menu"
            #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
            await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
            syslog(str(self.chatid),txt)
            self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_fct]:
            df = rds_df(f"SELECT distinct courseid FROM userdata WHERE client_name = '{self.client_name}' ORDER BY courseid;")
            if df is None:
                course_list = self.list_courseids
            else:
                df.columns = ['courseid']
                userdata_courseids = [ x for x in df.courseid ]
                course_list = [ x for x in self.list_courseids if x in userdata_courseids ]
            btn_course_list = build_menu( course_list , 1)
            if resp == fc_userimport :                
                await self.sender.sendMessage("System has scheduled a job to update user master.")                
                job_request("ServiceBot",adminchatid, self.client_name,"mass_update_usermaster","")
            elif resp == fc_edxupdate :
                await self.sender.sendMessage("System has scheduled a job to import from LMS.")                
                job_request("ServiceBot",adminchatid, self.client_name,"edx_mass_import","")
            elif resp == fc_schedule :
                await self.sender.sendMessage("System has scheduled a job to import from google calendar.")                
                job_request("ServiceBot",adminchatid, self.client_name,"mass_update_schedule","")               
            elif resp == fc_intv :
                await self.sender.sendMessage("Processing intervention check now.")
                await auto_intervent(bot_intance.client_name, bot_intance.resp_dict, bot_intance.pass_rate, self.chatid)
                await self.sender.sendMessage("Auto intervention check completed")
            elif resp == fc_notf :     
                await self.sender.sendMessage("Processing reminder check now.")
                await auto_notify(bot_intance.client_name, bot_intance.resp_dict, bot_intance.pass_rate, self.chatid)
                await self.sender.sendMessage("Auto reminder check completed")
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = 1

        elif self.menu_id == keys_dict[option_pb]:
            if resp==pb_config:
                qry = "SELECT  b.course_id, b.mentor FROM userdata a INNER JOIN playbooks b "
                qry += "ON a.client_name=b.client_name AND a.courseid=b.course_id INNER JOIN user_master c "
                qry += "ON a.client_name=c.client_name AND a.studentid=c.studentid "
                qry += f"WHERE b.eoc=0 AND a.client_name='{self.client_name}' AND a.studentid={self.student_id};"
                df = rds_df(qry)
                if df is None:
                    n = 0
                else:
                    df.columns = ['course_id', 'mentor']
                    title = "List of active courses in the playbooks configurators."
                    html_msg_dict[title] = html_report(df, df.columns, [50, 25], 20)                    
                    n = len(df)
                retmsg = f"Total number of active courses = {n}"
                
            elif resp == pb_userdata:
                txt = "Let's take a look on the persona playbooks."
                playbooklist_menu = [[x] for x in self.list_courseids]
                playbooklist_menu.append([option_back])
                #bot_prompt(self.bot, self.chatid, txt, playbooklist_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbooklist_menu))
                self.menu_id = keys_dict[pb_userdata]
            elif resp == pb_riskuser:
                dt_str = str(datetime.datetime.now().date().strftime('%Y-%m-%d'))
                fn = f"risk_report_{self.student_id}_{dt_str}.html"                
                qry = f"SELECT courseid, studentid, username, risk_level, "
                qry += f"mcq_zero AS mcq_pending, mcq_failed, "
                qry += f"as_zero AS assignment_pending, as_failed AS assignment_failed "
                qry += f"FROM userdata WHERE client_name = '{self.client_name}' AND risk_level >0 "
                qry += f"ORDER BY courseid, studentid;"
                df = rds_df( qry)
                if df is None:
                    await self.sender.sendMessage("There is no information available at the moment")
                    return
                df.columns = ['courseid','studentid','username','risk_level','mcq_pending','mcq_failed','assignment_pending','assignment_failed']
                write2html(df, title=f"Learners at risk - dated {dt_str}", filename=fn)
                await bot.sendDocument(chat_id, document=open(fn, 'rb'))
            elif (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = 1

        elif self.menu_id == keys_dict[pb_userdata]:
            if self.load_courseinfo(resp) == 1:                
                self.courseid = resp
                txt = "You are looking at :\n" + resp
                #bot_prompt(self.bot, self.chatid, txt, course_menu)                
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(course_menu))
                self.menu_id = keys_dict[opt_pbusr]
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are in playbook maintainence mode.'
                #bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbook_menu))
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[opt_pbusr]:
            if resp == ps_userdata :
                query = "SELECT u.studentid, m.username, m.email, u.amt, u.stage FROM userdata u INNER JOIN user_master m ON u.client_name=m.client_name "
                query += f" AND u.studentid=m.studentid WHERE u.client_name='{self.client_name}' AND u.courseid = '{self.courseid}' "
                query += ''.join([ " and lower(m.email) not like '%" + x + "'"  for x in bot_intance.efilter])
                df = rds_df( query)
                if df is None:
                    retmsg = "There is no information for this course at the moment"
                else:
                    df.columns = ['studentid','username','email','amt','stage']
                    title = "List of learners from " + self.courseid
                    html_msg_dict[title] = html_report(df, df.columns, [9,20,28,8,9], 30)
                    n = len(df)
                    retmsg = f"Total number of active learners = {n}"
            elif resp == ps_schedule :
                if self.stagetable is None:
                    retmsg = "The schedule information is not available"
                else:
                    title = "Course Schedule for " + self.courseid
                    cols = ['id', 'stage', 'name', 'startdate', 'stagedate', 'IU']
                    df = self.stagetable[cols]
                    html_msg_dict[title] = html_report(df, cols, [5,5,10,10,10,40], 8)
            elif resp == ps_stage:
                if self.stagetable is None:
                    retmsg = "The unit guides information is not available"
                else:
                    title = "Unit guides for " + self.courseid
                    cols = ['stage', 'name', 'mcq', 'assignment']
                    df = self.stagetable[cols]
                    if len(df)==0:
                        retmsg = "The unit guides information is not available"
                    else:
                        html_msg_dict[title] = html_report(df, cols, [4,9,29,29], 8)
            elif resp == ps_mcqzero:
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with MCQ test pending for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'mcq_zero']
                    df = self.userdata[cols]
                    df = df[df.mcq_zero!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one missed the mcq test so far"
                    else:
                        df['mcq_zero'] = df.apply(lambda x: str(x['mcq_zero']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols, [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == ps_mcqfailed:
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with MCQ test failed for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'mcq_failed']
                    df = self.userdata[cols]
                    df = df[df.mcq_failed!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one failed the mcq test so far"
                    else:
                        df['mcq_failed'] = df.apply(lambda x: str(x['mcq_failed']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols,  [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == ps_aszero:
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with Assignment test pending for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'as_zero']
                    df = self.userdata[cols]
                    df = df[df.as_zero!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one missed the assignment test so far"
                    else:
                        df['as_zero'] = df.apply(lambda x: str(x['as_zero']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols,  [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif resp == ps_asfailed:
                if self.userdata is None:
                    retmsg = "Learners information is not available"
                else:
                    title = "Learners with Assignment test failed for " + self.courseid
                    cols = ['studentid', 'username', 'stage', 'as_failed']
                    df = self.userdata[cols]
                    df = df[df.as_failed!='[]']
                    n = len(df)
                    if n==0:
                        retmsg = "no one failed the assignment test so far"
                    else:
                        df['as_failed'] = df.apply(lambda x: str(x['as_failed']).replace(' ',''), axis=1)
                        html_msg_dict[title] = html_report(df, cols, [10, 15, 10, 40], 8)
                        retmsg = f"Total number of learners = {n}"
            elif (resp == option_back) or (resp == "0"):
                txt = 'You are in playbooks maintainence mode.'
                #bot_prompt(self.bot, self.chatid, txt, playbook_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(playbook_menu))
                self.menu_id = keys_dict[option_pb]

        elif self.menu_id == keys_dict[opt_stage] :
            txt = edit_fields(self.client_name, self.courseid, "stages", "stage", self.student_id, resp)
            #bot_prompt(self.bot, self.chatid, txt, course_menu)
            await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(course_menu))
            self.menu_id = keys_dict[opt_pbusr]

        elif self.menu_id == keys_dict[opt_analysis]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = 1
            elif self.load_courseinfo(resp) == 1:
                self.courseid = resp
                txt = 'Please select the following mode:'
                #bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(analysis_menu))
                self.menu_id = keys_dict[option_analysis]
            else:  
                newlist = [[x] for x in self.list_courseids if resptxt in x.lower()]
                if len(newlist)==0:
                    txt = 'Please select the following mode:'
                    #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                    self.menu_id = 1
                else:
                    txt = "Please select from the list of course id below:"
                    btn_list = build_menu([x for x in self.list_courseids if resptxt in x.lower()],1)
                    #bot_prompt(self.bot, self.chatid, txt,btn_list)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_list))
                    return

        elif self.menu_id == keys_dict[option_analysis]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = 1
            elif resp == ml_grading:
                ( sid_list , df )= self.grad_prediction()
                df.columns = ['Student ID#','Name', 'Prediction']
                title = "AI Grading for " + self.courseid
                html_msg_dict[title] = html_report(df, df.columns, [10, 15, 10], 20)
                btn_list = build_menu( sid_list, 6, option_back, [])
                self.records['progress_sid'] = sid_list
                n = len(sid_list)
                txt = f"total {n} learners in the list.\nSelect the student id to see the progress :"
                #bot_prompt(self.bot, self.chatid, txt, btn_list)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(btn_list))
                self.menu_id = keys_dict[opt_aig]
            elif resp == an_mcq:
                (tbl1,tbl2,tbl3) = analyze_cohort(self.courseid, self.userdata, self.bot, self.chatid)
                if tbl1 == []:
                    await self.sender.sendMessage("There is no information available at the moment")
                    return

                title  = "Assignment & MCQ Score Summary for\n" + self.courseid
                df =  pd.DataFrame( tbl1 )
                df.columns = ['MCQ Test #', 'Average Score' , 'Assignment Test #', 'Average Score']
                html_msg_dict[title] = html_report(df, df.columns, [15,15,15,15], 20)

                title  = "MCQ Grouping for " + self.courseid
                df =  pd.DataFrame( tbl2 )
                df.columns = ['Grouping']
                html_msg_dict[title] = html_report(df, df.columns, [120], 28)

                title  = "Assignment Grouping for " + self.courseid
                df =  pd.DataFrame( tbl3 )
                df.columns = ['Grouping']
                html_msg_dict[title] = html_report(df, df.columns, [120], 28)
                
            elif resp == an_mcqd:
                txt = "MCQ Difficulty Analysis by:"
                #bot_prompt(self.bot, self.chatid, txt, mcqdiff_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(mcqdiff_menu))
                self.menu_id = keys_dict[opt_mcqd]
            elif resp == an_chart:
                (df,f) = self.mcqas_chart(True)
                await self.bot.sendPhoto(chat_id, f)
                df.columns = ['Test/IU','mcq test','assignment test']
                cohort_id = piece(piece(self.courseid,':',1),'+',1)
                title = f"MCQ and Assignment scores for cohort {cohort_id}"
                html_msg_dict[title] = html_report(df, df.columns, [10, 10, 10], 15)

        elif self.menu_id == keys_dict[opt_mcqd]:
            if (resp == option_back) or (resp == "0"):
                txt = 'Please select the following mode:'
                #bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(analysis_menu))
                self.menu_id = keys_dict[option_analysis]
            elif resp == an_avgatt:            
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                df = mcq_analysis.top10attempts()
                if df is None:
                    retmsg = 'There is no data for this course.'                    
                else:
                    title = "MCQ Analysis Difficulty By MCQ Attempts"
                    cols = ['MCQ No. Question No.', 'Average Attempts']
                    df =  df[cols]
                    html_msg_dict[title] = html_report(df, cols, [25,18], 10)
            elif resp == an_avgscore:
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                df = mcq_analysis.top10score()
                if df is None:
                    retmsg = 'There is no data for this course.'
                else:
                    title = "MCQ Analysis Difficulty By MCQ Scores"
                    cols = ['MCQ No. Question No.', 'Average Score %', 'Average Attempts']
                    df = df[cols]
                    html_msg_dict[title] = html_report(df, df.columns, [25,18,18], 10)
            elif resp == an_mcqavg:
                df = self.userdata
                client_name = list(df['client_name'])[0]
                course_id = list(df['courseid'])[0]
                self.client_name = client_name
                self.courseid = course_id
                qry = f"select * from mcq_data where client_name = '{client_name}' and course_id = '{course_id}';"
                df = rds_df(qry)
                df.columns = get_columns("mcq_data")
                mcq_list = [x for x in df.mcq]
                sorted_list = sorted(list(set([int(x) for x in mcq_list]))) 
                mcq_list = [str(x) for x in sorted_list ] + [option_back]
                m = int((len(mcq_list)+4)/5)
                mcq_menu = [  mcq_list[n*5:][:5] for n in range(m) ]
                txt = "Please select from the list of MCQs below:"
                #bot_prompt(self.bot, self.chatid, txt, mcq_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(mcq_menu))
                self.menu_id = keys_dict[opt_mcqavg]

        elif self.menu_id == keys_dict[opt_mcqavg]:            
            if resp.isnumeric() :
                mcq_analysis.load_mcqdata(self.client_name, self.courseid)
                df = mcq_analysis.mcq_summary(int(resp))                
                if df is None:                
                    retmsg = 'There is no data for this course.'
                else:
                    title = f"MCQ Analysis Difficulty By MCQ Average for MCQ Test {resp}"                    
                    html_msg_dict[title] = html_report(df, df.columns, [10, 18, 18], 20)
            elif (resp == option_back) or (resp == "0"):
                self.menu_id = keys_dict[opt_mcqd]
                txt = "MCQ Difficulty Analysis by:"
                #bot_prompt(self.bot, self.chatid, txt, mcqdiff_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(mcqdiff_menu))

        elif self.menu_id == keys_dict[opt_aig]:
            if resp.isnumeric() :
                sid = int(resp)
                df = self.records['progress_df'][sid]
                title = self.records['progress_tt'][sid]
                html_msg_dict[title] = html_report(df, df.columns, [10,10,10,10,10], 10)
            elif (resp == option_back) or (resp == "0"):
                txt = 'Select your option:'
                #bot_prompt(self.bot, self.chatid, txt, analysis_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(analysis_menu))
                self.menu_id = keys_dict[option_analysis]
                return

        elif self.menu_id ==  keys_dict[option_usermgmt]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the main menu."
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = keys_dict[option_mainmenu]
                return
            if resp == option_searchbyname:
                txt = "Search Student-ID by name"
                #bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[option_searchbyname]                
            elif resp == option_searchbyemail:
                txt = "Search Student-ID by email"                
                #bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[option_searchbyemail]                
            elif resp == option_resetuser:
                txt = "Please enter valid Student-ID :"
                #bot_prompt(self.bot, self.chatid, txt, [[option_back]])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                self.menu_id = keys_dict[option_resetuser]                
            elif resp == option_admin_users:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and usertype=11 limit 50;"
                result = "List of admin users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 25)                
            elif resp == option_blocked_users:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and usertype=0 limit 50;"
                result = "List of blocked users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 25)                
            elif resp == option_binded_users :
                query = f"UPDATE user_master SET binded=0 WHERE chat_id=0 and client_name = '{self.client_name}';"
                rds_update(query)
                query = f"select studentid,username,email,chat_id from user_master where client_name = '{self.client_name}' and binded=1 limit 50;"
                result = "List of binded users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email','chat_id']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,30,40,20], 25)
            elif resp == option_active_users :
                sid_list = [ bot_intance.user_list[x][1] for x in list(bot_intance.user_list) ] + bot_intance.adm_list
                sid_list = list(set(sid_list))
                slist = ','.join([str(x) for x in sid_list])
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and "
                query += f"studentid in ({slist}) limit 50;"
                result = "List of active users (top 50)\n"
                df = rds_df(query)
                if df is None:
                    await self.sender.sendMessage("Sorry, no results found.")
                    return
                df.columns = ['studentid','username','email']
                title = result
                html_msg_dict[title] = html_report(df, df.columns, [10,20,40], 25)                
        elif self.menu_id in [keys_dict[option_searchbyname],keys_dict[option_searchbyemail]] :
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                #bot_prompt(self.bot, self.chatid, txt, users_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(users_menu))
                self.menu_id = keys_dict[option_usermgmt]
                return
            idx = [keys_dict[option_searchbyname],keys_dict[option_searchbyemail]].index(self.menu_id)
            if idx==0:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and lower(username) like '%{resptxt}%' ;"
                result = "Name search matching " + resp + "\n"
            if idx==1:
                query = f"select studentid,username,email from user_master where client_name = '{self.client_name}' and lower(email) like '%{resptxt}%' ;"
                result = "Email search matching " + resp + "\n"
            df = rds_df(query)
            if df is None:
                await self.sender.sendMessage("Sorry, no results found.")
                return
            df.columns = ['studentid','username','email']
            title = result
            html_msg_dict[title] = html_report(df, df.columns, [10,30,40], 20)            
            
        elif self.menu_id == keys_dict[option_resetuser]:
            if (resp == option_back) or (resp == "0"):
                txt = "You are back to the user management menu."
                #bot_prompt(self.bot, self.chatid, txt, users_menu)
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(users_menu))
                self.menu_id = keys_dict[option_usermgmt]
                return
            sid = 0
            if resp.isnumeric():
                sid = int(resp)
                if sid > 0:
                    query = f"select * from user_master where client_name = '{self.client_name}' and studentid = {resptxt} ;"
                    df = rds_df(query)
                    if df is None:
                        sid = 0
                if sid == 0:
                    retmsg = "Unable to find the matchnig record. Please try again."
                else:
                    df.columns = bot_intance.usermaster_cols
                    rec = df.iloc[0]
                    sid = rec['studentid']
                    self.student_id = sid
                    username = rec['username']
                    email = rec['email']
                    tid = rec['chat_id']
                    binded = 'Yes' if rec['binded']==1 else 'No'
                    telegid = str(tid) if rec['binded']==1 else 'None'
                    txt = f"Student-ID : #{sid}\nName : {username}\nEmail : {email}\nBinded :{binded}\nTelegramID : {telegid}\n"
                    query = "select distinct a.courseid from userdata a inner join user_master b "
                    query += " on a.client_name=b.client_name  and a.studentid=b.studentid where "
                    query += f" a.client_name = '{self.client_name}' and a.studentid={sid} " 
                    query += "order by a.courseid;"
                    df = rds_df(query)
                    if df is not None:
                        df.columns = ['courseid']
                        n=len(df)
                        if n > 0:
                            if n <=10:
                                txt += "Courses:\n" + '\n'.join([x for x in df.courseid])
                            else:
                                txt += f"You have {n} courses registered"                         
                    txt += "\nWhat would you like to do ?"
                    #bot_prompt(self.bot, self.chatid, txt, useraction_menu)
                    await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup(useraction_menu))
            elif resp == opt_blockuser:
                query = f"update user_master set usertype = 0 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been blocked."
            elif resp == opt_setadmin:
                query = f"update user_master set usertype = 11 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been set as admin."
            elif resp == opt_setlearner:
                query = f"update user_master set usertype = 1 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User with Student-ID {self.student_id} has been set as learner."
            elif resp == opt_resetemail:
                retmsg = "Please enter the new email address:"
                self.menu_id = bot_intance.keys_dict[opt_resetemail]
            elif resp == opt_unbind:
                query = f"update user_master set binded = 0, chat_id = 0 where client_name = '{self.client_name}' and studentid = {self.student_id} ;"
                rds_update(query)
                retmsg = f"User telegram account has been unbinded from Student-ID {self.student_id}."

        elif self.menu_id == keys_dict[opt_resetemail]:
                if '@' in resp:
                    query = f"update user_master set email = '{resp}' where client_name = '{self.client_name}' and studentid = {self.student_id};"
                    rds_update(query)
                    retmsg = f"Email address for Student-ID {self.student_id} has been set to {resp}."
                self.menu_id = bot_intance.keys_dict[option_resetuser]

        elif self.menu_id == keys_dict[option_chatlist]:
            if chat_id in bot_intance.chat_list:
                tid = bot_intance.chat_list[ chat_id ]
                await bot.sendMessage(tid , resp)
                txt = "(type bye when you want to end the conversation)"
                #bot_prompt(self.bot, self.chatid, "(type bye when you want to end the conversation)", [['bye']])
                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([['bye']]))
                self.menu_id = bot_intance.keys_dict[option_chat]
            else:
                rlist = resp.replace('     ','*').split('*')
                if len(rlist)>=2 :
                    sid = rlist[1]
                    tid = rlist[3]
                    if sid.isnumeric():
                        tid = int(tid)
                        if tid in bot_intance.chat_list:
                            retmsg = "User " + sid + " is on another conversation."
                        else:
                            sid = int(sid)
                            (status, info) = self.livechat( sid, tid )
                            if status == 1:
                                tid = bot_intance.chat_list[self.chatid]
                                if self.chatid in bot_intance.user_list:
                                    user_from = bot_intance.user_list[self.chatid][2]
                                else:
                                    user_from = self.username
                                txt = '❚█══ Live Chat ══█❚\nHi ' + info + ', you are in the live chat with : <a href=\"tg://user?id=' + str(self.chatid) + '">' + user_from + '</a>'
                                await self.bot.sendMessage(tid, txt, parse_mode='HTML')
                                txt = '❚█══ Live Chat ══█❚\nHi, you are in the live chat with : <a href=\"tg://user?id=' + str(tid) + '">' + info + '</a>'
                                await self.bot.sendMessage(self.chatid, txt, parse_mode='HTML')
                                txt = "(type bye when you want to end the conversation)"
                                await self.bot.sendMessage(self.chatid, txt, reply_markup=self.reply_markup([[option_back]]))
                                self.menu_id = keys_dict[option_chat]
                            else:
                                await self.sender.sendMessage(info)
                            
                elif (resp == option_back) or (resp == "0"):
                    if self.is_admin :
                        #bot_prompt(self.bot, self.chatid, "You are back in the main menu", mentor_menu)
                        await self.bot.sendMessage(self.chatid, 'You are back in the main menu', reply_markup=self.reply_markup(mentor_menu))
                        self.menu_id = 1
                    else:
                        #bot_prompt(self.bot, self.chatid, 'bye', self.menu_home)
                        await self.bot.sendMessage(self.chatid, 'bye', reply_markup=self.reply_markup(self.menu_home))
                        self.menu_id = keys_dict[lrn_student]

        elif self.menu_id == keys_dict[option_chat]:
            if self.chatid in [d for d in bot_intance.chat_list]:
                tid = bot_intance.chat_list[self.chatid]
                if resp.lower() == 'bye':
                    tid = self.endchat()
                    if tid > 0:
                        txt = "Live chat session disconnected. 👋"
                        await self.bot.sendMessage(tid, txt)
                        await self.bot.sendMessage(chat_id, txt)                    
                    if self.is_admin :
                        txt = "You are back in the main menu"
                        bot_intance.user_list[ tid ][4] = ""         
                        self.menu_id = 1
                    else:
                        txt = "bye"                        
                        self.menu_id = keys_dict[lrn_student]
                    #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                else:
                    await bot.sendMessage(tid, resp)
                    txt = ''
            else:
                if self.is_admin :
                    txt = "Welcome back to main menu"
                    #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                    self.menu_id = 1
                else:
                    txt = "Live chat has been ended."
                    #bot_prompt(self.bot, self.chatid, 'Live chat has been ended.', self.menu_home)
                    await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                    self.menu_id = keys_dict[lrn_student]

        elif chat_id in bot_intance.chat_list and (resp.strip() != "") :
            if resp.lower() == 'bye':
                tid = self.endchat()
                if tid > 0:
                    txt = "Live chat session disconnected. 👋"
                    await self.bot.sendMessage(tid, txt)
                    await self.bot.sendMessage(chat_id, txt)                
            else:
                if self.menu_id != keys_dict[option_chat]:
                    #bot_prompt(self.bot, self.chatid, "(type bye when you want to end the conversation)", [['bye']])
                    await self.bot.sendMessage(chat_id, "(type bye when you want to end the conversation)", reply_markup=self.reply_markup([['bye']]))
                    self.menu_id = bot_intance.keys_dict[option_chat]
                tid = bot_intance.chat_list[chat_id]
                await bot.sendMessage(tid, resp)

        elif self.menu_id == keys_dict[option_faq] and resp in ['0', 'exit', option_back]:
            #bot_prompt(self.bot, self.chatid, "FAQ option is closed.", self.menu_home)
            await self.bot.sendMessage(chat_id, "FAQ option is closed.", reply_markup=self.reply_markup(self.menu_home))
            self.menu_id = keys_dict[lrn_student]

        elif (self.menu_id != keys_dict[option_chat]) and (self.chatid in [d for d in bot_intance.chat_list]):
            tid = bot_intance.chat_list[ chat_id ]
            await bot.sendMessage(tid , resp)
            #bot_prompt(self.bot, self.chatid, "you said : " + resp, [['bye']])
            await self.bot.sendMessage(chat_id,"you said : " + resp, reply_markup=self.reply_markup([['bye']]))
            self.menu_id = keys_dict[option_chat]

        elif self.menu_id > 0:
            syslog( str(self.student_id) , "Q:" + resp )
            (txt,recommendation) = self.runfaq(resp)            
            if (txt != ""):
                #bot_prompt(self.bot, self.chatid, txt, self.menu_home)
                await self.sender.sendMessage(txt)
                #await self.bot.sendMessage(chat_id, txt, reply_markup=self.reply_markup(self.menu_home))
                self.menu_id = keys_dict[lrn_student]
            if len(recommendation) > 0:
                rec_menu = build_menu(recommendation,1,option_back,[])
                await self.bot.sendMessage(self.chatid, "You might want to ask :", reply_markup=self.reply_markup(rec_menu))
                txt = ""
                self.menu_id = bot_intance.keys_dict[option_faq]
            # zz

        for title in list(html_msg_dict):
            for msg in html_msg_dict[title] :
                txt = "<b>" + title + "</b>\n" + "<pre>" + msg +  "</pre>"
                await bot.sendMessage(chat_id, txt, parse_mode='HTML')                

        while retmsg != "":
            txt = retmsg[:4000]
            retmsg = retmsg[4000:]
            await self.sender.sendMessage(txt)
        return

def syslog(msgtype, message):
    global vmbot,bot_intance
    if message == "":
        return
    date_now = time.strftime('%Y%m%d', time.localtime() )
    time_now = time.strftime('%H%M%S', time.localtime() )
    try:
        query = """insert into syslog(date,time,type,message) values(_d,"_t","_x","_y");"""
        query = query.replace('_d',date_now)
        query = query.replace('_t',str(time_now))
        query = query.replace('_x',msgtype)
        query = query.replace('_y',message)
        rds_update(query)
    except:
        #print("Error for ", msgtype, message)
        pass
    return
   
def verify_student(cname, userdata, student_id, courseid, stagedf):
    global vmbot,bot_intance
    vars = dict()
    amt = 0    
    sid = int(student_id)
    if userdata is None:
        query = f"select * from userdata where client_name = '{cname}' and studentid={sid} and courseid='{courseid}';"    
        df = rds_df(query)
        if df is not None:
            df.columns = bot_intance.userdata_cols
            userdata = df
    if userdata is None:
        return (msg, vars)
        
    vars = load_vars(userdata, student_id)    
    if vars == {}:
        return (f"Sorry there is no data for this student id {student_id}", vars)
    amt = vars['amt']
    student_name = vars['username']
    stage = vars['stage']
    grade = vars['grade']
    if stagedf is None:
        condqry = f" client_name = '{cname}' and courseid = '{courseid}'"
        query = "select * from stages where " + condqry 
        stage_table = rds_df(query)    
        if stage_table is None:        
            msg = f'Unable to load stage table for course-id {courseid}' 
            return (msg, vars)
        stage_table.columns = bot_intance.stages_cols
    else:
        stage_table = stagedf.copy()
        
    stage_list = [x for x in stage_table.name]
    date_list = [x for x in stage_table.stagedate]    
    vars['stage_dates'] = date_list
    vars['stage_names'] = stage_list
    msg = '\n\nHi ' + student_name + " !\n"
    if stage is None:
        stage = stage_list[0]
        vars['stage'] = stage
    msg += 'You are currently on stage ' + stage + "\n\n"    
    
    cohort_id = piece(piece(courseid,':',1),'+',1)
    module_code = piece(cohort_id,'-',0)
    qry = f"select assistance_email from course_module where client_name = '{cname}' and module_code = '{module_code}';"
    asst_email = rds_param(qry)
    qry = f"select mentor_email from course_module where client_name = '{cname}' and module_code = '{module_code}';"
    mentor_email = rds_param(qry)
    vars['asst_email'] = asst_email
    vars['mentor_email'] = mentor_email
    return (msg, vars)

def evaluate_progress(vars,iu_list,passingrate,var_prefix,var_title):    
    avg_prefix = var_prefix + "_avg"
    att_prefix = var_prefix + "_attempts"    
    score_zero = [] ; iu_score = [] ; iu_attempts = [] ; score_pass = [] ; score_failed = []
    score_avg = 0 ; tt = '' ; iu_cnt = 0 ; attempts_balance = "" 
    if iu_list != '0' :
        iu_attempts = [ vars[x] for x in [ att_prefix + x for x in iu_list.split(',') ]]
        iu_vars = sorted_numlist( str(iu_list) ) 
        iu_att = dict(zip(iu_vars, iu_attempts))
        score_pass = [ x for x in iu_vars if vars[avg_prefix + str(x)]>=passingrate]
        score_failed = [ x for x in iu_vars if (iu_att[x] > 0) and (vars[avg_prefix + str(x)] < passingrate) and (vars[avg_prefix + str(x)] >= 0)]
        score_zero = [ x for x in iu_vars if (iu_att[x] == 0) and (vars[avg_prefix + str(x)] == 0)]
        iu_score = [ vars[x] for x in [ avg_prefix + x for x in iu_list.split(',') ]]  
        iu_score = [ eval(str(x)) for x in iu_score]
        iu_arr = dict(zip(iu_vars, iu_score))
        iu_attempts = [ 1 if iu_arr[x]>0 else iu_att[x] for x in iu_att ]
        score_avg = sum(iu_score)/len(iu_score) if iu_score != [] else 0
        tt += "\n" + var_title + " average test score : " + "{:.2%}".format(score_avg)
        if len(score_pass) > 0:
            tt += "\n☑ " + var_title + " test passed : " + str(score_pass)
        if len(score_failed) > 0:
            tt += "\n‼ " + var_title + " test failed : " + str(score_failed)
        if len(score_zero) > 0:
            tt += "\n☐ " + var_title + " test pending : " + str(score_zero)
        iu_cnt = len(score_pass) + len(score_failed)
        m = 4 if var_prefix=="mcq" else 1
        attempts_balance = "".join([ ("\n" + var_title + ' #'+str(x) + " has " + str(m-vars[att_prefix + str(x)])+" attempts left"  ) for x in iu_vars \
            if vars[avg_prefix + str(x)] < passingrate ])        
    return (tt, score_avg, score_zero, score_pass, score_failed, iu_score , iu_attempts, iu_cnt, attempts_balance)

def get_stageinfo(vars, pass_rate, amt, stagecode, mcqvars, asvars):
    ma_list = [] ; list_att = [] ; tt = "" ; t1 = '' ; t2 = '' 
    mcnt = 0 ; mcq_avg = 0 ; mcq_zero = [] ; mcq_pass = [] ; mcq_failed = [] ; mbal = ""
    acnt = 0 ; as_avg = 0 ; as_zero = [] ; as_pass = [] ; as_failed = [] ; abal = ""
    mcq_att = [] ; as_att = []
    (t1, mcq_avg, mcq_zero, mcq_pass, mcq_failed, mcqas_list1, mcq_att, mcnt, mbal) = evaluate_progress(vars, mcqvars, pass_rate, 'mcq','MCQ')
    ma_list += mcqas_list1
    list_att += mcq_att
    (t2, as_avg, as_zero, as_pass, as_failed, mcqas_list2, as_att, acnt, abal) = evaluate_progress(vars, asvars, pass_rate, 'as','Assignment')
    ma_list += mcqas_list2
    list_att += as_att
    mm = len(ma_list)
    has_score = 1 if mm > 0 else 0
    ascore  = sum(ma_list)/mm if mm > 0 else 0
    ascore = round(ascore*100)/100
    mcqas_comp = 1 if len([n+1 for n in range(len(list_att)) if list_att[n]==0])==0 else 0
    max_att = 0 if len(list_att)==0 else max(list_att)
    
    pass_stage = 0
    if stagecode.lower()=="soc" and amt == 0:
        pass_stage = 1
    if (mcqas_comp==1) and (ascore>= pass_rate) and (mm>0):
        pass_stage = 1
    if "eoc" in stagecode.lower():
        pass_stage = 0 if amt > 0 else 1
        
    tt = t1 + t2
    risk_level = 0
    if len(mcq_zero) > 0:
        risk_level = 1
    if len(as_zero) > 0:
        risk_level = 1
    if (mm > 0) and (ascore < pass_rate) and (max_att<4):
        risk_level = 2
    if (mm > 0) and (ascore < pass_rate) and (ascore > 0):        
        risk_level = 2
    if len(mcq_zero)>0 and (ascore==0):
        risk_level = 3
    if amt > 0 :
        pass_stage = 0

    return (pass_stage, has_score, ascore, ma_list, max_att, list_att, mcq_avg, mcq_zero, mcq_pass, mcq_failed, mcq_att, mcnt, mbal, \
        as_avg, as_zero, as_pass, as_failed, as_att, acnt, abal, mcqas_comp, risk_level, tt)

def load_progress(df, student_id, vars, client_name, resp_dict, pass_rate, stagedf):
    if (df is None) or (vars == {}) or (stagedf is None):
        return ("", "", vars)
    rec = df[df.studentid==student_id].iloc[0]
    courseid = rec['courseid']    
    sid = student_id
    dtnow = datetime.datetime.now().date().strftime('%Y%m%d')
    list1=[datetime.datetime.strptime(dt,"%d/%m/%Y").strftime('%Y%m%d') for dt in stagedf.startdate]
    list2=[x for x in list1 if x < dtnow ]
    if list2==[]:
        return ("", "", vars)
    stagebyschedule = stagedf.iloc[ (len(list2) - 1) ]['name']
    stg_list = [x for x in stagedf.stage]    
    stglen = len(stg_list)
    if stglen==0:
        return ("", "", vars)
    df = stagedf.copy()
    try:
        begin_date_list = ['' if x is None else x[:10] for x in df.startdate]
    except:
        return ("", "", vars)
    
    #if (date_today > last_date):        
    #    return ("_eoc_", "", vars)
    if list1[0] > dtnow:
        return ("_soc_", "", vars)
        
    due_date_list = [x for x in df.stagedate]
    stage_names_list = [x for x in df.name]
    stage_desc_list = [x for x in df.desc]
    stage_daysnum_list = [x for x in df.days]
    mcqvars_list = [x for x in df.mcq]
    mcqlist_max = max(mcqvars_list)    
    asvars_list = [x for x in df.assignment]
    aslist_max = max(asvars_list)    
    amt = vars['amt']    

    try:
        missing_dates = vmedxlib.sms_missingdates(client_name, courseid, sid, cols)
    except:
        missing_dates = ['' for n in range(stglen)]
        
    pm_stage = vmedxlib.sms_pmstage(client_name, courseid)
    att_rate = vmedxlib.sms_att_rate(client_name, courseid,sid)
    missing_stages = []
    stagebyprogress = ""
    statusbyprogress = ""
    mcnt = 0
    acnt = 0
    mcq_avg = 0
    as_avg = 0
    overall_passed = 1    
    missed_stage = ""
    stgcode = ""
    txt = ''
    stage_date = ""
    stagebyprogress = ""
    mcq_pending = []
    assignment_pending = []
    mfail = []
    afail = []
    n1 = 0
    for n in range(stglen):
        stagecode = stg_list[n]        
        stagename = stage_names_list[n]
        stage_missing = missing_dates[n]
        f2f_missing = 0 if stage_missing=='' else 1
        if (f2f_missing==1):
            missing_stages.append(stagecode) 
        stg_date = due_date_list[n]
        duedate = due_date_list[n]
        stagedesc = stage_desc_list[n]
        if '(' in stagedesc:
            stagedesc = stagedesc.split('(')[0]
        #stagedesc = stagedesc.replace('?','').replace('⭐️','').replace('(','').replace(')','').replace('Asynchronous','')
        mcqvars = mcqvars_list[n]
        asvars = asvars_list[n]
        if ('SA' in stagecode) or (stagecode=='EOC'):
            mcqvars = mcqlist_max
            asvars = aslist_max
        (pass_stage,has_score,avg_score,mcqas_list,max_attempts,list_attempts,mcq_avg,mcq_zero,mcq_pass,mcq_failed,mcq_attempts,mcnt,\
        mcq_att_balance,as_avg,as_zero,as_pass,as_failed,as_attempts,acnt,as_att_balance,mcqas_complete,risk_level,tt) \
            = get_stageinfo(vars, pass_rate, amt, stagecode, mcqvars, asvars)
        #print(n, stagecode,stagename, stagebyschedule, statusbyprogress, mcqvars,pass_stage,asvars)
        #if stglen==(n+1):
            #stagename = stagebyschedule = statusbyprogress = stage_names_list[n]
            #stagename = statusbyprogress = stage_names_list[n]
            #statusbyprogress = f"{stagename} ({stagedesc})"
            #break
        if (stagebyprogress == "") and (pass_stage == 0):
            stagebyprogress = stagename
            statusbyprogress = f"{stagename} ({stagedesc})"
            overall_passed = 0            
            mcq_pending = mcq_zero
            assignment_pending = as_zero
            mfail = mcq_failed
            afail = as_failed
            stgcode = stagecode
        if stagebyschedule == stagename: 
            mcqvars = mcqvars_list[n]
            asvars = asvars_list[n],
            stagedesc = stage_desc_list[n]
            stg_date = due_date_list[n]
            duedate = due_date_list[n]
            date_from = begin_date_list[n]
            stage_days = stage_daysnum_list[n]
            vars['stage'] = stagebyschedule
            txt = tt + "\n\n"
            n1 = n
            if stagebyprogress != "":                
                mcq_pending = mcq_zero
                assignment_pending = as_zero
                mfail = mcq_failed
                afail = as_failed
                stage_date = stg_date
                break    
        if n1 > 0 and n == n1:
            if (stagebyprogress == ""):
                stagebyprogress = stagename
                statusbyprogress = f"{stagename} ({stagedesc})"                
            break

    f2f_stage = ','.join(missing_stages)
    pass_stage = overall_passed
    stg_date = stage_date
    mcqdate = stg_date
    asdate  = stg_date
    eldate  = stg_date
    fcdate  = stg_date
    stage = stagebyschedule
    mcq_zero = mcq_pending
    as_zero = assignment_pending
    mcq_failed = mfail
    as_failed = afail
    stagecode = stgcode
    stage_desc = stagedesc
    stagebyprogress = stage if stagebyprogress=='' else stagebyprogress
    vars['stage'] =  stagebyprogress

    if stagecode=="":
        if stagebyprogress in stage_names_list:
            n = stage_names_list.index(stagebyprogress)
            stagecode = stg_list[n]
            stage_desc = stage_desc_list[n]
    
    mcqlist = list(set(mcq_failed + mcq_zero))
    aslist = list(set(as_failed + as_zero))
    txt_hdr = resp_dict['stg0']
    if ("eoc" in stagebyschedule.lower()) and (pass_stage == 1) and (att_rate >=0.75):
        txt = ""
        txt_hdr = "_eoc_"
    else:
        txt_hdr += resp_dict['stg1']
    if '{stage_desc}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stage_desc}' , stage_desc)
    if '{username}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{username}' , vars['username'])
    if '{stage}' in txt_hdr:        
        txt_hdr = txt_hdr.replace('{stage}' , stage)
    if '{stagebyschedule}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stagebyschedule}' , stagebyschedule)
    if '{stagebyprogress}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{stagebyprogress}' , statusbyprogress)
    if '{lf}' in txt_hdr:
        txt_hdr = txt_hdr.replace('{lf}' , '\n')
        
    if (pm_stage==1) and (att_rate < 0.75):
        risk_level = 3
        
    query = f"update userdata set risk_level = {risk_level}, stage = '{stagebyprogress}', mcq_zero = '{mcq_zero}' "
    query += f" ,mcq_failed = '{mcq_failed}', as_zero = '{as_zero}', as_failed = '{as_failed}' "
    query += f" where client_name='{client_name}' and courseid='{courseid}' and studentid={student_id};"
    try:
        rds_update(query)
    except:
        pass
        
    for vv in ['stage', 'stagecode', 'mcqdate', 'asdate', 'eldate', 'fcdate', 'avg_score', 'mcqas_complete', 'mcq_pass', 'mcq_failed', 'missed_stage', "aslist", \
            'has_score', 'pass_stage', 'max_attempts', 'mcqas_list', 'mcq_zero', 'mcq_avg', 'as_pass', 'as_failed', 'risk_level', 'stg_list', 'mcqlist', 'pm_stage', 'att_rate', \
            'mcnt', 'acnt', 'as_avg', 'as_zero', 'f2f_stage', 'stagebyprogress', 'mcq_attempts',  'mcq_att_balance', 'as_att_balance', 'as_attempts', 'duedate'] :
        vars[vv] = eval(vv)
    return (txt_hdr, txt, vars)

def display_progress(df, sdf, sid, vars, client_name, resp_dict, pass_rate=0.7):
    if len(list(vars)) == 0:
        return "Your information is incomplete, please do not proceed and inform you faculty admin."
    (txt1, txt2, vars) = load_progress(df, sid, vars, client_name, resp_dict, pass_rate, sdf)
    txt = txt1 + txt2
    
    vars['course_alive'] = 1
    if txt == "":
        vars['notification'] = "Your information is incomplete, please do not proceed and inform you faculty admin."
        vars['course_alive'] = 0
        return vars
    if txt == "_soc_":
        vars['notification'] = "Hi, the course is not yet ready for now."
        vars['course_alive'] = 0
        return vars
    if txt == "_eoc_":
        vars['notification'] = "Congratulations, you have reached the end of the course."
        vars['course_alive'] = 0
        return vars
    cid = vars['courseid']
    sid = vars['studentid']
    stg = vars['stage']
    stagebyprogress = vars['stagebyprogress']
    stagecode = vars['stagecode']
    stg_list = vars['stg_list']
    pass_stage = vars['pass_stage']
    pmlaststage = [ x for x in stg_list if x[:2]=='PM' ][-1]
    f2f_stage = vars['f2f_stage']
    f2f_error = 0 if f2f_stage=="" else 1
    risk_level = vars['risk_level']
    mcq_zero = vars['mcq_zero']
    mcq_failed = vars['mcq_failed']
    mcq_att = vars['mcq_attempts']
    mcqlist = vars['mcqlist']
    as_zero = vars['as_zero']
    as_failed = vars['as_failed']    
    duedate = vars['duedate']
    amt = vars['amt']
    att_rate = vars['att_rate']
    pm_stage  = vars['pm_stage']
    if (pm_stage==1) and (att_rate < 0.75):
        f2f_error=1
    if vars['has_score'] == 1:
        txt += resp_dict['avg_score']
    if stg.lower() == "soc days":
        if amt == 0 :
            resp_amt0 = resp_dict['amt0']
            txt += resp_amt0 + "\n\n"
        elif amt > 0:
            resp_amt1 = resp_dict['amt1']
            txt += resp_amt1 + "\n\n"
    else:
        if amt > 0 :
            resp0 = resp_dict['resp0']
            txt += resp0 + "\n\n"

    if vars['mcqas_complete']==1 and vars['avg_score']>= pass_rate :
        #resp1 = resp_dict['resp1']
        resp10 = resp_dict['resp10']
        txt += resp10 + "\n\n"
    
    if f2f_error == 1:
        if pm_stage==0:
            resp11 = resp_dict['resp11']            
            txt += resp11 + '\n\n'
        else:
            if att_rate >= 0.75:
                resp11 = resp_dict['resp11']                
                txt += resp11 + '\n\n'
            else:
                resp12 = resp_dict['resp12']
                txt += resp12 + '\n\n'

    if '{lf}' in txt:
        txt = txt.replace('{lf}' , '\n')
    if '{f2f_stage}' in txt:
        txt = txt.replace('{f2f_stage}' , f2f_stage)
    if '{stage}' in txt:
        txt = txt.replace('{stage}' , stg)
    if '{avg_score}' in txt:
        txt = txt.replace('{avg_score}' , "{:.2%}".format(vars['avg_score']))
    if '{amt}' in txt:        
        txt = txt.replace('{amt}' ,  "{:8.2f}".format(amt).strip() )
    if '{mcqdate}' in txt:
        txt = txt.replace('{mcqdate}' , vars['mcqdate'])
    if '{asdate}' in txt:
        txt = txt.replace('{asdate}' ,  vars['asdate'] )
    if '{eldate}' in txt:
        txt = txt.replace('{eldate}' ,  vars['eldate'] )
    if '{fcdate}' in txt:
        txt = txt.replace('{fcdate}' ,  vars['fcdate'] )
    if '{duedate}' in txt:
        txt = txt.replace('{fcdate}' ,  duedate )
    if '{due_date}' in txt:
        txt = txt.replace('{fcdate}' ,  duedate )
    vars['notification'] = txt
    
    #query = f"update userdata set risk_level = {risk_level}, stage = '{stagebyprogress}', mcq_zero = '{mcq_zero}' "
    #query += f" ,mcq_failed = '{mcq_failed}', as_zero = '{as_zero}', as_failed = '{as_failed}' "
    #query += f" where client_name='{client_name}' and courseid='{cid}' and studentid={sid};"
    #try:
        #rds_update(query)
    #except:
        #print(query)
        #syslog(client_name,f"unable to execute sql\n{query}")
    return vars

def grad_pred_text(vars, client_name, use_neural_network = False):
    txt = ""
    #use_neural_network = False # True or False
    if use_neural_network:                    
        #global nn_model        
        pass
        # list_avg = lambda x : 0 if len(x)==0 else sum(x)/len(x)
        # if (nn_model.model_name != "") and ((vars['mcnt'] + vars['acnt']) >0):
        #    mavg = vars['mcq_avg']
        #    mcqavgatt = list_avg( vars['mcq_attempts'] )
        #    mcqmaxatt = max( vars['mcq_attempts'] )
        #    aavg = vars['as_avg'] 
        #    acnt = vars['acnt']  
        #    as_avgatt = list_avg( vars['as_attempts'] )
        #    as_maxatt = max( vars['as_attempts'] )
        #    grad_pred_nn = nn_model.pred(client_name,mavg,mcqavgatt,mcqmaxatt,aavg,as_avgatt,as_maxatt,acnt)
        #    txt += "\n\nAI grading prediction : " +  "{:.2%}".format(grad_pred[0]) + "\n\n"
    else:
        global dt_model 
        if (dt_model.model_name != "") and ((vars['mcnt'] + vars['acnt']) >0):
            grad_pred = dt_model.predict(vars['mcq_avg'] , vars['as_avg'], 13)
            txt += "\n\nAI grading prediction : " +  "{:.2%}".format(grad_pred[0]) + "\n\n"            
    return txt
    
def load_vars(df, sid):
    if df is None:
        return {}
    ulist = [x for x in df.studentid]
    if sid not in ulist:
        return {}
    vars = df[df.studentid==sid].iloc[0].to_dict()
    for x in list(vars):   
        if (x in ['amt', 'grade']) or ('_avg' in x):
            vars[x] = eval(str(vars[x]))
    vars['mcq_due_dates'] = []
    vars['as_due_dates'] = []
    #printdict(vars)
    return vars

def job_request(bot_req,chat_id,client_name,func_req,func_param):
    if (func_req == "") or ('.db' in vmsvclib.rds_connstr):
        return
    gen_job_id = lambda : (''.join(random.choice( "ABCDEFGHJKLMNPQRTUVWXY0123456789" ) for i in range(12))).upper()    
    date_now = time.strftime('%Y%m%d', time.localtime() )
    time_now = time.strftime('%H%M%S', time.localtime() )
    job_id = gen_job_id()
    try:
        query = 'insert into job_list(date,time_start,time_end,job_id,client_name,chat_id,'
        query += 'bot_req, func_req, func-param, status, message)'
        query += ' values(_d, "_t", "0", "_j", "_c", _x, "_b", "_f", "_p","open","");'
        query = query.replace('_d',date_now)
        query = query.replace('_t',str(time_now))
        query = query.replace('_j',job_id)
        query = query.replace('_c',client_name)
        query = query.replace('_x',str(chat_id))
        query = query.replace('_b',bot_req)
        query = query.replace('_f',func_req)
        query = query.replace('_p',func_param)
        query = query.replace('-p','_p')
        rds_update(query)
        #print(query)
    except:
        print(f"Error for job {job_id} on function {func_req}")
    return

def edxapi_getuser(headers, url):
    edx_info = {}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:        
        data = response.content.decode('utf-8')
        if len(data)>0:
            edx_info = json.loads(data)
    return edx_info

def rds_loadcourse(client_name, stud):
    course_id_list = []
    course_name_list = []
    #qry0 = "SELECT DISTINCT ud.courseid,pb.course_name,pb.eoc FROM userdata ud INNER JOIN playbooks pb "
    qry0 = "SELECT DISTINCT ud.courseid, pb.course_name, pb.eoc,ud.risk_level FROM userdata ud INNER JOIN playbooks pb "
    qry0 += "ON ud.client_name=pb.client_name AND ud.courseid=pb.course_id INNER JOIN "
    qry0 += "course_module m ON m.client_name=pb.client_name AND m.module_code=pb.module_code "
    #qry = qry0 + f"WHERE pb.eoc=0 and m.enabled=1 and ud.studentid={str(stud)} and ud.client_name='{client_name}';"
    qry = qry0 + f"WHERE m.enabled=1 and ud.studentid={str(stud)} and ud.client_name='{client_name}';"
    df = rds_df(qry)
    course_id_list = []
    course_name_list = []
    logon_selection = []
    if df is not None:
        df.columns = ['courseid','course_name','eoc', 'risk_level']
        course_id_list = [x for x in df.courseid]
        course_name_list = [x for x in df.course_name]
        eoc_list = [x for x in df.eoc]
        risklvl_list = [x for x in df.risk_level]
        m = len(course_id_list)        
        for n in range(m):
            cid = course_id_list[n]            
            rlvl = risklvl_list[n]
            try:
                eoc = vmedxlib.edx_endofcourse(client_name, cid)
            except:
                eoc = eoc_list[n]
            if eoc == 0:
                logon_selection.append(cid)                
            if eoc==1:
                eoc_gap = vmedxlib.edx_eocgap(client_name, cid, 7)
                if (eoc_gap == 1) and (rlvl > 0):
                    logon_selection.append(cid)                    
    return (course_id_list, course_name_list, logon_selection)

def analyze_cohort(course_id, userdata, bot, chat_id):
    global vmbot, dt_model
    try:
        x = dt_model.score
    except:
        dt_model = vmaiglib.MLGrader()
        dt_model.load_model("dt_model.bin")
        
    itemlist = lambda x,y : "\n".join([ ",".join(x[n*y:][:y]) for n in range(int((len(x)+y-1)/y))])
    above_70 = lambda x : True if x >= 0.7 else False
    between_3070 = lambda x : True if (x > 0.3 and x < 0.7) else False
    below_30 = lambda x : True if x <= 0.3 else False
    above_2 = lambda x : True if x >= 2 else False
    
    def report_lines(udict, rdict, rpt_summary, title, lambda_fn, fmt = '{:.2%}'):
        rlist = [ r for r in rdict if lambda_fn(rdict[r]) ]
        if rlist!=[]:
            n = len(rlist)
            rpt_summary.append(title + f" ({n} learners)")
            for r in rlist:
                lines = (str(r) + ' '*8)[:8] + (udict[r] + ' '*30)[:30] + fmt.format(rdict[r])
                rpt_summary.append( lines )
            rpt_summary.append("")
        return rpt_summary

    result_table = [ [' ' for m in range(4)] for n in range(13) ]
    summary_mcq = []
    summary_as = []
    sid_list = [x for x in userdata.studentid]
    uname_list = [x for x in userdata.username]
    udict = dict(zip(sid_list,uname_list))

    avgsum_list = [ userdata[ "mcq_avg" + str(x) ].mean() for x in range( 1, 14 ) ]    
    rr = [ 1 if r>0 else 0 for r in avgsum_list ]
    rsum = sum(rr)
    idx = 0
    if rsum>0:
        for n in range(13) :
            if rr[n]>0:
                result_table[idx][0] = '# ' + str(n+1)
                result_table[idx][1] = "{:.2%}".format(avgsum_list[n])
                idx+=1        
        df = userdata[[ "mcq_avg" + str(x) for x in range( 1, rsum + 1 ) ]]
        df["mcq_avg"] = df.apply(lambda x: x.mean(), axis=1)
        avg_list = [x for x in df.mcq_avg]
        rdict = dict(zip(sid_list,avg_list))
        df = userdata[[ "mcq_attempts" + str(x) for x in range( 1, rsum + 1 ) ]]
        df["mcq_attempts"] = df.apply(lambda x: x.mean(), axis=1)
        avg_list = [x for x in df.mcq_attempts]
        xdict = dict(zip(sid_list,avg_list))
        summary_mcq = report_lines(udict, rdict, summary_mcq, "70% and above", above_70)
        summary_mcq = report_lines(udict, rdict, summary_mcq, "Between 30% and 70%", between_3070)
        summary_mcq = report_lines(udict, rdict, summary_mcq, "Below 30%", below_30)
        summary_mcq = report_lines(udict, xdict, summary_mcq, "2 or more attempts", above_2, '{:.2}')

    avgsum_list = [ userdata[ "as_avg" + str(x) ].mean() for x in range( 1, 14 ) ]
    rr = [ 1 if r>0 else 0 for r in avgsum_list ]
    rsum = sum(rr)    
    idx = 0
    if rsum>0:        
        for n in range(13) :
            if rr[n]>0:
                result_table[idx][2] = '# ' + str(n+1)
                result_table[idx][3] = "{:.2%}".format(avgsum_list[n])
                idx+=1
        df = userdata[[ "as_avg" + str(x) for x in range( 1, rsum + 1 ) ]]
        df["as_avg"] = df.apply(lambda x: x.mean(), axis=1)
        avg_list = [x for x in df.as_avg]
        rdict = dict(zip(sid_list,avg_list))
        summary_as = report_lines(udict, rdict, summary_as, "70% and above", above_70)
        summary_as = report_lines(udict, rdict, summary_as, "Between 30% and 70%", between_3070)
        summary_as = report_lines(udict, rdict, summary_as, "Below 30%", below_30)        
        
    if result_table==[]:
        bot.sendMessage(chat_id, "There is no information available at the moment.")
        return ([],[],[])
    return (result_table,summary_mcq,summary_as)

def checkjoblist():
    global bot_intance
    client_name = bot_intance.client_name
    df = rds_df(f"select * from job_list where status='open' and client_name='{client_name}';")    
    if df is None:
        #print("no job found")
        return
    df.columns = get_columns("job_list")
    jobitem = df.iloc[0].to_dict()
    bot_intance.job_items = jobitem
    msg =  runbotjob()
    if msg == "":
        msg = "job complete complete"
    time_now = time.strftime('%H%M%S', time.localtime() )
    time_end = str(time_now)
    status = "completed"
    job_id = jobitem['job_id']
    updqry = f"update job_list set time_end = '{time_end}', status = '{status}', message = '{msg}' where job_id = '{job_id}';"
    rds_update(updqry)
    return

def runbotjob():
    global edx_api_header, edx_api_url,bot_intance,vmbot
    jobitem = bot_intance.job_items
    client_name = bot_intance.client_name    
    #printdict(jobitem)
    job_id = jobitem['job_id']
    chat_id = int(jobitem['chat_id'])
    bot_req = jobitem['bot_req']
    func_req = jobitem['func_req']
    func_param = jobitem['func_param']
    func_svc_list = ["update_assignment" , "update_mcq" , "edx_import", "update_schedule"]    
    txt = "job "
    updqry = f"update job_list set status = 'running', message = '' where job_id = '{job_id}';"
    rds_update(updqry)
    jobitem['status'] = 'running'
    vmsvclib.rds_connstr = ""
    vmsvclib.rdscon = None
    if func_req == "generate_mcq_as":
        try:
            vmedxlib.generate_mcq_as(func_param)
            txt += " completed successfully."
        except:
            txt += " failed."
    elif func_req in ["edx_mass_import", "mass_update_assignment", "mass_update_mcq", "mass_update_schedule", "mass_update_usermaster"]:
        #try:
        #    func_svc = "vmedxlib." + func_req + "(client_name)"
        #    status = eval(func_svc)
        if func_req == "mass_update_assignment":
            vmedxlib.mass_update_assignment(client_name)
        elif func_req == "mass_update_mcq":
            vmedxlib.mass_update_mcq(client_name)            
        elif func_req == "mass_update_schedule":
            vmedxlib.mass_update_schedule(client_name)
        elif func_req == "edx_mass_import":
            vmedxlib.edx_mass_import(client_name)
        txt += " completed successfully."
    elif func_req in func_svc_list :
        course_id = func_param           
        #try:     
        #    func_svc = "vmedxlib." + func_req + "(course_id, client_name)"
        #    status = eval(func_svc)       
        if func_req == "update_assignment":
            vmedxlib.update_assignment(course_id, client_name)
        elif func_req == "update_mcq":
            vmedxlib.update_mcq(course_id, client_name)
        elif func_req == "update_schedule":
            vmedxlib.update_schedule(course_id, client_name)
        elif func_req == "edx_import":
            vmedxlib.edx_import(course_id, client_name)
        txt += " completed successfully."
        #except:
        #    txt += " failed."
    elif func_req == 'load_edxdata':
        load_edxdata(client_name)
    else:
        try:
            func_svc = func_req + "(" + func_param + ")"
            status = eval(func_svc)
            if status:
                txt += " completed successfully."
            else:
                txt += " failed."
        except:
            txt += " failed."
    try:        
        bot_intance.bot.sendMessage(chat_id,f"completed job {job_id}")
    except:
        print(f"completed job {job_id}")
    jobitem['status'] = 'completed'
    bot_intance.job_items = {}
    return txt

#------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:        
        #do_main()
        vmsvclib.rds_connstr = ""
        vmsvclib.rdscon = None 
        client_name='Lithan'
        #resp_dict = load_respdict()
        print("this is vmbotlib")
    else:
        print("Unable to use this version of python\n", version)
