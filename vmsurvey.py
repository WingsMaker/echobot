import sys
import vmbotlib
import pyodbc

import pyodbc 
global azcon, azcursor

def html_msg(bot, chat_id, title = "", body=""):
    if (title == "") and (body==""):
        return
    if title == "":
        result = "<pre>" + body +  "</pre>"
    elif body == "":
        result = "<b>" + title + "</b>"
    else:
        result = "<b>" + title + "</b>\n<pre>" + body +  "</pre>"
    bot.sendMessage(chat_id,result,parse_mode='HTML')
    return

def connect_azuredb():
    # https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver15
    # https://dataedo.com/kb/query/sql-server/list-table-columns-in-database
    # pip install pyodbc
    global azcon, azcursor
    driver = "SQL Server"
    host = "lithanbidb.database.windows.net"
    user = "LithanBI"
    passwd = "Lithan4u$"
    port = 1433 
    db = "LithanSurvey"
    pss_connstr = f"DRIVER={driver};SERVER=tcp:{host};DATABASE={db};UID={user};PWD={passwd}"
    azcon = pyodbc.connect(pss_connstr)
    if azcon is None:
        print("unable to connect tb_pss")
        return None
    print("Connect Azure SQL server")
    azcursor = azcon.cursor()
    return

def surveyresults(client_name, course_id):
    global azcon, azcursor
    if azcon is None:
        connect_azuredb()
    if azcon is None:
        return ("", "")
    cohort_id = piece(piece(course_id,':',1),'+',1)
    module_code = piece(cohort_id,'-',0)
    module_number = piece(cohort_id,'-',1)
    qry = f"select pillar, course_code from course_module where client_name = '{client_name}' and module_code = '{module_code}';"
    df = rds_df(qry)
    df.columns = ['pillar', 'course_code']
    pillar_code = df.pillar.values[0]
    course_code = df.course_code.values[0]

    fld_list = ['cohort', 'learner_email', 'content', 'duration', 'faculty', 'support', 'understanding']
    field_list = ','.join(fld_list)
    query = f"select {field_list} from tb_pss where cohort = '{module_number}' and pillar='{pillar_code}' "
    query += f" and qualification = '{course_code}';"
    azcursor.execute(query)
    row = azcursor.fetchone() 
    if row is None:
        azcon.close()
        azcon = None    
        print("there is nothing found for this course : "+course_id)
        return ("", "")
    title = "cohort : " + str(row[0])
    print('='*50)
    print( title )
    print('-'*50)
    body = '\t'.join(['email\t\t\t', 'content\t', 'duration','duration', 'support', 'understanding'])
    print( body )
    duration_comments = []
    faculty_comments = []
    while row:
        email = (row[1] + ' '*15)[:25]
        txt = '\t'.join([email, row[2], row[3], row[4], row[5], row[6]])
        print( txt )
        body += txt
        row = azcursor.fetchone()    
    print('-'*50)
    azcon.close()
    azcon = None    
    return (title, body)


def test_surveydatapoint():
    # https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver15
    # https://dataedo.com/kb/query/sql-server/list-table-columns-in-database
    # https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver15
    # https://datalere.com/tips-guides/ec2-aws-linux-ami-pyodbc/    
    driver = "SQL Server"  # windows 10
    driver = "ODBC Driver 17 for SQL Server" # AWS EC2 Ubuntu 18.04
    host = "lithanbidb.database.windows.net"
    user = "LithanBI"
    passwd = "Lithan4u$"
    port = 1433 
    db = "LithanSurvey"
    pss_connstr = f"DRIVER={driver};SERVER=tcp:{host};DATABASE={db};UID={user};PWD={passwd}"
    psscon = pyodbc.connect(pss_connstr)
    if psscon is None:
        print("unable to connect tb_pss")
    print("Connect Azure SQL server")
    psscur = psscon.cursor()
    qry_version = "SELECT @@version;"
    psscur.execute(qry_version)
    row = psscur.fetchone()     
    print(''.join(list(row)))
    
    print("Table tb_pss consisting of :")
    qry_desc = """SELECT column_name FROM (
    SELECT col.name as column_name,
        col.column_id,
         schema_name(tab.schema_id) as schema_name,
        tab.name as table_name        
    from sys.tables as tab
        inner join sys.columns as col
        on tab.object_id = col.object_id
        left join sys.types as t
        on col.user_type_id = t.user_type_id
    WHERE schema_name(tab.schema_id) = 'dbo' and 
        tab.name = 'tb_pss' 	
    )  pss
    """
    psscur.execute(qry_desc)
    row = psscur.fetchone()
    flist = []
    k = 20
    while row:
        w = (row[0] + ' '*k)[:k]
        flist.append(w)
        row = psscur.fetchone()
    cnt = len(flist)    
    m = int((cnt+3)/4)
    field_list = '\n'.join(['\t'.join([ flist[i+n*4] if i+n*4<cnt else '' for i in range(4) ]) for n in range(m)])
    print(field_list)
    
    fld_list = ['cohort', 'mode_of_session', 'learner_name', 'learner_email', 'content', 'content_comments', \
    'duration', 'duration_comments', 'faculty', 'faculty_comments', 'support', 'support_comments', 'understanding', 'understanding_comments', \
    'content_response', 'duration_response', 'faculty_response', 'support_response', 'understanding_response']
    field_list = ','.join(fld_list)
    query = f"select {field_list} from tb_pss where cohort = '0620A' and pillar='SM' and qualification = 'EIT'; "
    psscur.execute(query)
    row = psscur.fetchone() 
    cnt=len(list(row))
    print('='*50)
    print( '\n'.join( [ fld_list[n] + ' : ' + str(row[n]) for n in range(2) ] ) )
    print('-'*50)
    print( '\t'.join(['email\t\t\t', 'content\t', 'duration','duration', 'support', 'understanding']))
    duration_comments = []
    faculty_comments = []
    while row:
        name = row[2]        
        email = (row[3] + ' '*15)[:25]
        content = row[4]
        duration = row[6]
        faculty = row[8]
        support = row[10]
        understanding = row[12]
        comments1 = str(row[7])
        print( '\t'.join([email, content, duration,duration, support, understanding]))
        if comments1.lower() not in ['none' , 'nil', 'na']:
            duration_comments.append(name + '\t' + email)
            duration_comments.append(comments1)
            duration_comments.append('\n')
        #print( '\n'.join( [ fld_list[n] + ' : ' + str(row[n]) for n in range(cnt) if n>1] ) )
        row = psscur.fetchone()    
    print('-'*50)
    print("=== duration_comments ===")
    print('\n'.join(duration_comments))
    print('-'*50)
    return

if __name__ == "__main__":
    version = sys.version_info    
    if version.major == 3 and version.minor >= 7:
        test_surveydatapoint()
        #opt_surveylist = "Survey Data"
        #analysis_menu = [[ml_grading, an_mcq, an_mcqd, an_chart, option_back]]
        #analysis_menu = [[ml_grading, an_mcq, an_mcqd], [an_chart, opt_surveylist, option_back]]        
        #
        #(title, msg ) = surveyresults(vmbot.client_name, self.courseid)
        #html_msg(self.bot, self.chatid, title, msg)   
        #        
        #elif self.menu_id == keys_dict[option_analysis]:
            #elif resp == opt_surveylist:
                #(title, msg ) = surveyresults(vmbot.client_name, self.courseid)
                #html_msg(self.bot, self.chatid, title, msg)
        
    else:
        print("Unable to use this version of python\n", version)
