import sys
import os
import pyodbc
from Logger import Logger
from macpath import split
import traceback

class DBImporter(object):
    '''
    classdocs
    '''
    
    #db params
    dbHost=""
    dbPort=2638
    dbUser=""
    dbPassword=""
    dbDatabase=""
    
    #other params
    configDir="config"
    
    #local vars
    dbConnection = None
    
    def __init__(self, dbHost, dbPort, dbUser, dbPassword, dbDatabase):
        self.dbHost=dbHost
        self.dbPort=dbPort
        self.dbUser=dbUser
        self.dbPassword=dbPassword
        self.dbDatabase=dbDatabase
        
        if(not self.connectDB()):
            Logger.writeAndPrintLine("Could not connect specified database, Program exiting.", 4)  
            sys.exit()
        self.disconnectDB()  
            
    def connectDB(self):
        try:
            self.dbConnection = pyodbc.connect('UID='+self.dbUser+';PWD='+self.dbPassword+';DSN='+self.dbHost)
        except: 
            Logger.writeAndPrintLine("Could not connect specified database.", 3)    
            return False
        return True

    def disconnectDB(self):
        self.dbConnection.close()
        
    def insertSurveyFromCSV(self, csv):
        #CSV ANATOMY: dateSubmitted, FormType, CaseHash
        splitString=csv.split('"\n"')
        splitString[0]=splitString[0].lstrip('"')
        splitString[1]=splitString[1][0:-2] #some kinda EOF character in there. 
        answers=splitString[1].split('","')
        
        formType=answers[1]
        caseHash=answers[2]
        #print(formType)
        self.connectDB()
        if(self.isOriginalSurvey(formType, caseHash)):
            qConfig=self.getSurveyQuestions(formType)
            if(qConfig==None):
                self.disconnectDB()
                Logger.writeAndPrintLine("Failed to find config in WKM_surveys and WKM_survey questions for formType "+formType+".", 3)
                return False
            if(self.insertAnswers(answers, qConfig, caseHash)):
                self.disconnectDB()
                return True
            else:
                self.disconnectDB() 
                return False   
        else:
            Logger.writeAndPrintLine("Ignoring duplicate survey "+formType+": "+caseHash, 2)
            return True
        self.disconnectDB() 
        
    def isOriginalSurvey(self, formType, caseHash):
        sql="""
            select WKM_completed_surveys.id from WKM_completed_surveys 
            left join WKM_surveys on WKM_completed_surveys.survey_id=WKM_surveys.id
            where caseHash=? and formName=?
            """
        cursor=self.dbConnection.cursor()
        cursor.execute(sql,[caseHash, formType])
        rowset=cursor.fetchall()
        cursor.close()
        return rowset==[]

    def getSurveyQuestions(self, formType):
        sql="select id, qOrder, survey_id, question from WKM_survey_questions where survey_id=(select top 1 id from WKM_surveys where formName=?) order by qOrder asc"
        cursor=self.dbConnection.cursor()
        cursor.execute(sql,[formType])
        rowset=cursor.fetchall()
        cursor.close()
        return rowset
        
    def insertAnswers(self, answers, qConfig, caseHash):
        surveyID=qConfig[0][2]
        try:
            sql="begin INSERT INTO WKM_completed_surveys (survey_id, caseHash) VALUES (?,?) select @@identity end"
            cursor=self.dbConnection.cursor()
            cursor.execute(sql,[surveyID,caseHash])
            entryID=cursor.fetchone()[0]
            #print("ID: "+str(entryID))
            sql="INSERT INTO WKM_completed_survey_answers (c_survey_id,survey_q_id,answer) VALUES (?,?,?) commit"
            for q in qConfig:
                cursor=self.dbConnection.cursor()
                cursor.execute(sql,[entryID,q[0],answers[q[1]+2]])
            self.insertCaseNote(answers, qConfig, entryID)
            self.messageOnBad(entryID)
        except: 
            Logger.writeAndPrintLine("Failed run survey insert queries. "+traceback.format_exc(), 3)
            return False
        finally:
            cursor.close()
        return True
    
    def insertCaseNote(self, answers, qConfig, entryID):
        sql="select case_id, note_topic, question, qOrder "
        sql+="from WKM_completed_surveys "
        sql+="inner join WKM_surveys on WKM_completed_surveys.survey_id=WKM_surveys.id "
        sql+="inner join WKM_case_data on WKM_case_data.casenumHash=WKM_completed_surveys.caseHash "
        sql+="left join WKM_survey_questions on WKM_surveys.id=WKM_survey_questions.survey_id "
        sql+="where WKM_completed_surveys.id=? order by qOrder asc"
        
        cursor=self.dbConnection.cursor()
        cursor.execute(sql,[entryID])
        results=cursor.fetchall()
        cursor.close()
        message="Survey received from website: "+'\x0d\x0a'
        for row in results:
            message+=(row[2]+": "+'\x0d\x0a'+answers[row[3]+2]+'\x0d\x0a\x0d\x0a')
            
        sql="exec WKM_InsertCaseNote ?,?,?,? commit"
        cursor=self.dbConnection.cursor()
        cursor.execute(sql, [results[0][1],message,"SYSTEM",str(results[0][0])])
        cursor.close()
        Logger.writeAndPrintLine("Needles note added to case "+str(results[0])+". ",1)  
        
        
    def messageOnBad(self, entryID):
        sql="""
            select qOrder, question, answer, query, explanation, alert_staff from WKM_survey_bad_answers
            left join WKM_survey_questions on WKM_survey_bad_answers.q_id=WKM_survey_questions.id
            left join WKM_completed_surveys on WKM_survey_questions.survey_id=WKM_completed_surveys.survey_id
            left join WKM_completed_survey_answers on WKM_completed_surveys.id=WKM_completed_survey_answers.c_survey_id
            and WKM_survey_questions.id=WKM_completed_survey_answers.survey_q_id
            where WKM_completed_surveys.id=?
            order by qOrder asc
        """
        cursor=self.dbConnection.cursor()
        cursor.execute(sql,[entryID])
        results=cursor.fetchall()
        cursor.close()
        alertStaff=[]
        
        message="Survey feedback received. Please investigate, see case note for more details."+'\x0d\x0a'
        for row in results:
            checkQuery=row[3]
            
            #check query may accept the answer in multiple places. set up an input value array. 
            inArr=[]
            for x in range(checkQuery.count("##answer##")): inArr.append(row[2])
            
            checkQuery=checkQuery.replace("##answer##",'?')
            cursor=self.dbConnection.cursor()
            cursor.execute(checkQuery, inArr)
            ansBad=cursor.fetchall()
            if(ansBad[0][0]=='1'):
                message+=row[1]+" ("+row[5]+")\x0d\x0a"+row[2]+'\x0d\x0a'
                if(not (row[5] in alertStaff)):#who are we messaging?
                    alertStaff.append(row[5])
        
        #lazy way to check if any of the questions were in fact flagged as bad. 
        if(alertStaff==[]): return
        
        #assemble "to" field. 
        toBlock=''
        for staff in alertStaff:
            toBlock=toBlock+staff+'; '
        toBlock=toBlock[:-2]
        
        sql="select top 1 casenum, party_id from cases "
        sql+="left join party on cases.casenum=party.case_id "
        sql+="inner join WKM_case_data on cases.casenum=WKM_case_data.case_id "
        sql+="inner join WKM_completed_surveys on WKM_completed_surveys.caseHash=WKM_case_data.casenumHash "
        sql+="where WKM_completed_surveys.id=? and party.role='Plaintiff' and our_client='Y' "
        cursor=self.dbConnection.cursor()
        cursor.execute(sql,[entryID])
        caseInfo=cursor.fetchall()
        cursor.close()
        
        for staff in alertStaff:
            sql="exec WKM_InsertMessage ?,?,?,?,?,? commit"
            cursor=self.dbConnection.cursor()
            cursor.execute(sql,[staff,toBlock,message,caseInfo[0][0],caseInfo[0][1],""])
            cursor.close()
            Logger.writeAndPrintLine("Needles message sent regarding feedback for survey ID "+str(entryID)+" to "+staff,1)  
            
        
    def isSurveyRepeat(self, surveyID, caseHash):
        sql="select * from WKM_completed_surveys where survey_id=? and caseHash=?"
        cursor=self.dbConnection.cursor()
        cursor.execute(sql,[surveyID, caseHash])
        results=cursor.fetchall()
        cursor.close()
        return True