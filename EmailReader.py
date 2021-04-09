import poplib
import quopri
import email
import os
import re
import time
import sys
from Logger import Logger
import datetime
import traceback
from DBImporter import DBImporter

class EmailReader(object):
    #Implements a pop3 client to read voicemail emails and parse them to XML jobs.

    #email params
    emailAddr = None
    emailPassword = None
    emailServer = None
    emailServerPort = 995
    emailSSL = False

    #other params
    idle = 1
    
    #local vars
    popServer = None
    running = True

    errorCount=0
    lastErrorTime=datetime.datetime.now()
    lastErrorMessage = None
    errorAcknowledged=True
    dbImporter=None
    
    
    def __init__(self, addr, password, server, port, ssl, idle, dbImporter):
        
        self.emailAddr=addr
        self.emailPassword=password
        self.emailServer=server
        self.emailServerPort=port
        self.emailSSL=ssl
        self.idle=idle
        self.dbImporter=dbImporter
        
        if(not self.connectPOP()):
            Logger.writeAndPrintLine("Could not connect POP3 account. Program exiting.", 4)
            sys.exit()
            self.disconnectPOP()

    def connectPOP(self):
        try:
            if(self.emailSSL):
                self.popServer=poplib.POP3_SSL(self.emailServer)
            else:
                self.popServer=poplib.POP3(self.emailServer)
            self.popServer.port=self.emailServerPort
            self.popServer.user(self.emailAddr)
            self.popServer.pass_(self.emailPassword)
        except:
            return False
        return True
    
    def disconnectPOP(self):
        try:
            self.popServer.quit()#We're done with emails. Closes the connection and triggers message deletion.
        except:
            Logger.writeAndPrintLine("Error quitting POP3 connection.",3)

    def run(self):
        try:
            while self.running:
                try:
                    self.connectPOP()
                    numMessages = len(self.popServer.list()[1])
                except:
                    Logger.writeAndPrintLine("Error connecting to POP3 email account.", 3)
                    continue
                #print("Number of emails: "+str(numMessages))
                for msgNum in range(numMessages):
                    try:
                        success=self.importMessage(msgNum+1)
                    except Exception as e:
                        Logger.writeAndPrintLine("Failed to import message, unknown error "+traceback.format_exc(),3)  
                        continue
                    if(success=="MALFORMED"):
                        try:
                            Logger.writeAndPrintLine("Deleting malformed email "+str(msgNum),2)  
                            self.popServer.dele(msgNum+1)
                        except:
                            pass
                    if(success=="SUCCESS"):
                        Logger.writeAndPrintLine("Finished importing message "+str(msgNum)+", deleting.",1)  
                        self.popServer.dele(msgNum+1)
                    else:#ERROR - do nothing, try again next round. 
                        None
                self.disconnectPOP()
                time.sleep(self.idle)

        except Exception as e: 
            Logger.writeAndPrintLine("An unexpected error occurred in EmailImporter, halting: "+traceback.format_exc(),3)  
        Logger.writeAndPrintLine("EmailImporter exiting gracefully???",3) 
        
    def importMessage(self, messageNum):
        raw_message=self.popServer.retr(messageNum)[1]
        str_message = email.message_from_bytes(b'\n'.join(raw_message))
        body = str(str_message.get_payload()[0])# GETS BODY
        messageUID=str(self.popServer.uidl(messageNum))
        messageUID=re.findall('UID\d+-\d+',messageUID,0)[0]
        
        csv=self.getCSV(str_message)
        if(csv==None):
            Logger.writeAndPrintLine("Email has no attached CSV ",2)  
            return "MALFORMED"
        csv=csv.decode("ascii", "replace")
        print(csv)
        result=self.dbImporter.insertSurveyFromCSV(csv)
        if(result==True):
            Logger.writeAndPrintLine("Survey imported successfully.",1) 
            return "SUCCESS"
        else:
            Logger.writeAndPrintLine("Error importing survey.",3)
            return "ERROR"
        
    def getCSV(self, str_message):
        for part in str_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                #print("no content dispo")
                continue
            contents=part.get_payload(decode=1)
            return contents

  