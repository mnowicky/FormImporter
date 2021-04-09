import configparser
from EmailReader import EmailReader
from DBImporter import DBImporter
import os
from threading import Thread
from Logger import Logger
import sys
import datetime
import time
import traceback

class Loader(object):
    
    #config obj
    config = None
    configFileName="FormImporter.config"

    #etc params
    runOnce = False
    idle = 5

    #email params
    emailAddr = None
    emailPassword = None
    emailServer = None
    emailServerPort = 995
    emailSSL = False
    emailDelete = False

    #database params
    dbHost = None
    dbPort = 2638
    dbUser = "dba"
    dbPassword = "sql"
    dbDatabase = None

    #local vars
    reader = None
    readerThread = None
    importer = None
    importerThread = None

    def __init__(self):
        self.config = configparser.ConfigParser()

    def loadConfig(self):
        self.config.read(self.configFileName)

        #Etc configuration
        self.runOnce = self.config['DEFAULT']['runOnce']
        self.idle = int(self.config['DEFAULT']['idle'])

        #Email configuration
        self.emailAddr = self.config['EMAIL']['emailAddr']
        self.emailPassword = self.config['EMAIL']['password']
        self.emailServer = self.config['EMAIL']['server']
        self.emailPort = self.config['EMAIL']['port']
        self.emailSSL = self.config['EMAIL']['SSL']

        #Database configuration
        self.dbHost = self.config['DATABASE']['host']
        self.dbPort = self.config['DATABASE']['port']
        self.dbUser = self.config['DATABASE']['user']
        self.dbPassword = self.config['DATABASE']['password']
        self.dbDatabase = self.config['DATABASE']['database']

        #parse strings
        if(self.runOnce.upper()=="TRUE"):
            self.runOnce=True;
        else:
            self.runOnce=False;
        if(self.emailSSL.upper()=="TRUE"):
            self.emailSSL=True;
        else:
            self.emailSSL=False;

    def printConfig(self):
        print("DEFAULT: ")
        print("runOnce: "+str(self.runOnce))
        print("idle: "+str(self.idle))
        print("")
        print("EMAIL: ")
        print("emailAddr: "+self.emailAddr)
        print("emailPassword: "+self.emailPassword)
        print("emailServer: "+self.emailServer)
        print("emailPort: "+self.emailPort)
        print("emailSSL: "+str(self.emailSSL))
        print("emailDelete: "+str(self.emailDelete))
        print("")
        print("DATABASE: ")
        print("dbHost: "+self.dbHost)
        print("dbPort: "+self.dbPort)
        print("dbUser: "+self.dbUser)
        print("dbPassword: "+self.dbPassword)
        print("dbDatabase: "+self.dbDatabase)
        print("")

    def run(self):
        Logger.writeAndPrintLine("Program started.", 0)
        self.loadConfig()
        print("Launching FormImporter with the following parameters! :")
        self.printConfig()
        
        self.importer=DBImporter(self.dbHost, self.dbPort, self.dbUser, self.dbPassword, self.dbDatabase)
        self.reader=EmailReader(self.emailAddr, self.emailPassword, self.emailServer, self.emailPort, self.emailSSL, self.idle, self.importer)
        self.readerThread=Thread(target = self.reader.run)
        self.readerThread.start()
        Logger.writeAndPrintLine("Program launched successfully.", 1)
        

