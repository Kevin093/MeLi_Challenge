from __future__ import print_function
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file as oauth_file, client, tools
import email
import base64
import pymysql
from datetime import datetime

# DB connection
dbIp = '127.0.0.1'
dbLoginName = 'MYSQL_USER'
dbLoginPassword = 'MYSQL_PASSWORD'
dbName = 'devopsmeli'

def connectToGmail():
    SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
    store = oauth_file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('gmail', 'v1', http=creds.authorize(Http()))
    return service


def getEmailMessagesFromGmail():
    # Call the Gmail API
    connection = connectToGmail()
    allEmailsInInbox = connection.users().messages().list(userId='me').execute()
    emails = []
    emails.extend(allEmailsInInbox['messages'])
    return emails


def getMimeMessageFromMailId(emailId):
    connection = connectToGmail()
    message = connection.users().messages().get(userId='me', id=emailId, format='raw').execute()
    mime_message = email.message_from_bytes(base64.urlsafe_b64decode(message['raw'].encode('UTF-8')))
    return mime_message


def devOpsIsInSubject(emailSubject):
    return "devops" in emailSubject.lower()


def devOpsIsInBody(emailBody):
    return next((True for line in emailBody if "devops" in (str(line.get_payload())).lower()), False)


def convertDateTimeForMySQL(emailDate):
    dateFromEmail = datetime.strptime(emailDate, '%a, %d %b %Y %H:%M:%S %z')
    dateTimeForMYSQL = datetime.strftime(dateFromEmail, '%Y-%m-%d %H:%M:%S')
    return dateTimeForMYSQL


def insertDataIntoMySQLDb(emailDate,emailFrom,emailSubject,emailId):
    dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword, db=dbName)
    dbConnectionCursor = dbConnection.cursor()
    dateForMySQL = convertDateTimeForMySQL(emailDate)
    insertQuery = 'INSERT INTO devopsmails (emailId,emailDate,emailFrom,emailSubject) values (%s,%s,%s,%s)'
    dbConnectionCursor.execute(insertQuery,(emailId,dateForMySQL,emailFrom,emailSubject))
    dbConnection.commit()
    dbConnection.close()


def createDataBaseForDevOps():
    dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword)
    dbConnectionCursor = dbConnection.cursor()
    createDBQuery = "CREATE DATABASE " + dbName
    createTableQuery = '''CREATE TABLE `''' + dbName + '''`.`devopsmails` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `emailId` VARCHAR(45) NOT NULL,
  `emailDate` DATETIME NOT NULL,
  `emailFrom` VARCHAR(255) NOT NULL,
  `emailSubject` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC),
  UNIQUE INDEX `emailId_UNIQUE` (`emailId` ASC));
  '''
    if not existDatabase(dbName):
        dbConnectionCursor.execute(createDBQuery)
        dbConnectionCursor.execute(createTableQuery)
        dbConnection.commit()
        dbConnection.close()


def existDatabase(databaseName):
    dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword)
    dbConnectionCursor = dbConnection.cursor()
    selectDatabasesQuery = 'show databases;'
    dbConnectionCursor.execute(selectDatabasesQuery)
    databases = []
    for database in dbConnectionCursor:
        databases.append(database[0])
    dbConnection.close()
    return databaseName in databases


def findDevOpsInMailsAndStoreInDB():
    createDataBaseForDevOps()
    emails = getEmailMessagesFromGmail()
    for emailMessage in emails:
        emailId = emailMessage['id']
        mime_message = getMimeMessageFromMailId(emailId)
        emailSubject = mime_message['subject']
        emailBody = mime_message.get_payload()
        emailFrom = mime_message['from']
        emailTo = mime_message['to']
        emailDate = mime_message['date']
        if devOpsIsInBody(emailBody) or devOpsIsInSubject(emailSubject):
            insertDataIntoMySQLDb(emailDate,emailFrom,emailSubject,emailId)


findDevOpsInMailsAndStoreInDB()