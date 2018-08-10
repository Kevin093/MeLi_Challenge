from __future__ import print_function
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import Http
from oauth2client import file as oauth_file, client, tools, clientsecrets
import email
import base64
import pymysql
from datetime import datetime

# DB connection
dbIp = '127.0.0.1'
dbLoginName = 'MYSQL_USER'
dbLoginPassword = 'MYSQL_PASSWORD'
dbName = 'devopsmeli'

# Log File
logFile = '.\MeLi_Ch2_Gmail.log'
log = open(logFile, "a")


def writeDataInLog(data):
    log.write(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + data)


def connectToGmail():
    try:
        SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
        store = oauth_file.Storage('token.json')
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
            creds = tools.run_flow(flow, store)
        service = build('gmail', 'v1', http=creds.authorize(Http()))
        return service
    except (clientsecrets.InvalidClientSecretsError) as e:
        writeDataInLog(" GMAIL Connection Error. Error: " + str(e) + " \n")


def getEmailMessagesFromGmail():
    # Call the Gmail API
    emails = []
    try:
        connection = connectToGmail()
        allEmailsInInbox = connection.users().messages().list(userId='me').execute()
        emails.extend(allEmailsInInbox['messages'])
        return emails
    except (KeyError) as e:
        writeDataInLog(" Error getting Emails from Inbox. Inbox is empty. Error: " + str(e) + " \n")
        return emails
    except (HttpError,AttributeError) as e:
        writeDataInLog(" Error getting Emails. Connection Error. Error: " + str(e) + " \n")
        return emails

def getMimeMessageFromMailId(emailId):
    connection = connectToGmail()
    try:
        message = connection.users().messages().get(userId='me', id=emailId, format='raw').execute()
        mime_message = email.message_from_bytes(base64.urlsafe_b64decode(message['raw'].encode('UTF-8')))
        return mime_message
    except (Exception) as e:
        writeDataInLog(" Error getting Message from Email ID. Error: " + str(e) + " \n")


def devOpsIsInSubject(emailSubject):
    return "devops" in emailSubject.lower()


def devOpsIsInBody(emailBody):
    try:
        for line in emailBody:
            # Print line.get_payload() to verify problem with .lower()
            print(line.get_payload())
            return "devops" in str(line.get_payload()).lower()
    except(AttributeError) as e:
        writeDataInLog(" Error checking devOps in Body. Error in line. Error: " + str(e) + "\n")


def convertDateTimeForMySQL(emailDate):
    dateFromEmail = datetime.strptime(emailDate, '%a, %d %b %Y %H:%M:%S %z')
    dateTimeForMYSQL = datetime.strftime(dateFromEmail, '%Y-%m-%d %H:%M:%S')
    return dateTimeForMYSQL


def insertDataIntoMySQLDb(emailDate,emailFrom,emailSubject,emailId):
    try:
        dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword, db=dbName)
        dbConnectionCursor = dbConnection.cursor()
        dateForMySQL = convertDateTimeForMySQL(emailDate)
        insertQuery = 'INSERT INTO devopsmails (emailId,emailDate,emailFrom,emailSubject) values (%s,%s,%s,%s)'
        dbConnectionCursor.execute(insertQuery,(emailId,dateForMySQL,emailFrom,emailSubject))
        dbConnection.commit()
        dbConnection.close()
        writeDataInLog(" Added correctly in DB." + " \n")
    except pymysql.err.IntegrityError as e:
        writeDataInLog(" " + emailId + ", " + emailSubject +", from:" + emailFrom + " already exist in DB. Error: " + str(e) + " \n")
    except (pymysql.err.OperationalError) as e:
        log.write(" DB Connection error to server: "+ dbIp + " Error: " + str(e) + "\n")


def createDataBaseForDevOps():
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
    try:
        if not existDatabase(dbName):
            dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword)
            dbConnectionCursor = dbConnection.cursor()
            dbConnectionCursor.execute(createDBQuery)
            dbConnectionCursor.execute(createTableQuery)
            dbConnection.commit()
            dbConnection.close()
            writeDataInLog(" DB " + dbName + " created successfully" + " \n")
        else:
            writeDataInLog(" DB " + dbName + " already exist" + " \n")
    except pymysql.err.IntegrityError as e:
        writeDataInLog(" Can't create DB: " + dbName + "Error: " + str(e) + "\n")
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        writeDataInLog(" Connection error to server: " + dbIp + ", can't create DB. Error: " + str(e) + " \n")


def existDatabase(databaseName):
    try:
        dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword)
        dbConnectionCursor = dbConnection.cursor()
        selectDatabasesQuery = 'show databases;'
        dbConnectionCursor.execute(selectDatabasesQuery)
        databases = []
        for database in dbConnectionCursor:
            databases.append(database[0])
        dbConnection.close()
        return databaseName in databases
    except pymysql.err.IntegrityError as e:
        writeDataInLog(" Can't show databases. Error: " + str(e) + " \n")
        return False
    except (pymysql.err.OperationalError) as e:
        writeDataInLog(" Connection error to server when try to check if DB exists. Error: " + str(e) + " \n")
        return False


def findDevOpsInMailsAndStoreInDB():
    createDataBaseForDevOps()
    emails = getEmailMessagesFromGmail()
    for emailMessage in emails:
        try:
            emailId = emailMessage['id']
            mime_message = getMimeMessageFromMailId(emailId)
            emailSubject = mime_message['subject']
            emailBody = mime_message.get_payload()
            emailFrom = mime_message['from']
            emailTo = mime_message['to']
            emailDate = mime_message['date']
            if devOpsIsInBody(emailBody) or devOpsIsInSubject(emailSubject):
                insertDataIntoMySQLDb(emailDate,emailFrom,emailSubject,emailId)
        except (TypeError) as e:
            writeDataInLog(" Error getting email Attributes: " + str(e) + " \n")
    log.close()


findDevOpsInMailsAndStoreInDB()