import csv
import string
from random import *
from ldap3 import Connection, MODIFY_REPLACE, HASHED_SALTED_SHA
from ldap3.utils.hashed import hashed
import pymysql
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Server OpenLDAP connection
openLDAPip = 'IP_ADR'
openLDAPServerLoginName = "cn=ldapadm,dc=MeLi_Ch_1,dc=local"
openLDAPServerLoginPassword = "OpenLDAP_Password"
## OU and DC where users will be store
openLDAPServerOU = 'ou=ChallengeUsers'
openLDAPServerDC = 'dc=MeLi_Ch_1,dc=local'

# DB connection
dbIp = '127.0.0.1'
dbLoginName = 'MYSQL_USER'
dbLoginPassword = 'MYSQL_PASSWORD'
dbName = 'meli_abm'

# SMTP connection
smtpServerIp = 'smtp.mail.yahoo.com'
smtpPort = 587
smtpEmail = "EMAIL_ACCOUNT"
smtpUser = smtpEmail
smtpPassword = "PASSWORD"

# CSV file
csvFileRoute = 'C:/users/kevin/MeLi_Challenge_1/Users.csv'

def readCSVFile(CSVFileName):
    usersList = []
    with open(CSVFileName, newline='') as csvfile:
        usersInFile = csv.reader(csvfile, delimiter=',')
        for userInFile in usersInFile:
            userDic = {}
            name = userInFile[0]
            lastName = userInFile[1]
            email = userInFile[2]
            userDic = {"name": name, "lastName": lastName, "email": email}
            usersList.append(userDic)
    return usersList


def hashPassword(randomPassword):
    hashedPassword = hashed(HASHED_SALTED_SHA, randomPassword)
    return hashedPassword;


def createRandomPassword():
    min_char = 12
    max_char = 12
    allchar = string.ascii_letters + string.digits + string.digits
    randomPassword = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
    return randomPassword;


def existUserInOpenLDAP(user):
    serverConnection = Connection(openLDAPip, user=openLDAPServerLoginName, password=openLDAPServerLoginPassword, version=3)
    serverConnection.bind()
    userEmail = user["email"]
    existUser = serverConnection.search(openLDAPServerOU+','+openLDAPServerDC, '(mail='+userEmail+')', attributes=['cn', 'uidNumber'])
    return existUser;


def createUserInOpenLDAP(user,hashedPassword):
    #Server connection
    serverConnection = Connection(openLDAPip, user=openLDAPServerLoginName, password=openLDAPServerLoginPassword, version=3)
    serverConnection.bind()
    #Add user to LDAP Server and set password
    userName = user["name"]
    userLastName = user["lastName"]
    userEmail = user["email"]
    #Parameters
    addUserDN = 'uid='+userEmail+','+openLDAPServerOU+','+openLDAPServerDC
    addUserClasses = {'top','inetOrgPerson','posixAccount','shadowAccount'}
    addUserAttributes = {'gidNumber':'0','cn': userName,'SN': userLastName, 'givenName': userName,'displayName': userName+ ' ' + userLastName,
                'homeDirectory': '/','uidNumber': '0','mail':userEmail,'shadowLastChange':'0','userPassword': '{crypt}x'}
    setPasswordDN = 'uid='+userName+',ou=ChallengeUsers,dc=MeLi_Ch_1,dc=local'
    setPasswordPass = {'userPassword': [(MODIFY_REPLACE, hashedPassword)]}

    if existUserInOpenLDAP(user):
        return "Already created"
    else:
        userCreatedOk = serverConnection.add(addUserDN, addUserClasses, addUserAttributes)
        serverConnection.modify(setPasswordDN, setPasswordPass)
        if userCreatedOk:
            return "Created Successfully"
        else:
            return "Not created due to error"


def createDataBaseForUsersABM():
    dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword)
    dbConnectionCursor = dbConnection.cursor()
    createDBQuery = "CREATE DATABASE " + dbName
    createTableUsersQuery = '''CREATE TABLE `''' + dbName + '''`.`users` (
  `user_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(45) NOT NULL,
  `user_lastname` varchar(45) NOT NULL,
  `user_email` varchar(60) NOT NULL,
  `user_password` varchar(50) NOT NULL,
  `user_status` varchar(45) NOT NULL,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `user_id_UNIQUE` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
  '''
    createTableLogQuery = '''CREATE TABLE `''' + dbName + '''`.`log` (
  `log_id` int(11) NOT NULL AUTO_INCREMENT,
  `log_user` int(11) NOT NULL,
  `log_message` varchar(45) NOT NULL,
  `log_date` datetime DEFAULT NULL,
  PRIMARY KEY (`log_id`),
  UNIQUE KEY `log_id_UNIQUE` (`log_id`),
  KEY `user_id_idx` (`log_user`),
  CONSTRAINT `user_id_log_user` FOREIGN KEY (`log_user`) REFERENCES `users` (`user_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;
    '''
    if not existDatabase(dbName):
        dbConnectionCursor.execute(createDBQuery)
        dbConnectionCursor.execute(createTableUsersQuery)
        dbConnectionCursor.execute(createTableLogQuery)
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


def storeStatusAndUserInDB(user,hashedPassword,creationStatus):
    userName = user["name"]
    userLastName = user["lastName"]
    userEmail = user["email"]

    createDataBaseForUsersABM()
    dbConnection = pymysql.connect(host=dbIp, user=dbLoginName, passwd=dbLoginPassword, db=dbName)
    dbConnectionCursor = dbConnection.cursor()

    findUserQuery = "select user_id from users where user_email = %s"
    insertLogQuery = "insert into log (log_user,log_message,log_date) values (%s,%s,%s)"
    insertUserQuery = "insert into users (user_name,user_lastname,user_email,user_password,user_status) values (%s,%s,%s,%s,%s)"

    if creationStatus == "Already created":
        dbConnectionCursor.execute(findUserQuery,userEmail)
        for row in dbConnectionCursor: userId = row[0]
        dbConnectionCursor.execute(insertLogQuery,(userId,creationStatus,time.strftime('%Y-%m-%d %H:%M:%S')))
        dbConnection.commit()
    elif creationStatus == "Created Successfully":
        dbConnectionCursor.execute(insertUserQuery,(userName,userLastName,userEmail,hashedPassword,'CREATED'))
        dbConnection.commit()
        dbConnectionCursor.execute(findUserQuery,userEmail)
        for row in dbConnectionCursor: userId = row[0]
        dbConnectionCursor.execute(insertLogQuery, (userId,creationStatus, time.strftime('%Y-%m-%d %H:%M:%S')))
        dbConnection.commit()
    else:
        dbConnectionCursor.execute(insertUserQuery, (userName, userLastName, userEmail, hashedPassword,'NOT CREATED'))
        dbConnection.commit()
        dbConnectionCursor.execute(findUserQuery,userEmail)
        for row in dbConnectionCursor: userId = row[0]
        dbConnectionCursor.execute(insertLogQuery, (userId, creationStatus, time.strftime('%Y-%m-%d %H:%M:%S')))
        dbConnection.commit()
    dbConnection.close()


def sendConfirmationEmail(user,userRandomPassword,creationStatus):
    userName = user["name"]
    userLastName = user["lastName"]
    userEmail = user["email"]
    body = userName + ' ' + userLastName + ', el usuario '+ userEmail +' fue creado con exito. Su contraseña es: '+ userRandomPassword
    subject = 'Nuevo usuario en MeLi'
    fromAddr = smtpEmail
    toAddr = userEmail
    msg = MIMEMultipart()
    msg['From'] = fromAddr
    msg['To'] = toAddr
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    textMsg = msg.as_string()
    if creationStatus == "Created Successfully":
        smtpServer = smtplib.SMTP(smtpServerIp, smtpPort)
        smtpServer.ehlo()
        smtpServer.starttls()
        smtpServer.ehlo()
        smtpServer.login(smtpEmail, smtpPassword)
        smtpServer.sendmail(fromAddr, toAddr, textMsg)


def createUsersInApp():
    # Read CSV File
    usersList = readCSVFile(csvFileRoute)
    for user in usersList:
        # Create random password
        userRandomPassword = createRandomPassword()
        # Hash password
        hashedPassword = hashPassword(userRandomPassword)
        # Create user in OpenLDAP
        creationStatus = createUserInOpenLDAP(user,hashedPassword)
        # Store user in DB
        storeStatusAndUserInDB(user,hashedPassword,creationStatus)
        # Send email confirmation
        sendConfirmationEmail(user,userRandomPassword,creationStatus)


# Create Users from User List
createUsersInApp()