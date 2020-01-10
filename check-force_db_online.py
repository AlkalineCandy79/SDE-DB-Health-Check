#-------------------------------------------------------------------------------
# Name:        Check/Force Databases Online
# Purpose:     Checks the status of a database and forces it back online.
#
# Author:      John Spence, Spatial Data Administrator
#
# Created:      10 October 2019
# Modified:
# Modification Purpose:
#
#
#-------------------------------------------------------------------------------


# 888888888888888888888888888888888888888888888888888888888888888888888888888888
# ------------------------------- Configuration --------------------------------
# The initial configuration for the script comes from a single AD SDE connection
# that is set below for the variable db_connection.
# All other configurations are set via the publishing table contained in the
# Carta database.  See notes for dependencies below.
#
# ------------------------------- Dependencies ---------------------------------
# 1) admingts.SDE_Connections = Where all the data links are.
#
# 888888888888888888888888888888888888888888888888888888888888888888888888888888

# Configure only hard coded db connection here.
db_connection = r'Database Connections\\Connection to YourDB.sde'

# Configure the e-mail server and other info here.
mail_server = 'smtprelay.yours.gov'
mail_from = 'SDE Database Status <yours@yours.gov>'

# ------------------------------------------------------------------------------
# DO NOT UPDATE BELOW THIS LINE OR RISK DOOM AND DISPAIR!  Have a nice day!
# ------------------------------------------------------------------------------

# Import Python libraries
import arcpy, datetime, smtplib, os
from datetime import timedelta

#-------------------------------------------------------------------------------
#
#
#                                 Functions
#
#
#-------------------------------------------------------------------------------

def check_db():
    global source_db
    global source_db_type
    global db_owner
    global conn_string

    print ('Pulling connections')

    try:
        # Check DB status
        print ('Connecting to SQL.')
        pull_relevant_dbs_SQL = ('''select * from admingts.SDE_Connections where Data_Owner = 'DBO' ''')
        pull_relevant_dbs_return = arcpy.ArcSDESQLExecute(db_connection).execute(pull_relevant_dbs_SQL)
        for row in pull_relevant_dbs_return:
            source_db = row[1]
            source_db_type = row[2]
            db_owner = row[3]
            conn_string = row[4]
            retry = 0
            print ('Checking database status.')
            force_online (retry)

    except Exception as error:
        hard_fail += 1

        print ('Trouble connecting to SQL.')

        if hard_fail == 1:
            mail_body = ('Unable to check database online status at this time.\nError: {0}'.format(error))
            mail_subject = ('Geodatabase Online Check Failed')
            mail_priority = '1'
        else:
            if hard_fail % 5 == 0:
                mail_body = ('Unable to check database online status at this time.  This is attempt #{1}.\nError: {0}'.format(error, hard_fail))
                mail_subject = ('Geodatabase Online Check Failed {0} Times'.format(hard_fail))
                mail_priority = '1'

        send_message(mail_body, mail_subject, mail_priority)

        time.sleep(900)
        check_db()

    return

def force_online(retry):
    print ('Trying to force online.')

    try:
        check_db_SQL = ('''select num_prop_value from [sde].[SDE_server_config] where prop_name = 'Status' ''')
        check_db_return = arcpy.ArcSDESQLExecute(conn_string).execute(check_db_SQL)

        if check_db_return == 1:
            print ('{0} on {1} is online.  There were {2} tries.'.format(source_db, source_db_type, retry))
            mail_body = '{0} on {1} is online.  There were {2} tries.'.format(source_db, source_db_type, retry)
            mail_subject = 'Success! {0} on {1} online'.format (source_db, source_db_type)
            mail_priority = '5'
            if retry > 0:
                send_message(mail_body, mail_subject, mail_priority)
        else:
            print ('{0} on {1} is offline.  Trying....'.format(source_db, source_db_type))
            arcpy.AcceptConnections(conn_string, True)

            if retry % 5 == 0:
                mail_body = ('{0} on {1} is offline and multiple attempts have been made to bring it back online.  '.format(source_db, source_db_type)
                + 'There have been {0} attempts made so far.  This will continue trying until successful.\n\n'.format(retry)
                + 'Additional notifications will be sent if required.')
                mail_subject = 'FAILURE! {0} on {1} offline'.format (source_db, source_db_type)
                mail_priority = '1'
                send_message(mail_body, mail_subject, mail_priority)
                retry += 1
                time.sleep(300)
                force_online(retry)
            else:
                retry += 1
                time.sleep(60)
                force_online(retry)
        hard_fail = 0
    except Exception as error:
        hard_fail += 1

        print ('Trouble connecting to SQL DB {0} on {1}.'.format(source_db, source_db_type))

        if hard_fail == 1:
            mail_body = ('Unable to check database status for {0} on {1} at this time.\n\nError: {2}'.format(source_db, source_db_type, error))
            mail_subject = ('Geodatabase Online Check Failed:  {0} on {1}'.format (source_db, source_db_type))
            mail_priority = '1'
        else:
            if hard_fail % 5 == 0:
                mail_body = ('Unable to check database online status for {0} on {1} at this time.  This is attempt #{2}.\n\nError: {3}'.format(source_db, source_db_type, error, hard_fail))
                mail_subject = ('Geodatabase Online Check Failed {0} Times:  {1} on {2}'.format(hard_fail, source_db, source_db_type))
                mail_priority = '1'

        send_message(mail_body, mail_subject, mail_priority)

        time.sleep(900)
        force_online(retry)

    return

def send_message(mail_body, mail_subject, mail_priority):
    server = smtplib.SMTP(mail_server)

    mail_body = mail_body + ('\n\n[SYSTEM AUTO GENERATED MESSAGE]')

    email_target = 'yours@yours.gov'

    send_mail = 'To: {0}\nFrom: {1}\nX-Priority: {2}\nSubject: {3}\n\n{4}'.format(email_target, mail_from, mail_priority, mail_subject, mail_body)

    print ('Sending message to recipients.')

    server.sendmail(mail_from, email_target, send_mail)
    server.quit()

    return

# Start here
print ('Starting up....')
retry = 0
hard_fail = 0
check_db()
