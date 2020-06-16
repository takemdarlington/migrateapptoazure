import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

ADMIN_EMAIL_ADDRESS = os.getenv('ADMIN_EMAIL_ADDRESS') or 'info@techconf.com'
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY') or '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"'
DB_HOST = os.getenv('DB_HOST') or 'migratingappdatabaseserver.postgres.database.azure.com'
DB_USER = os.getenv('DB_USER') or 'darlington@migratingappdatabaseserver'
DB_PASS = os.getenv('DB_PASS') or 'tak651dar,.'
DB_DATABASE = os.getenv('DB_DATABASE') or 'techconfdb'


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_DATABASE
    )

def close_connection(cursor, connection):
    try:
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error('Could not close connection and cursor with error: ' + str(e))


def send_email(email, subject, message):
    try:
        message = Mail(
                from_email=ADMIN_EMAIL_ADDRESS,
                to_emails=email,
                subject=subject,
                html_content='<strong>{}</strong>'.format(message)
            )
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)

        return True
    except Exception as e:
        logging.error('Error sending email: {}'.format(str(e)))
    return False


def get_attendees(cur):
    query = "SELECT first_name,last_name,email FROM attendee;".format(id)
    cur.execute(query)
    rows = cur.fetchall()
    results = []

    if rows != None:
        for row in rows:
            attendee = {}
            attendee['firstName'] = row[0]
            attendee['lastName'] = row[1]
            attendee['email'] = row[2]
            results.append(attendee)
        return results
    return None

def get_notication(cur, id):
    query = "SELECT id,status,message,subject,completed_date FROM notification where id={};".format(id)
    cur.execute(query)
    row = cur.fetchone()

    if row != None:
        notif = {}
        notif['id'] = row[0]
        notif['status'] = row[1]
        notif['message'] = row[2]
        notif['subject'] = row[3]
        notif['completedDate'] = row[4]

        return notif
    return None

def set_notif_complete(cur, con, notif, total):
    query = "UPDATE notification SET status= %s, completed_date = %s WHERE id = %s"

    notif['completedDate'] = datetime.utcnow()
    notif['status'] = 'Notified {} attendees'.format(total)
    
    cur.execute(query, (notif['status'], notif['completedDate'], notif['id']))
    con.commit()
    return

def main(msg: func.ServiceBusMessage):
    notification_id = None
    try:
        notification_id = int(msg.get_body().decode('utf-8'))
        logging.info('Python ServiceBus queue trigger processed message: %s', notification_id)
    except Exception as e:
        logging.error('Message is not valid {}'.format(message.get_body().decode('utf-8')))
        return

    try:
        con = get_connection()
        
        cur = con.cursor()

        notification = get_notication(cur, notification_id)

        if notification != None:
            attendees = get_attendees(cur)
            for attendee in attendees:
                subject = '{}: {}'.format(attendee['firstName'], notification['subject'])
                send_email(attendee['email'], notification['subject'], notification['message'])
            
            set_notif_complete(cur, con, notification, len(attendees))

    except Exception as e:
        logging.error('Exception: ' + str(e))
    finally:
        close_connection(cur, con)
