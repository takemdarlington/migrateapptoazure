class Notification:
    def __init__(self, id, status, message, subject, completedDate):
        self.id = id
        self.message = message
        self.subject = subject
        self.completedDate = completedDate

    def setNotificationCompleted(self, totalAttendees):
        self.completedDate = datetime.utcnow()
        self.status = 'Notified {} attendees'.format(totalAttendees)
        
class Notications:

    def __init__(self):
        self.queryById = "SELECT id,status,message,subject,completed_date FROM notification where id={};"
        self.queryUpdateToCompleted = "UPDATE notification SET status= %s, completed_date = %s WHERE id = %s"

    def getById(self, id, cursor):
        q = self.queryById.format(id)
        cursor.execute(q)
        nrows = cursor.fetchone()
        if nrows != None:
            return Notification(
                id=nrows[0],
                status=nrows[1],
                message=nrows[2],
                subject=nrows[3],
                completedDate=nrows[4]
            )
        return None

    def setCompleted(self, n: Notification, cursor, conn):
        try:
            cursor.execute(self.queryUpdateToCompleted,
                           (n.status, n.completedDate, n.id))

            logging.info("Updated Rows {}".format(cursor.rowcount))

            conn.commit()
            return True
        except Exception as e:
            logging.error('Could commit changes with error: ' + str(e))
            return False
