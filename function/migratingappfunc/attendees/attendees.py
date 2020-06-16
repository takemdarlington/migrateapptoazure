class Attendee:
    def __init__(self, firstName, lastName, email):
        self.firstName = firstName
        self.lastName = lastName
        self.email = email


class Attendees:
    def __init__(self):
        self.queryAllAttendeesEmail = "SELECT first_name,last_name,email FROM attendee;"

    def getAttendees(self, cursor):
        qNbyId = self.queryAllAttendeesEmail.format(id)
        cursor.execute(qNbyId)
        nrows = cursor.fetchall()
        results = []
        for row in nrows:
            results.append(
                Attendee(
                    firstName=row[0],
                    lastName=row[1],
                    email=row[2]
                )
            )
        return results


