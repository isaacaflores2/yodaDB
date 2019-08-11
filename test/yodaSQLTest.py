import unittest
from yodaSQL import yodaSQL

filename = "class.db"
tablename = "students"
tableColumns = ("StudentName", "StudentID", "Grade")
testRows = [("Princess Leia", "1", "A"), ("Han Solo", "2", "B"), ("Chewbacca", "3", "B"), ("R2-D2", "4", None)]

class yodaSQLTest(unittest.TestCase):

    def testInit(self):
        db = yodaSQL()
        self.assertEqual(db.filename, "yodadb.db")
        self.assertEqual(db.tablename, "yodatable")
        db.close()

    def testConnect(self):
        db = yodaSQL()
        self.assertIsNotNone(db._database)
        db.close()

        db = yodaSQL(filename=filename, tablename=tablename)
        self.assertIsNotNone(db._database)
        db.close()

    def testInsertAndReadRow(self):
        db = yodaSQL(filename=filename, tablename=tablename)
        db.createTable(columns=tableColumns, droptable=True)

        db.insertRow(values=testRows[0])
        db.insertRow(values=testRows[1])
        db.insertRow(values=testRows[2])
        db.insertRow(values=testRows[3][:2], forColumns=("StudentName", "StudentID"))

        i = 0
        for row in db.readRow():
            rowValues = tuple([value for value in row])
            self.assertEqual(testRows[i], rowValues)
            i+=1

        for row in db.readRow(whereColumn="StudentName", equals="Princess Leia"):
            rowValues = tuple([value for value in row])
            self.assertEqual(testRows[0], rowValues)

        for row in db.readRow(whereColumn="StudentName", equals="Han Solo", returnColumns=("StudentName", "StudentID")):
            rowValues = tuple([value for value in row])
            self.assertEqual(testRows[1][:2], rowValues)

        db.close()

    def testUpdateRow(self):
        db = yodaSQL(filename=filename, tablename=tablename)
        updatedTestRow = ("Princess Leia", "1", "F")
        db.updateRow(whereColumn="StudentName", equals="Princess Leia", setColumn="Grade", toValue="F")

        for row in db.readRow(whereColumn="StudentName", equals="Princess Leia"):
            rowValues = tuple([value for value in row])
            self.assertEqual(updatedTestRow, rowValues)

        db.close()


    def testDeleteRow(self):
        db = yodaSQL(filename=filename, tablename=tablename)
        db.deleteRow(whereColumn="StudentName", equals="Chewbacca")

        expectedRow = None
        for row in db.readRow(whereColumn="StudentName", equals="Chewbacca"):
            expectedRow = tuple([value for value in row])

        self.assertIsNone(expectedRow)

        db.close()

    def testExecute(self):
        db = yodaSQL(filename=filename, tablename=tablename)

        sqlCommand = f'UPDATE {db.tablename}  SET GRADE="F" WHERE StudentName="R2-D2"'
        db.execute(sqlCommand)

        for row in db.readRow(whereColumn="StudentName", equals="R2-D2"):
            returnedRow = tuple([value for value in row])

        expectedRow = ("R2-D2", "4", "F")
        self.assertEqual(expectedRow, returnedRow)

        sqlCommand = f'UPDATE {db.tablename}  SET GRADE=? WHERE StudentName=?'
        db.execute(sqlCommand, parameters=("D", "R2-D2"))

        for row in db.readRow(whereColumn="StudentName", equals="R2-D2"):
            returnedRow = tuple([value for value in row])

        expectedRow = ("R2-D2", "4", "D")
        self.assertEqual(expectedRow, returnedRow)

        db.close()


    def testQuery(self):
        db = yodaSQL(filename=filename, tablename=tablename)

        query = f'SELECT * FROM {db.tablename} WHERE StudentName="R2-D2"'
        for row in db.query(query):
            returnedRow = tuple([value for value in row])

        expectedRow = ("R2-D2", "4", None)
        self.assertEqual(expectedRow, returnedRow)

        query = f'SELECT * FROM {db.tablename} WHERE StudentName=?'
        for row in db.query(query, paremeters=("R2-D2",)):
            returnedRow = tuple([value for value in row])

        expectedRow = ("R2-D2", "4", None)
        self.assertEqual(expectedRow, returnedRow)

        db.close()

    def testReadMe(self):
        # Create instace of yodaSQL
        db = yodaSQL(filename="myclass.db", tablename="students")

        # Create table
        db.createTable(columns=("StudentName", "StudentID", "Grade"))

        # Insert data
        db.insertRow(values=("Princess Leia", "1", "A"))
        db.insertRow(values=("Han Solo", "2", "B"))
        db.insertRow(values=("Chewbacca", "3", "B"))
        db.insertRow(values=("R2-D2", "4"), forColumns=("StudentName", "StudentID"))

        # Read data
        for row in db.readRow():
            rowData = [value for value in row]
            print(rowData)

        #Update data
        db.updateRow(whereColumn="StudentName", equals="Princess Leia", setColumn="Grade", toValue="F")

        #Delete data
        db.deleteRow(whereColumn="StudentName", equals="Chewbacca")

        expectedRow = None
        for row in db.readRow(whereColumn="StudentName", equals="Chewbacca"):
            expectedRow = tuple([value for value in row])

        self.assertIsNone(expectedRow)

if __name__ == "__main__":
    unittest.main()
