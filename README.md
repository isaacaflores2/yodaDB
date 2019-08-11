# yodaDB
Small and simple database interface for the Python sqlite3 library. 
Makes use of Python keyword arguments for clean, easy to read, and intuitive code.

## Environment
Python verson 3.7 or higher

## Usage
``` Python

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

```

![](yoda.jpeg)