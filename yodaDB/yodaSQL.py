import sqlite3
import sys

class yodaSQL:

    def __init__(self, rowFactory=False **kwargs):
        """
        yodaSQL constructor method:
        :Args:
            rowFactory (bool): Sets the database row factory to sqlite3.Row if set True. Default value is False.
            **kwargs:
                **filename (str): Database filename Default value is "yodadb.db"
                **tablename (str): Database tablename. Default value is "yodatable"
        :Returns: None
        """
        self._filename = kwargs["filename"] if "filename" in kwargs else "yodadb.db"
        self._tablename = kwargs["tablename"] if "tablename" in kwargs else "yodatable"
        self.connect(rowFactory)

    def connect(self, rowFactory=False):
        """
        Creates connection to database. This method is called internally by the constructor.
        :Args:
            rowFactory (bool): Sets the database row factory to sqlite3.Row if set True. Default value is False.
        :Returns: None
        """
        self._database = sqlite3.connect(self._filename)
        if(rowFactory):
            self._database.row_factory = sqlite3.Row
        self._cursor = self._database.cursor()

    def createTable(self, columns, droptable=False):
        """ Creates a table using the tablename provided by constructor. Each yodaSQL instance handles only one table.
        :Args:
            columns (tuple): List of column names for table.\n
            dropTable (bool): Droptable flag. Rewrites the table if set to True. Default value is False
        :Returns: None
        """
        try:
            command = f'CREATE TABLE {self.tablename} {columns}'
            self._cursor.execute(command)
        except sqlite3.OperationalError as sqlError:
            if(droptable):
                print(f'Recreating table...')
                command = f'DROP TABLE {self._tablename}'
                self._database.execute(command)
                command = f'CREATE TABLE {self.tablename} {columns}'
                self._database.execute(command)
                self._database.commit()
            else:
                print(f'Failed to create table due to the following error: {sqlError}')
                print("The droptable table flag is set to False. Set the flag to True to rewrite the table.")
        except Exception as error:
            print(f'Unexpected error: {sys.exc_info()[0]}')

    def printTable(self):
        """
        Prints the table entire table.
        :Args: None
        :Returns: None
        """
        self._database.row_factory = sqlite3.Row
        cursor = self._database.cursor()
        command = f'SELECT * FROM {self.tablename}'
        cursor.execute(command)
        rows = cursor.fetchall()
        print(rows[0].keys())
        for row in rows:
            print(tuple(row))

    def readRow(self, **kwargs ):
        """ Generator that yields database rows.
        :Args:
            Reads every row when no arguments are provided.
            **kwargs:
                whereColumn (str): Optional argument to filter the database. Specifies which column will be used to filter the database.\n
                equals='' (str): Optional argument to filter the database. This is argument is required when the whereColumn argument is provided.\n
                Condition for which the specified column must meet.\n
                returnColumns (tuple): Optional argument that provides an additional filter to return only the
                specified columns instead of every columns for the filtered table that meet the WHERE Clause condition.\n
        :Yields:  A row based on the query. Row is a tuple or sqlite3.Row object.
        """
        kwargsLength = len(kwargs)
        if ( kwargsLength > 1 ):
            returnColumns = ",".join(kwargs.get("returnColumns", "*"))
            query = f'SELECT {returnColumns} FROM {self._tablename} WHERE {kwargs["whereColumn"]}=?'
            parameters = (kwargs["equals"],)
            for row in self._cursor.execute(query, parameters):
                yield row
        else:
            query = f'SELECT * FROM {self._tablename}'
            for row in self._cursor.execute(query):
                yield row


    def insertRow(self, **kwargs):
        """ Inserts row into the database.
        :Args:
            **kwargs:
                values (tuple): Specified values to insert into the database.\n
                forColumns (tuple): Optional argument that specifies which columns new values will be added to.
                If forColumns kwargs is not specified the values kwarg will need provide values for every column of the database.
        :Returns: None
        """
        kwargsLength = len(kwargs)
        if ( kwargsLength == 2 ):
            command = f'INSERT INTO {self.tablename} {kwargs["forColumns"]} VALUES {kwargs["values"]}'
            self._cursor.execute(command)
            self._database.commit()
        elif ( kwargsLength == 1 ):
            command = f'INSERT INTO {self.tablename} VALUES {kwargs["values"]}'
            self._cursor.execute(command)
            self._database.commit()
        else:
            raise Exception(""" Arguments Error: Keyword arguments were not correctly provided.\n 
                            Use values (tuple) and forColumns (tuple) keyword arguments to insert values.""")

    def updateRow(self, **kwargs):
        """ Updates existing row(s) in the database.\n
        This function only queries the database for one column i.e only one where clause can be provided.\n
        Additionally this function only updates the value for one column in the filtered database.
        :Args:
            **kwargs:
                whereColumn (str): Column name used to filter the the database.\n
                equals (str): Value for columns to equal to filter the database.\n
                setColumns (str): Column name that will have its values updated in the filtered database.\n
                toValue (str): New value you would like to insert into the filtered database.
                Value type depends on the value type of the columns specified by the setColumns kwarg.
        :Returns: None
        """
        kwargsLength = len(kwargs)
        if ( kwargsLength == 4 ):
            command = f'UPDATE {self.tablename}  SET {kwargs["setColumn"]}=? WHERE {kwargs["whereColumn"]}=?'
            parameters = (kwargs["toValue"], kwargs["equals"],)
            self._cursor.execute(command, parameters)
            self._database.commit()
        else:
            raise Exception(""" Arguments Error: Keyword arguments were not correctly provided.\n 
                            Use whereColumn (str), equals (str), setColumns (str), and toValue (str) keyword arguments to update values.""")

    def deleteRow(self, **kwargs):
        """ Deletes row from the database.
        :Args:
            **kwargs:
                whereColumn (str): Column name used to filter the the database.\n
                equals (str): Value for columns to equal to filter the database.\n
        :Returns: None
        """
        kwargsLength = len(kwargs)
        if (kwargsLength == 2):
            command = f'DELETE FROM {self._tablename} WHERE {kwargs["whereColumn"]}=?'
            parameters = (kwargs["equals"],)
            self._cursor.execute(command, parameters)
            self._database.commit()
        else:
            raise Exception(""" Arguments Error: Keyword arguments were not correctly provided.\n 
                            Use whereColumn (str) and equals (str)keyword arguments to delete a row.""")

    def execute(self, command, **kwargs):
        """ Executes specified SQL command.
        :Args:
            command (str): SQL command for database.
            *kwargs:
                parameters (tuple): Optional argument that provides a tuple of values for DB-API parameter substitution.\n
                The tuple must have a trailing comma if only one value is provided in the tuple.
        :Returns: None
        """
        kwargsLength = len(kwargs)
        if( kwargsLength == 0):
            self._cursor.execute(command)
            self._database.commit()
        else:
            self._cursor.execute(command, kwargs["parameters"])
            self._database.commit()

    def query(self, query, **kwargs):
        """ Generator that returns specified SQL query.
        :Args:
            query (str): SQL command for database.
            *kwargs:
                parameters (tuple): Optional argument that provides a tuple of values for DB-API parameter substitution.\n
                The tuple must have a trailing comma if only one value is provided in the tuple.
        :Returns: None
        """
        kwargsLength = len(kwargs)
        if( kwargsLength == 0):
            for row in self._cursor.execute(query):
                yield row
        else:
            for row in self._cursor.execute(query, kwargs["paremeters"]):
                yield row


    def close(self):
        self._database.close()
        del self._filename

    def parameterSubs(self, length):
        return ", ".join("?" * length)

    @property
    def filename(self):
        return self._filename

    @filename.setter
    def filename(self, filename):
        self.filename = filename
        self.connect()

    @filename.deleter
    def filename(self):
        self.close()


    @property
    def tablename(self):
        return self._tablename

    @tablename.setter
    def table(self, tablename):
        self._tablename = tablename