# ------------------------------------------------------------------------------
# Licence:
# Copyright (c) 2012-2018 Luzzi Valerio
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        mssqldb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     13/08/2018
# ------------------------------------------------------------------------------
import os,sys,re
import pyodbc
from abstractdb import *
from gecosistema_core import *


def __readmssqlstring__(text):
    """
    __readmssqlstring__

        "dbname='Catasto' host=.\SQLEXPRESS user='sa' password='12345' srid=4326 type=MultiPolygon table="dbo"."SearchComboBox" (Geometry) sql="  from .qgs file
        "MSSQL:server=.\SQLEXPRESS;trusted_connection=no;uid=sa;pwd=12345;database=Catasto;"   MSSQLSpatial dns
        "DRIVER={SQL Server};SERVER=.\SQLEXPRESS;PORT=1433;DATABASE=Catasto;uid=sa;pwd=12345"  ODBC

    """
    env = {}
    DICTIONARY = {
        "host": "server",
        "user": "uid",
        "password": "pwd",
        "dbname": "database",
        "table": "tablename",
        "type": "geometry_type"
    }
    text = text.replace("MSSQL:", "")

    arr = listify(text, " ;")
    for item in arr:
        item = item.split("=", 1)
        if len(item) > 1:
            key = DICTIONARY[item[0]] if item[0] in DICTIONARY else item[0]
            env[key] = ("%s" % item[1]).strip("'")
    # some correction
    if "tablename" in env and "." in env["tablename"]:
        text = chrtran(env["tablename"], '"', '')
        env["catalog"], env["tablename"] = text.split(".", 1)
    return env


class mssqlDB(AbstractDB):
    """
    mssqlDB - Microsoft Sql database wrapper
    """
    version = "2.0.0"

    def __init__(self, filename="", modules="", verbose=False):
        """
        Constructor
        """
        if isinstance(filename, dict):
            self.__env__ = filename
        elif isstring(filename) and isfile(filename):
            self.__env__ = readmssql(dsn)
        elif isstring(filename):
            self.__env__ = __readmssqlstring__(filename)
        else:
            raise Exception("Unable to initialize the mssql connection!")

        self.dsn = sformat("""DRIVER={SQL Server};SERVER={server};PORT=1433;DATABASE={database};uid={uid};pwd={pwd}""",self.__env__)
        AbstractDB.__init__(self, self.dsn )

    def __connect__(self):
        """
        Connect to the mysql instance
        """
        try:
            self.conn = pyodbc.connect(self.dsn)

        except pyodbc.Error as err:
            print(err)
            self.close()


    def GetTables(self, like="%", verbose=False):
        """
        GetTables - Return a list with all tablenames
        """
        env = {"like":like}
        tables = self.execute("""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME LIKE '{like}';""",env,outputmode="array",verbose=verbose)
        return [item for (item,) in tables]

    def GetFieldNames(self, tablename, ctype="%", typeinfo=False):
        """
        GetFieldNames
        """
        env = {"tablename":tablename, "ctype":ctype}

        #select   COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION, DATETIME_PRECISION,IS_NULLABLE
        #from INFORMATION_SCHEMA.COLUMNS
        #where TABLE_NAME = 'civici'
        if typeinfo:
            sql = """SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '{tablename}' AND DATA_TYPE LIKE '{ctype}';"""
        else:
            sql = """SELECT COLUMN_NAME            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME LIKE '{tablename}' AND DATA_TYPE LIKE '{ctype}';"""
        return self.execute(sql, env, outputmode="array", verbose=True)


    def insertMany(self, tablename, values, commit=True, verbose=False):
        """
        insertMany
        """
        #TODO

    def Execute(text, env=None, outputmode="cursor", verbose=False):
        """
        Execute
        """
        # TODO

    Execute = staticmethod(Execute)

if __name__ == "__main__":
    env = {
        "server": r".\SQLEXPRESS",
        "uid": r"sa",
        "pwd": r"12345",
        "database": "Catasto",
        "tablename": "civici"
    }

    db = mssqlDB(env)

    cursor = db.execute("""SELECT TOP(5) comune FROM  [{database}].[dbo].[{tablename}]""", env, verbose=True)
    #print(cursor)

    filename = Desktop()+"/civici.csv"
    #print(db.toCsv(filename, tables="civici", sep=";", decimal=".", verbose=True))

    print(db.toExcel(filename, tables="civici", sep=";", decimal=".", verbose=True))