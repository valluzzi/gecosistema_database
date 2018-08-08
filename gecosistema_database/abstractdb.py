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
# Name:        abstractdb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     31/07/2018
# ------------------------------------------------------------------------------
import os,sys,re
import time
from gecosistema_core import *


class AbstractDB:
    """
    AbstractDB - an asbtract class with common base methods
    """
    def __init__(self, dsn ):
        """
        Constructor
        """
        # Copy contructor
        if isinstance(dsn, (AbstractDB,)):
            db = dsn
            self.dsn = db.dsn
        else:
            mkdirs(justpath(dsn))
            self.dsn = dsn
        self.conn = None
        self.__connect__()

    def close(self, verbose=False):
        """
        Close the db connection
        """
        if self.conn:
            if verbose:
                print("closing db...")
            self.conn.close()

    def __del__(self):
        """
        destructor
        """
        self.close()

    def __get_cursor__(self):
        """
        __get_cursor__
        """
        return self.conn.cursor()

    def __connect__(self):
        """
        __connect__
        """
        raise Exception("Not implemented here!")

    def __prepare_query__(self, sql, env={}, verbose=False):
        """
        prepare the query
        remove comments and blank lines
        """
        comment1 = "--"
        comment2 = "#"
        comment3 = "//"
        sql = re.sub(r'(\r\n|\n)','\n',sql,re.I)
        lines = split(sql, "\n", "'\"")
        print lines

        # follow statement remove comments after SQL line code.
        lines = [split(line, comment1, "'\"")[0] for line in lines]
        lines = [split(line, comment2, "'\"")[0] for line in lines]
        lines = [split(line, comment3, "'\"")[0] for line in lines]
        lines = [line.strip(" \t") for line in lines]

        print lines

        # follow statement remove all lines of comments
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment1)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment2)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment3)]

        if verbose:
            for j in range(len(lines)):
                print("%s) %s"%(j,lines[j]))
                print("-"*80)

        sql = " ".join(lines)
        #remove spaces between stetements
        sql = re.sub(r';\s+',';',sql)

        #env = self.__check_args__(env)

        return sql, env

    def execute(self, sql, env={}, outputmode="array", commit=True, verbose=False, stop_on_error=True):
        """
        Make a query statement list
        Returns a cursor
        """
        rows = []
        cursor = self.__get_cursor__()
        if cursor:
            sql, env = self.__prepare_query__(sql, env, verbose)

            sql = sformat(sql, env)
            commands = split(sql, ";", "'\"")
            commands = [command.strip() + ";" for command in commands if len(command) > 0]

            for command in commands:
                try:
                    t1 = time.time()
                    cursor.execute(command)

                    if commit==True and not command.upper().strip(' \r\n').startswith("SELECT"):
                         self.conn.commit()

                    t2 = time.time()

                    if verbose:
                        command = command.encode('ascii', 'ignore').replace("\n", " ")
                        print("->%s:Done in (%.4f)s" % (command[:], (t2 - t1)))

                except Exception as ex:
                    command = command.encode('ascii', 'ignore').replace("\n", " ")
                    print( "No!:SQL Exception:%s :(%s)"%(command,ex))

                    if outputmode == "response":
                        res = {"status": "fail", "success": False, "exception": ex, "sql": command}
                        return res

                    if stop_on_error:
                        return None

            if outputmode == "cursor":
                return cursor

            elif outputmode == "array":
                for row in cursor:
                    rows.append(row)

            elif outputmode == "scalar":
                row = cursor.fetchone()
                if row and len(row):
                    return row[0]
                else:
                    return None

            elif outputmode == "table":
                metadata = cursor.description
                if metadata:
                    rows.append(tuple([item[0] for item in metadata]))
                for row in cursor:
                    rows.append(row)

            elif outputmode == "object":
                if cursor.description:
                    columns = [item[0] for item in cursor.description]
                    for row in cursor:
                        line = {}
                        for j in range(len(row)):
                            line[columns[j]] = row[j]
                        rows.append(line)

            elif outputmode == "columns":
                n = len(cursor.description)
                rows = [[] for j in range(n)]
                for row in cursor:
                    for j in range(n):
                        rows[j].append(row[j])

            elif outputmode == "response":
                metadata = []
                res = {}
                if cursor.description:
                    metadata = cursor.description
                    columns = [item[0] for item in cursor.description]
                    for row in cursor:
                        line = {}
                        for j in range(len(row)):
                            line[columns[j]] = row[j]
                        rows.append(line)

                    res = {"status": "success", "success": True, "data": rows, "metadata": metadata, "exception": None}
                return res

        return rows

    def executeMany(self, sql, env={}, values=[], commit=True, verbose=False):
        """
        Make a query statetment
        Returns a cursor
        """
        cursor = self.__get_cursor__()
        line = sformat(sql, env)
        try:
            t1 = time.time()
            cursor.executemany(line, values)
            if commit:
                self.conn.commit()
            t2 = time.time()
            if verbose:
                line = line.encode('ascii', 'ignore').replace("\n", " ")
                print("->%s:Done in (%.2f)s" % (line[:], (t2 - t1)))

        except Exception as ex:
            command = command.encode('ascii', 'ignore').replace("\n", " ")
            print( "No!:SQL Exception:%s :(%s)"%(command,ex))