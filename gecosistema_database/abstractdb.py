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
import unicodecsv as csv
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

        # follow statement remove comments after SQL line code.
        lines = [split(line, comment1, "'\"")[0] for line in lines]
        lines = [split(line, comment2, "'\"")[0] for line in lines]
        lines = [split(line, comment3, "'\"")[0] for line in lines]
        lines = [line.strip(" \t") for line in lines]

        # follow statement remove all lines of comments
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment1)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment2)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment3)]

        #if verbose:
        #for j in range(len(lines)):
        #    print("%s) %s"%(j,lines[j]))
        #    print("-"*80)

        sql = " ".join(lines)
        #remove spaces between stetements
        sql = re.sub(r';\s+',';',sql)

        #env = self.__check_args__(env)

        return sql, env

    def execute(self, sql, env=None, outputmode="array", commit=True, verbose=False):
        """
        Make a query statement list
        Returns a cursor
        """
        rows = []
        cursor = self.__get_cursor__()
        env = env.copy() if env else {}
        env.update(os.environ)
        if cursor:
            sql, env = self.__prepare_query__(sql, env, verbose)

            sql = sformat(sql, env)
            commands = split(sql, ";", "'\"")
            commands = [command.strip() + ";" for command in commands if len(command) > 0]

            for command in commands:
                t1 = time.time()
                command = sformat(command, env)

                try:

                    cursor.execute(command)

                    if commit==True and not command.upper().strip(' \r\n').startswith("SELECT"):
                         self.conn.commit()

                    env.update(os.environ)

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

    def select(self, tablename, fieldnames="*", orderby="", limit=-1, outputmode="array", verbose=False):
        """
        select
        """
        fieldnames = ",".join(wrap(listify(fieldnames, ","), "[", "]")) if fieldnames != "*" else fieldnames
        orderby = ",".join(wrap(listify(orderby, ","), "[", "]"))
        env = {
            "tablename": tablename,
            "fieldnames": fieldnames,
            "where_clause": "",
            "group_by_clause": "",
            "having_clause": "",
            "order_by_clause": "ORDER BY %s" % orderby if orderby else "",
            "limit_clause": "LIMIT %d" % limit if limit >= 0 else ""
        }
        sql = """
        SELECT {fieldnames} 
            FROM [{tablename}]
                {where_clause}
            {group_by_clause}
            {having_clause}
            {order_by_clause}
                {limit_clause};
        """
        return self.execute(sql, env, outputmode=outputmode, verbose=verbose)

    def createTable(self, tablename, fieldlist,
                    typelist=None,
                    primarykeys=None,
                    Temp=False,
                    overwrite=False,
                    verbose=False):
        """
        Create a Table from field list
        """
        fieldlist = trim(listify(fieldlist))
        typelist = trim(listify(typelist, ","))
        primarykeys = trim(listify(primarykeys))

        # print(fieldlist,typelist,primarykeys)
        typelist = [""] * len(fieldlist) if not typelist else typelist
        fieldnames = ["[%s] %s" % (fieldname, fieldtype) for (fieldname, fieldtype) in zip(fieldlist, typelist)]
        if primarykeys:
            fieldnames += ["""PRIMARY KEY(%s)""" % (",".join(wrap(primarykeys, "[", "]")))]
        fieldnames = ",".join(fieldnames)

        temp = "TEMPORARY" if Temp else ""
        tablename = tablename.strip("[]")
        env = {"tablename": tablename, "TEMPORARY": temp, "fieldnames": fieldnames}
        sql = """"""
        if overwrite:
            sql += """DROP TABLE IF EXISTS [{tablename}];"""

        sql += """CREATE {TEMPORARY} TABLE IF NOT EXISTS [{tablename}]({fieldnames});"""
        self.execute(sql, env, verbose=verbose)

        return tablename


    def toCsv(self, filename, tables="", sep=";", decimal=".", verbose=True):
        """
        Generate a csv file from cursor
        """
        ext = justext(filename).lower()
        filecsv = filename
        dbtables = [tablename.lower() for tablename in self.GetTables()]
        tablenames = listify(tables, ';') if tables else dbtables

        for tablename in tablenames:

            if isquery(tablename):
                cursor = self.execute(tablename, outputmode="cursor")
            elif tablename.lower() in dbtables:
                cursor = self.execute("""SELECT * FROM [%s];"""%(tablename), outputmode="cursor")
            else:
                continue

            metadata = cursor.description
            columnnames = [item[0] for item in metadata]

            if len(tablenames) > 1:
                filecsv = forceext(filename, "[%s].%s" % (tablename, ext))

            # Finally write on csv!!
            with open(filecsv, 'wb') as stream:
                if verbose:
                    print("writing <%s>..." % filecsv)
                writer = csv.writer(stream, dialect='excel', delimiter=sep, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                line = columnnames
                writer.writerow(line)
                for row in cursor:
                    row = [("%s"% (item if item != None else ""))  for item in row]
                    #row = [item for item in row]
                    if decimal == ",":
                        row = [item.replace(".", ",") for item in row]
                    writer.writerow(row)

    def toExcel(self, filename, tables="", verbose=False):
        """
        Generate a excel file from sql query
        """
        ext = justext(filename).lower()
        cursor = None
        dbtables   = self.GetTables()
        tablenames = listify(tables, ';') if tables else dbtables
        if len(tablenames) == 0:
            return False
        # Create or open the workbook
        wb = Workbook(type=ext)

        for tablename in tablenames:
            if verbose:
                print("adding <%s>..." % tablename)

            if isquery(tablename):
                cursor = self.getCursorFor(tablename)
                tablename = tempname("tmp-")
            elif tablename.lower() in lower(dbtables):
                cursor = self.select(tablename, outputmode='cursor')
            else:
                continue

            # Get an existing sheet or create a new one
            sheet = wb.add_sheet(tablename)
            metadata = cursor.description

            all_columns = [item[0] for item in metadata]
            columnnames = [item for item in all_columns if not item.startswith("style-")]
            columnidxs = [all_columns.index(item) for item in columnnames]

            styles = {}
            for columnname in columnnames:
                if "style-" + columnname in all_columns:
                    styles[columnname] = all_columns.index("style-" + columnname)  # index of column-style related

            # Write the header
            i = 0
            for j in range(len(columnnames)):
                sheet.cell(i, j, columnnames[j])
            i = 1
            # For each row,column  write ...
            for row in cursor:
                j = 0
                for jj in range(len(row)):
                    # eclude style-column
                    if jj in columnidxs:
                        # - get style info
                        columnname = all_columns[jj]
                        if columnname in styles:
                            sj = styles[columnname]
                            style = row[sj]
                        else:
                            style = None
                        # ---

                        value = row[jj]
                        if value != None:
                            sheet.cell(i, j, value, style)

                        j += 1
                i += 1

        wb.save(filename)
        return True