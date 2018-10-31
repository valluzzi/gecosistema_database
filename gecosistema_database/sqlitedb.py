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
# Name:        sqlitedb.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     31/07/2018
# ------------------------------------------------------------------------------
import os,sys,re
import unicodecsv as csv
import sqlite3 as sqlite
import inspect
try:
    from .abstractdb import *
    from .sql_utils import *
except:
    from abstractdb import *
    from sql_utils import *

from gecosistema_core import *

from multiprocessing import Process



class SqliteDB(AbstractDB):
    """
    SqliteDB - Next version of sqlite database wrapper
    """
    version = "2.0.2"

    def __init__(self, filename=":memory:", modules="", verbose=False):
        """
        Constructor
        :param filename:
        :param modules:
        :param verbose:
        """
        if verbose:
            print("SqliteDB version v%s"%self.version)
        AbstractDB.__init__(self, filename )
        self.pragma("synchronous=OFF",verbose=verbose)
        self.pragma("journal_mode=WAL",verbose=verbose)
        self.pragma("foreign_keys=ON",verbose=verbose)
        self.pragma("cache_size=4000",verbose=verbose)
        self.load_extension(modules, verbose=verbose)
        self.conn.enable_load_extension(True)

    def __connect__(self):
        """
        Connect to the sqlite instance
        """
        try:
            if not self.dsn.startswith(":"):
                self.dsn = forceext(self.dsn,"sqlite") if justext(self.dsn)=="" else self.dsn
            self.conn = sqlite.connect(self.dsn)

        except sqlite.Error as err:
            print(err)
            self.close()


    def pragma(self, text, env={}, verbose=True):
        """
        pragma
        """
        try:
            text = sformat(text,env)
            if verbose:
                print("PRAGMA " + text)
            self.conn.execute("PRAGMA " + text)
        except sqlite.Error as err:
            print(err)

    def create_function(self, func, nargs, fname):
        """
        create_function
        """
        self.conn.create_function(func, nargs, fname)

    def create_aggregate(self, func, nargs, fname):
        """
        create_aggregate
        """
        self.conn.create_aggregate(func, nargs, fname)

    def load_extension(self, modules, verbose=False):
        """
        load_extension
        """
        try:
            modules = listify(modules)
            self.conn.enable_load_extension(True)
            if isLinux() or isMac():
                modules = [os.join(justpath(item), juststem(item)) for item in modules]

            for module in modules:
                try:
                    self.conn.execute("SELECT load_extension('%s');" % (module))
                    if verbose:
                        print("loading module %s...ok!" % (module))
                except OperationalError as ex:
                    print("I can't load  %s because:%s" % (module,ex))

            self.conn.enable_load_extension(False)
        except(Exception):
            print("Unable to load_extension...")

    def load_function(self, modulename="math", fnames="", verbose=False):
        """
        load_function
        :modulename - the name of the module
        """
        try:
            module = __import__(modulename)
            fnames = listify(fnames)
            if "*" in fnames:
                objs = inspect.getmembers(module)
                fnames = [ fname for fname,_ in objs ]

            for fname in fnames:
                try:
                    obj = getattr(module, fname)

                    if inspect.isfunction(obj):
                        n = len(inspect.getargspec(obj).args)
                        self.create_function(fname, n, obj)
                        if verbose:
                            print("load function %s(%s)" % (fname, n))
                    elif inspect.isclass(obj) and "step" in dir(obj):
                        fstep = getattr(obj, "step")
                        n = len(inspect.getargspec(fstep).args) - 1
                        self.create_aggregate(fname, n, obj)
                        if verbose:
                            print("load aggregate function %s(%s)" % (fname, n))
                    elif inspect.isbuiltin(obj):
                        print("Unable to inspect %s because is a built-in functions!"%(fname))
                except Exception as ex1:
                    print("function <%s> not found:%s" % (fname,ex1))
        except Exception as ex2:
            print("module <%s> not found. searching <%s>:%s" % (modulename, fnames, ex2))


    def GetTables(self, like="%"):
        """
        GetTables - Return a list with all tablenames
        """
        sql = """
        SELECT tbl_name FROM sqlite_master      WHERE type IN ('table','view') AND tbl_name LIKE '{like}'
        UNION
        SELECT tbl_name FROM sqlite_temp_master WHERE type IN ('table','view') AND tbl_name LIKE '{like}';""";
        env = {"like": like}
        table_list = self.execute(sql, env, verbose=False)
        table_list = [item[0] for item in table_list]
        return table_list


    def GetFieldNames(self, tablename, ctype="", typeinfo=False):
        """
        GetFieldNames
        """
        env = {"tablename": tablename.strip("[]")}
        sql = """PRAGMA table_info([{tablename}])"""
        info = self.execute(sql, env)
        if typeinfo:
            if not ctype:
                return [(name, ftype) for (cid, name, ftype, notnull, dflt_value, pk) in info]
            else:
                return [(name, ftype) for (cid, name, ftype, notnull, dflt_value, pk) in info if (ftype in ctype)]
        else:
            if not ctype:
                return [name for (cid, name, ftype, notnull, dflt_value, pk) in info]
            else:
                return [name for (cid, name, ftype, notnull, dflt_value, pk) in info if (ftype in ctype)]

        return []

    def insertMany(self, tablename, values, commit=True, verbose=False):
        """
        insertMany
        """
        if isinstance(values, (tuple, list,)) and len(values) > 0:
            # list of tuples
            if isinstance(values[0], (tuple, list,)):
                n = len(values[0])
                env = {"tablename": tablename, "question_marks": ",".join(["?"] * n)}
                sql = """INSERT OR REPLACE INTO [{tablename}] VALUES({question_marks});"""
            # list of objects
            elif isinstance(values[0], (dict,)):
                fieldnames = [item for item in values[0].keys() if item in self.GetFieldNames(tablename)]
                data = []
                for row in values:
                    data.append([row[key] for key in fieldnames])

                n = len(fieldnames)
                env = {"tablename": tablename, "fieldnames": ",".join(wrap(fieldnames, "[", "]")),
                       "question_marks": ",".join(["?"] * n)}
                sql = """INSERT OR REPLACE INTO [{tablename}]({fieldnames}) VALUES({question_marks});"""
                values = data
            else:
                return

            self.executeMany(sql, env, values, commit, verbose)

    def createTableFromCSV(self, filename,
                           dialect = False,
                           tablename="",
                           primarykeys="",
                           append=False,
                           Temp=False,
                           nodata=["", "Na", "NaN", "-", "--", "N/A"],
                           verbose=False):
        """
        createTableFromCSV - make a read-pass to detect data fieldtype
        """
        primarykeys = trim(listify(primarykeys))
        # ---------------------------------------------------------------------------
        #   Open the stream
        # ---------------------------------------------------------------------------
        with open(filename, "rb") as stream:

            # detect the dialect
            if not dialect:
                dialect = csv.Sniffer().sniff(stream.read(1024), delimiters=";,")
                stream.seek(0)

            # ---------------------------------------------------------------------------
            #   decode data lines
            # ---------------------------------------------------------------------------
            fieldnames = []
            fieldtypes = []
            n = 1
            line_no = 0
            header_line_no = 0
            csvreader = csv.reader(stream, dialect)

            for line in csvreader:
                line = [unicode(cell, 'utf-8-sig') for cell in line]
                if len(line) < n:
                    # skip empty lines
                    pass
                elif not fieldtypes:
                    n = len(line)
                    fieldtypes = [''] * n
                    fieldnames = line
                    header_line_no = line_no
                else:
                    fieldtypes = [SQLTYPES[min(SQLTYPES[item1], SQLTYPES[item2])] for (item1, item2) in
                                  zip(sqltype(line, nodata=nodata), fieldtypes)]

                line_no += 1

            self.createTable(tablename, fieldnames, fieldtypes, primarykeys, Temp=Temp, overwrite=not append,
                             verbose=verbose)
            return (fieldnames, fieldtypes, header_line_no)


    def importCsv(self, filename,
                  tablename="",
                  primarykeys="",
                  append=False,
                  Temp=False,
                  nodata=["", "Na", "NaN", "-", "--", "N/A"], verbose=False):
        """
        importCsv
        """
        #detect the dialect
        dialect = None
        with open(filename, "rb") as stream:
            stream.readline()
            dialect = csv.Sniffer().sniff(stream.read(2048), delimiters=";,\t ")
            stream.seek(0)

        tablename = tablename if tablename else juststem(filename)
        if self.createTableFromCSV:
            (fieldnames, fieldtypes, header_line_no) = self.createTableFromCSV(filename, dialect, tablename, primarykeys,
                                                                               append, Temp, nodata, verbose)
        else:
            (fieldnames, fieldtypes, header_line_no) = [],[],0
        # ---------------------------------------------------------------------------
        #   Open the stream
        # ---------------------------------------------------------------------------
        data = []
        line_no = 0
        with open(filename, "rb") as stream:
            dialect = csv.Sniffer().sniff(stream.read(2048), delimiters=";,\t ")
            stream.seek(0)
            reader = csv.reader(stream, dialect)

            for line in reader:
                if line_no > header_line_no:
                    line = [unicode(cell, 'utf-8-sig') for cell in line]
                    #if len(line) == n:
                    data.append(line)
                line_no += 1

            values = [parseValue(row,nodata) for row in data]
            self.insertMany(tablename, values, verbose=verbose)

    @staticmethod
    def ExecuteBranch( text, env=None, outputmode="cursor", verbose=False):
        """
        ExecuteBranch
        """
        db = False
        res = None
        mode="sync"
        # 0) check if text is empty
        if text.strip('\t\r\n ')=="":
            return res

        # 1a) detect dsn to use
        g = re.search(r'^SELECT\s+\'(?P<filedb>.*?)\'\s*(?:,\s*\'(?P<mode>a?sync)\')?;', text, flags=re.I | re.M)
        if g:
            filedb = g.groupdict()["filedb"]
            env.update(os.environ)
            filedb = sformat(filedb,env)
            mode   = g.groupdict()["mode"] if "mode" in g.groupdict() else mode
            if justext(filedb).lower() in ('db', 'sqlite'):
                db = SqliteDB(filedb)
        if not db:
            # no database selected
            db = SqliteDB(":memory:")

        # 1b) detect load_extension and enable extension loading
        g = re.search(r'^\s*SELECT load_extension\s*\(.*\)', text, flags=re.I | re.M)
        if g:
            db.conn.enable_load_extension(True)

        # 1c) detect functions to load
        imports = re.findall(
            r'^\s*--\s*from\s*(?P<modulename>\w+)\s+import\s+(?P<fname>(?:\w+(?:\s*,\s*\w+)*)|(?:\*))\s*', text,
            flags=re.I | re.M)
        # print ">>",imports
        for (modulename, fnames) in imports:
            db.load_function(modulename, fnames, verbose=False)

        # 2) finally execute the script
        res= db.execute(text, env, outputmode=outputmode, verbose=False)

        return res

    @staticmethod
    def ExecuteP(text, env=None, outputmode="cursor", verbose=False):
        """
        ExecuteP - Parallel
        """
        t1 = now()
        res = None
        #env.update(os.environ)
        text = sformat(filetostr(text), env) if isfile(text) else text
        # 1) Split text into branch
        branchs = splitby(r'(SELECT\s+\'.*\'\s*(?:,\s*\'a?sync\')?;)|(SELECT\s+WAIT\s*;)', text, re.I)

        nb = len(branchs)
        running_processes = [] # running processes

        for j in range(nb):
            text = branchs[j]
            g = re.search(r'SELECT\s+\'WAIT\'\s*;', text, flags=re.I | re.M)
            if g:
                print "--------------------- JOIN-----------------------------"
                [p.join() for p in running_processes]
                running_processes = []
                checkpoint(t1,"t1")
                t1= now()
            else:
                g = re.search(r'^SELECT\s+\'(?P<filedb>.*?)\'\s*(?:,\s*\'(?P<mode>a?sync)\')?;', text, flags=re.I | re.M)
                if g:
                    mode   = g.groupdict()["mode"] if "mode" in g.groupdict() else "sync"

                    print "--"*40
                    if mode=="async" and j<nb-1:
                        print "go parallel!"
                        print text
                        p = Process(target=sql_worker, args=(text,env,outputmode,verbose) )
                        p.daemon=True
                        running_processes.append(p)
                        p.start()
                    else:
                        print "go in master thread"
                        print text
                        if j==nb-1:
                           [p.join() for p in running_processes]
                        res = SqliteDB.ExecuteBranch(text,env,outputmode,verbose)

        checkpoint(t1,"t1")
        return res

def sql_worker(sql,env,outputmode,verbose):
    """
    sql_worker -  called by ExecuteP
    """
    t1 = now()
    print "Process Started"
    SqliteDB.ExecuteBranch(sql, env, outputmode, verbose)
    print "[%s] Task done in %s!"%(sql[:32],time_from(t1))

if __name__ == "__main__":
    import os

    chdir(r'D:\Program Files (x86)\SICURA\apps\irriclime\lib\sql')
    filedb = "test.sqlite"
    filecsv = 'G:/Il mio Drive/Projects/3_R&D_CLARA_IDR_31012017_S/IRRICLIME_dev/database/nobackup/smhid10/sm_loucr/R_scripts/export/export_CLARA_GECOS/outputs/198101/timeCPRC_250.txt'
    db = SqliteDB(filedb)
    db.importCsv(filecsv)
    db.close()