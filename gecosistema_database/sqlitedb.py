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
import sqlite3 as sqlite
import inspect
try:
    from .abstractdb import *
except:
    from abstractdb import *
from gecosistema_core import *
from Queue import Queue
from threading import Thread

def splitby(pattern, text, flags=0):
    """
    splitby -  split text by pattern
    """
    res = []
    idxs = [0]
    p = re.compile(pattern, flags)
    for m in p.finditer(text):
        #print m.start(), m.group()
        idxs+= [m.start()]
    idxs+=[len(text)+1]
    for j in range(1,len(idxs)):
        start = idxs[j-1]
        end = idxs[j]
        res.append( text[start:end])
    return res

def sql_worker(q):
    """
    sql_worker
    """
    while True:
        sql,env = q.get()
        SqliteDB.Execute(sql, env, verbose=True)
        q.task_done()

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
        self.load_extension(modules,verbose=verbose)

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

    @staticmethod
    def ExecuteBranch( text, env=None, outpumode="cursor", verbose=False, q=None):
        """
        ExecuteBranch
        """
        db = False
        res = None
        mode = "sync"
        # 0) check if text is empty
        if text.strip('\t\r\n ')=="":
            return res

        # 0a)
        g = re.search(r'SELECT\s+\'WAIT\'\s*;', text, flags=re.I | re.M)
        if g and q:
            q.join()

        # 1a) detect dsn to use
        g = re.search(r'^SELECT\s+\'(?P<filedb>.*)\'\s*(?:,\s*\'(?P<mode>a?sync)\')?;', text, flags=re.I | re.M)
        if g:
            filedb = g.groupdict()["filedb"]
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
            db.load_function(modulename, fnames, verbose=verbose)

        # 2) finally execute the script
        if mode == "sync":
            return db.execute(text, env, outputmode=outputmode, verbose=verbose)
        else:
            if q:
                q.put((text, env, outputmode, verbose))
        return res

    @staticmethod
    def Execute(text, env=None, outputmode="cursor", verbose=False):
        """
        Execute
        """
        db = False
        res = None
        text = sformat(filetostr(text), env) if isfile(text) else text
        #1) Split text into branch
        branchs = splitby(r'SELECT\s+\'.*\'\s*(?:,\s*\'a?sync\')?;',text, re.I)
        for text in branchs:
            # 1)
            if text.strip().startswith("SELECT 'EXIT';"):
                break
            # 1a) detect dsn to use
            g = re.search(r'^SELECT\s+\'(?P<filedb>.*)\'\s*(?:,\s*\'a?sync\')?;', text, flags=re.I | re.M)
            if g:
                filedb = g.groupdict()["filedb"]
                filexls = forceext(filedb, "xls")

                if justext(filedb).lower() in ('db','sqlite'):
                    db = SqliteDB(filedb)

            if not db:
                #no database selected
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
                db.load_function(modulename, fnames, verbose=verbose)

            # 2) execute the script
            res = db.execute(text, env, outputmode=outputmode, verbose=verbose)

        return res

    @staticmethod
    def ExecuteP(text, env=None, outputmode="cursor", verbose=False):
        """
        ExecuteP - Parallel
        """
        res = None
        N = cpu_count()
        text = sformat(filetostr(text), env) if isfile(text) else text
        # 1) Split text into branch
        branchs = splitby(r'SELECT\s+\'.*\'\s*(?P:,\s*\'a?sync\')?;', text, re.I)
        q = Queue(maxsize=0)
        n = min(len(branchs),N)
        for j in range(n-1):
            worker = Thread(target=sql_worker, args=(q,))
            worker.setDaemon(True)
            worker.start()
        for text in branchs[:-1]:
            SqliteDB.ExecuteBranch( text, env, outputmode, verbose, q)

        q.join()
        last_branch = branchs[-1]
        res = SqliteDB.Execute(last_branch,env,outputmode,verbose)
        return res


if __name__ == "__main__":
    import os

    chdir(r'D:\Program Files (x86)\SICURA\apps\irriclime\lib\sql')
    print os.getcwd()

    text = filetostr(r"test.sql")
    SqliteDB.ExecuteP(text,{"hello":1})
