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
from abstractdb import *
from gecosistema_core import *


class SqliteDB(AbstractDB):
    """
    SqliteDB - Next version of sqlite database wrapper
    """
    version = "2.0"

    def __init__(self, filename=":memory:", modules="", verbose=False):
        """
        Constructor
        :param filename:
        :param modules:
        :param verbose:
        """
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
                self.dsn = forceext(self.dsn,"sqlite")
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


    def Execute(text, env=None, outputmode="cursor", verbose=False):
        """
        Execute
        """
        # 1) detect dsn to use
        db = False
        if text:
            text = sformat(filetostr(text), env) if isfile(text) else text

            g = re.search(r'^SELECT\s+\'(?P<filedb>.*)\'\s*;', text, flags=re.I | re.M)
            if g:
                filedb = g.groupdict()["filedb"]
                filedb = forceext(filedb, "sqlite")
                filexls = forceext(filedb, "xls")

                if isfile(filedb):
                    db = SqliteDB(filedb)
                #elif isfile(filexls) and not isfile(filedb):
                #    db = SqliteDB.FromXls(filexls, temp=False)
        if not db:
            db = SqliteDB(":memory:")

        # 2a) detect load_extension and enable extension loading
        if text and db:
            g = re.search(r'^\s*SELECT load_extension\s*\(.*\)', text, flags=re.I | re.M)
            if g:
                db.conn.enable_load_extension(True)

        # 2b) detect functions to load
        if text and db:
            imports = re.findall(
                r'^\s*--\s*from\s*(?P<modulename>\w+)\s+import\s+(?P<fname>(?:\w+(?:\s*,\s*\w+)*)|(?:\*))\s*', text,
                flags=re.I | re.M)
            # print ">>",imports
            for (modulename, fnames) in imports:
                db.load_function(modulename, fnames, verbose=verbose)

        # 3) execute the script
        if db:
            env = env if env else {}
            return db.execute(text, env, outputmode=outputmode, verbose=verbose)

        return None

    Execute = staticmethod(Execute)


if __name__ == "__main__":
    sql = """
    -- from gecosistema_core import *
    SELECT 0;
    SELECT md5("HELLO WORLD"), Desktop();
    """
    chdir(__file__)
    db =SqliteDB("test")
    db.close()
    print SqliteDB.Execute(sql,outputmode="array",verbose=False)