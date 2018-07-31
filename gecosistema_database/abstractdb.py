# -----------------------------------------------------------------------------
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
# -----------------------------------------------------------------------------
import os,sys,re
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


    def __del__(self):
        """
        destructor
        """
        self.close()

    def __get_cursor__(self):
        return self.conn.cursor()

    def __connect__(self):
        raise Exception("Not implemented here!")

    def __prepare_query__(self, sql, env={}, verbose=False):
        """
        prepare the query
        remove comments and blank lines
        """
        comment1 = "--"
        comment2 = "#"
        sql = re.sub(r'(\r\n|\n)','\n',sql,re.I)
        lines = split(sql, "\n", "'\"")

        # follow statement remove comments after SQL line code.
        lines = [split(line, comment2, "'\"")[0] for line in lines]
        lines = [line.strip(" \t") for line in lines]

        # follow statement remove all lines of comments
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment1)]
        lines = [line for line in lines if len(line) > 0 and not line.startswith(comment2)]

        sql = " ".join(lines)

        env = self.__check_args__(env)

        return sql, env



if __name__ == "__main__":
    sql = """
    SELECT * FROM [data]
    WHERE hello>0
    GROUP BY world;
    """
    db = AbstractDB("test.sqlite")
    print db.__prepare_query__(sql)