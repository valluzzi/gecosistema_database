# -------------------------------------------------------------------------------
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
# Name:        functions.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     29/08/2018
# -------------------------------------------------------------------------------
from gecosistema_core import mapify
from .sqlitedb import SqliteDB

def SQL_EXEC(sql, args):
    """
    SQL_EXEC - run a query o a file.sql
    """
    try:
        env = mapify(args, sep=' ', kvsep='=', strip_char=' ', glue='"')
        res = SqliteDB.Execute(sql, env, outputmode = 'response', verbose=False)
        print res
        print "-----------------------------------"
        return res
    except Exception as ex:
        print("--------------------------")
        print(ex)
        print("--------------------------")

    return 0