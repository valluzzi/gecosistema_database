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
from gecosistema_core import split
from .sqlitedb import SqliteDB

def SQL_EXEC(sql, args):
    """
    SQL_EXEC - run a query o a file.sql
    """
    try:
        env = {}
        args = split(args, sep=" ", glue='"', removeEmpty=True)
        if len(args):
            # load args in the environment
            for j in range(len(args)):
                arr = args[j].split("=", 1)
                varname = arr[0]
                value   = arr[1] if len(arr) > 1 else ""
                env[varname] = value

        return SqliteDB.Execute(sql, env, outputmode="response")
    except Exception as ex:
        print(ex)

    return {}