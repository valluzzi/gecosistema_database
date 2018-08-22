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
# Name:        http.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     22/08/2018
# -------------------------------------------------------------------------------
from .sqlitedb import SqliteDB
from gecosistema_core import filetostr,isfile,JSONResponse

def SQLResponse(sql, env={}, start_response=None, verbose=False):
    """
    SQLResponse
    """
    if sql:
        try:
            sql = filetostr(sql) if isfile(sql) else sql
            res = SqliteDB.Execute(sql, env, outputmode="response", verbose=verbose)
            return JSONResponse(res, start_response)
        except Exception as ex:
            res = {"status": "fail", "success": False, "exception": "%s" % ex, "sql": sql}
            return JSONResponse(res, start_response)

    # anyway
    res = {"status": "fail", "success": False, "exception": "sql not defined", "sql": sql}
    return JSONResponse(res, start_response)