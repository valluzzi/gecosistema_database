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
# Name:        sql_utils.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     05/09/2018
# -------------------------------------------------------------------------------
from gecosistema_core import *
import xlrd

SQLTYPES = {
    9999: "",
    9998: "EMPTY",
    1: "TEXT",
    2: "DATETIME",
    3: "DATE",
    4: "TIME",
    5: "FLOAT",
    6: "INTEGER",
    7: "GEOMETRY",
    "": 9999,
    "EMPTY": 9998,
    "TEXT": 1,
    "DATETIME": 2,
    "DATE": 3,
    "TIME": 4,
    "FLOAT": 5,
    "INTEGER": 6,
    "GEOMETRY": 7
}

def sqltype(cvalue, ctype=xlrd.XL_CELL_TEXT, nodata=("", "Na", "NaN", "-", "--", "N/A")):
    """
    Type symbol	Type number	Python value
    XL_CELL_EMPTY	0	empty string u''
    XL_CELL_TEXT	1	a Unicode string
    XL_CELL_NUMBER	2	float
    XL_CELL_DATE	3	float
    XL_CELL_BOOLEAN	4	int; 1 means TRUE, 0 means FALSE
    XL_CELL_ERROR	5	int representing internal Excel codes; for a text representation, refer to the supplied dictionary error_text_from_code
    XL_CELL_BLANK	6	empty string u''. Note: this type will appear only when open_workbook(..., formatting_info=True) is used.
    """
    if isarray(cvalue):
        if not isarray(ctype):
            ctype = [ctype] * len(cvalue)
        return [sqltype(cv, ct, nodata) for cv, ct in zip(cvalue, ctype)]
    if isinstance(cvalue, xlrd.sheet.Cell):
        return sqltype(cvalue.value, cvalue.ctype, nodata)
    if ctype == xlrd.XL_CELL_EMPTY:
        return 'EMPTY'
    elif ctype == xlrd.XL_CELL_TEXT and cvalue in nodata:
        return ''
    elif ctype == xlrd.XL_CELL_TEXT and isdate(cvalue):
        return 'DATE'
    elif ctype == xlrd.XL_CELL_TEXT and isdatetime(cvalue):
        return 'DATETIME'
    elif ctype == xlrd.XL_CELL_TEXT and parseInt(cvalue):
        return 'INTEGER'
    elif ctype == xlrd.XL_CELL_TEXT and parseFloat(cvalue) != None:
        return 'FLOAT'
    elif ctype == xlrd.XL_CELL_TEXT:
        return 'TEXT'
    elif ctype == xlrd.XL_CELL_NUMBER and int(cvalue) == cvalue:
        return 'INTEGER'
    elif ctype == xlrd.XL_CELL_NUMBER:
        return 'FLOAT'
    elif ctype == xlrd.XL_CELL_DATE:
        return 'DATETIME'
    elif ctype == xlrd.XL_CELL_BOOLEAN:
        return 'INTEGER'
    elif ctype == xlrd.XL_CELL_ERROR:
        return ''
    elif ctype == xlrd.XL_CELL_BLANK:
        return ''
    else:
        return 'TEXT'





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