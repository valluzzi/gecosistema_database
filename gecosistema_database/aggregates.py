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
# Name:        aggregates.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     28/08/2018
# -------------------------------------------------------------------------------
import numpy as np
from scipy import stats

class Ensemble:
    """
    Ensemble
    """
    def __init__(self):
        self.p = 0.5
        self.values  = []

    def step(self, value,probability):
        if (value!=None):
            self.p = probability
            self.values.append(value)

    def finalize(self):
        self.values = np.sort(self.values)
        mean,std = np.mean(self.values),np.std(self.values)
        cdf = stats.norm.cdf(self.values,mean,std)
        for j in range(len(cdf)):
            if cdf[j]>self.p:
                return self.values[j]
        return self.values[-1] if len(self.values)>0 else 0.0