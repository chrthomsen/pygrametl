""" A module with classes for aggregation.
An Aggregator has two methods: process and finish.

process(group, val) is called to "add" val to the aggregation of the set of 
values identified by the value of group. The value in group (which could be any 
hashable type, also a tuple as ('A', 'B')) thus corresponds to the GROUP BY 
attributes in SQL.

finish(group, default) is called to get the final result for group. If no such
results exists, default is returned.
"""


# Copyright (c) 2011, Christian Thomsen (chr@cs.aau.dk)
# All rights reserved.

# Redistribution and use in source anqd binary forms, with or without
# modification, are permitted provided that the following conditions are met:

# - Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.

# - Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '0.2.0'
__all__ = ['Aggregator', 'SimpleAggregator', 'Sum', 'Count', 'CountDistinct',
           'Max', 'Min', 'Avg']



class Aggregator(object):
    def process(self, group, val):
        raise NotImplementedError

    def finish(self, group, default=None):
        raise NotImplementedError

class SimpleAggregator(Aggregator):
    def __init__(self):
        self._results = {}

    def process(self, group, val):
        pass

    def finish(self, group, default=None):
        return self._results.get(group, default)


class Sum(SimpleAggregator):
    def process(self, group, val):
        tmp = self._results.get(group, 0)
        tmp += val
        self._results[group] = tmp


class Count(SimpleAggregator):
    def process(self, group, val):
        tmp = self._results.get(group, 0)
        tmp += 1
        self._results[group] = tmp


class CountDistinct(SimpleAggregator):
    def process(self, group, val):
        if group not in self._results:
            self._results[group] = set()
        self._results[group].add(val)

    def finish(self, group, default=None):
        if group not in self._results:
            return default
        return len(self._results[group])
    

class Max(SimpleAggregator):
    def process(self, group, val):
        if group not in self._results:
            self._results[group] = val
        else:
            tmp = self._results[group]
            if val > tmp:
                self._results[group] = val


class Min(SimpleAggregator):
    def process(self, group, val):
        if group not in self._results:
            self._results[group] = val
        else:
            tmp = self._results[group]
            if val < tmp:
                self._results[group] = val


class Avg(Aggregator):
    def __init__(self):
        self.__sum = Sum()
        self.__count = Count()

    def process(self, group, val):
        self.__sum.process(group, val)
        self.__count.process(group, val)

    def finish(self, group, default=None):
        tmp = self.__sum.finish(group, None)
        if tmp is None:
            return default
        else:
            return float(tmp) / self.__count(group)
