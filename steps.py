"""This module contains classes for making "steps" in an ETL flow.
   Steps can be connected such that a row flows from step to step and
   each step does something with the row.
"""

# Copyright (c) 2009, 2010, Christian Thomsen (chr@cs.aau.dk)
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

import pygrametl

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '0.1.1.0'
__all__ = ['Step', 'SourceStep', 'MappingStep', 'ValueMappingStep',
           'PrintStep', 'DimensionStep', 'SCDimensionStep', 'RenamingStep', 
           'GarbageStep', 'ConditionalStep', 'CopyStep',
           'connectsteps']

def connectsteps(*steps):
    """Set a.next = b, b.next = c, etc. when given the steps a, b, c, ..."""
    for i in range(len(steps) - 1):
        steps[i].next = steps[i+1]

class Step(object):
    """The basic class for steps in an ETL flow."""

    __steps = {}

    def __init__(self, worker=None, next=None, name=None):
        """Arguments:
           - worker: A function f(row) that performs the Step's operation.
             If None, self.defaultworker is used. Default: None
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """

        if name is not None:
            self.__class__.__steps[name] = self
        self.__name = name
        self.__redirected = False
        self.__row = None
        self.worker = (worker or self.defaultworker)
        self.next = next
 
    def process(self, row):
        """Perform the Step's operation on the given row.

           If the row is not explicitly redirected (see _redirect), it will
           be passed on the the next step if this has been set.
        """
        self.__redirected = False
        self.__row = row
        self.worker(row)
        self.__row = None
        if self.next is None or self.__redirected:
            return
        self._inject(row, self.next)

    def _redirect(self, target):
        """Redirect the current row to the given target.

           The target is either an instance of Step or the name of a Step
           instance.
        """
        self.__redirected = True
        self._inject(self.__row, target)

    def _inject(self, row, target=None):
        """Give a row to another Step before the current row is passed on.

           The target is either 1) an instance of Step, 2) the name of a Step
           instance, or 3) None. If None, the next default Step is used
           and must be defined.
        """
        if target is None:
            target = self.next

        if isinstance(target, Step):
            target.process(row)
        else:
            self.__class__.__steps[target].process(row)

    def __call__(self, row):
        self.process(row)

    def name(self):
        """Return the name of the Step instance"""
        return self.__name

    @classmethod
    def getstep(cls, name):
        """Return the Step instance with the given name"""
        return cls.__steps.get(name)

    def defaultworker(self, row):
        """Perform the Step's operation on the given row.

           Inheriting classes should implement this method.
        """
        pass



class SourceStep(Step):
    """A Step that iterates over a data source and gives each row to the 
       next step. The start method must be called.
    """

    def __init__(self, source, next=None, name=None):
        """Arguments:
           - source: The data source. Must be iterable.
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, next, name)
        self.source = source

    def start(self):
        """Start the iteration of the source's rows and pass them on."""
        for row in self.source:
            self.process(row)


class MappingStep(Step):
    """A Step that applies functions to attributes in rows."""

    def __init__(self, targets, requiretargets=True, next=None, name=None):
        """Argument:
           - targets: A sequence of (name, function) pairs. For each element,
             row[name] is set to function(row[name]) for each row given to the
             step.
           - requiretargets: A flag that decides if a KeyError should be raised
             if a name from targets does not exist in a row. If True, a 
             KeyError is raised, if False the missing attribute is ignored and
             not set. Default: True
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, next, name)
        self.targets = targets
        self.requiretargets = requiretargets

    def defaultworker(self, row):
        for (element, function) in self.targets:
            if element in row:
                    row[element] = function(row[element])
            elif self.requiretargets:
                raise KeyError, "%s not found in row" % (element,)

class ValueMappingStep(Step):
    """A Step that Maps values to other values (e.g., DK -> Denmark)"""

    def __init__(self, outputatt, inputatt, mapping, requireinput=True, 
                 defaultvalue=None, next=None, name=None):
        """Arguments:
           - outputatt: The attribute to write the mapped value to in each row.
           - inputatt: The attribute to map.
           - mapping: A dict with the mapping itself.
           - requireinput: A flag that decides if a KeyError should be raised
             if inputatt does not exist in a given row. If True, a KeyError
             will be raised when the attriubte is missing. If False, a 
             the outputatt will be set to defaultvalue. Default: True
           - defaultvalue: The default value to use when the mapping cannot be 
             done. Default: None
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, next, name)
        self.outputatt = outputatt
        self.inputatt = inputatt
        self.mapping = mapping
        self.defaultvalue = defaultvalue
        self.requireinput = requireinput

    def defaultworker(self, row):
        if self.inputatt in row:
            row[self.outputatt] = self.mapping.get(row[self.inputatt],
                                                   self.defaultvalue)
        elif not self.requireinput:
            row[self.attribute] = self.defaultvalue
        else:
            raise KeyError, "%s not found in row" % (self.attribute,)


class PrintStep(Step):
    """A Step that prints each given row."""

    def __init__(self, next=None, name=None):
        """Arguments:
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, next, name)

    def defaultworker(self, row):
        print(row)


class DimensionStep(Step):
    """A Step that performs ensure(row) on a given dimension for each row."""

    def __init__(self, dimension, keyfield=None, next=None, name=None):
        """Arguments:
           - dimension: the Dimension object to call ensure on.
           - keyfield: the name of the attribute that in each row is set to 
             hold the key value for the dimension member
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, next, name)
        self.dimension = dimension
        self.keyfield = keyfield

    def defaultworker(self, row):
        key = self.dimension.ensure(row)
        if self.keyfield is not None:
            row[self.keyfield] = key

class SCDimensionStep(Step):
    """A Step that performs scdensure(row) on a given dimension for each row."""

    def __init__(self, dimension, next=None, name=None):
        """Arguments:
           - dimension: the Dimension object to call ensure on.
           - keyfield: the name of the attribute that in each row is set to 
             hold the key value for the dimension member
           - next: The default next step to use. This should be 1) an instance
             of a Step, 2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on. Default: None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, next, name)
        self.dimension = dimension

    def defaultworker(self, row):
        self.dimension.scdensure(row)


class RenamingStep(Step):
    # Performs renamings of attributes in rows.
    def __init__(self, renaming, next=None, name=None):
        Step.__init__(self, None, next, name)
        self.renaming = renaming

    def defaultworker(self, row):
        pygrametl.rename(row, self.renaming)


class GarbageStep(Step):
    """ A Step that does nothing. Rows are neither modified nor passed on."""

    def __init__(self, name=None):
        """Argument:
            - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, None, name)

    def process(self, row):
        return

class ConditionalStep(Step):
    """A Step that redirects rows based on a condition."""

    def __init__(self, condition, whentrue, whenfalse=None, name=None):
        """Arguments:
           - condition: A function f(row) that is evaluated for each row.
           - whentrue: The next step to use if the condition evaluates to a
             true value. This argument  should be 1) an instance of a Step, 
             2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on.
           - whenfalse: The Step that rows are sent to when the condition 
             evaluates to a false value. If None, the rows are silently 
             discarded. Default=None
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
        """
        Step.__init__(self, None, whentrue, name)
        self.whenfalse = whenfalse
        self.condition = condition
        self.__nowhere = GarbageStep()

    def defaultworker(self, row):
        if not self.condition(row):
            if self.whenfalse is None:
                self._redirect(self.__nowhere)
            else:
                self._redirect(self.whenfalse)
        # else process will pass on the row to self.next (the whentrue step)

class CopyStep(Step):
    """A Step that copies each row and passes on the copy and the original"""

    def __init__(self, originaldest, copydest, deepcopy=False, name=None):
        """Arguments:
           - originaldest: The Step each given row is passed on to.
             This argument  should be 1) an instance of a Step, 
             2) the name of a Step, or 3) None.
             If if is a name, the next step will be looked up dynamically
             each time. If it is None, no default step will exist and rows
             will not be passed on.
           - copydest: The Step a copy of each given row is passed on to.
             This argument can be 1) an instance of a Step or 2) the name
             of a step.
           - name: A name for the Step instance. This is used when another
             Step (implicitly or explicitly) passes on rows. If two instanes
             have the same name, the name is mapped to the instance that was
             created the latest. Default: None
           - deepcopy: Decides if the copy should be deep or not. 
             Default: False
        """
        Step.__init__(self, None, originaldest, name)
        if copydest is None:
            raise ValueError, 'copydest is None'
        self.copydest = copydest
        import copy
        if deepcopy:
            self.copyfunc = copy.deepcopy
        else:
            self.copyfunc = copy.copy

    def defaultworker(self, row):
        copy = self.copyfunc(row)
        self._inject(copy, self.copydest)
        # process will pass on row to originaldest = self.next

# For aggregations. Experimental.

class AggregatedRow(dict):
    pass


class AggregatingStep(Step):
    def __init__(self, aggregator=None, finalizer=None, next=None, name=None):
        Step.__init__(self, aggregator, next, name)
        self.finalizer = finalizer or self.defaultfinalizer

    def process(self, row):
        if isinstance(row, AggregatedRow):
            self.finalizer(row)
            if self.next is not None:
                Step._inject(self, row, self.next)
        else:
            self.worker(row)

    def defaultworker(self, row):
        pass

    def defaultfinalizer(self, row):
        pass



class SumAggregator(AggregatingStep):
    def __init__(self, field, next=None, name=None):
        AggregatingStep.__init__(self, None, None, next, name)
        self.sum = 0
        self.field = field

    def defaultworker(self, row):
        self.sum += row[self.field]

    def defaultfinalizer(self, row):
        row[self.field] = self.sum
        self.sum = 0


class AvgAggregator(AggregatingStep):
    def __init__(self, field, next=None, name=None):
        AggregatingStep.__init__(self, None, None, next, name)
        self.sum = 0
        self.cnt = 0
        self.field = field

    def defaultworker(self, row):
        self.sum += row[self.field]
        self.cnt += 1

    def defaultfinalizer(self, row):
        if self.cnt > 0:
            row[self.field] = self.sum / float(self.cnt)
        else:
            row[self.field] = 0

        self.sum = 0
        self.cnt = 0


class MaxAggregator(AggregatingStep):
    def __init__(self, field, next=None, name=None):
        AggregatingStep.__init__(self, None, None, next, name)
        self.max = None
        self.field = field

    def defaultworker(self, row):
        if self.max is None or row[self.field] > self.max:
            self.max = row[self.field]

    def defaultfinalizer(self, row):
        row[self.field] = self.max
        self.max = None


class MinAggregator(AggregatingStep):
    def __init__(self, field, next=None, name=None):
        AggregatingStep.__init__(self, None, None, next, name)
        self.min = None
        self.field = field

    def defaultworker(self, row):
        if self.min is None or row[self.field] < self.min:
            self.min = row[self.field]

    def defaultfinalizer(self, row):
        row[self.field] = self.min
        self.min = None
