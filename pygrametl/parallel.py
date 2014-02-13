"""This module contains methods and classes for making parallel ETL flows.
   Note that this module in many cases will give better results with Jython
   (where it uses threads) than with CPython (where it uses processes).

   Warning: This is still experimental and things may be changed drastically.
   If you have ideas, comments, bug reports, etc., please report them to
   Christian Thomsen (chr@cs.aau.dk)
"""

# Copyright (c) 2011-2014, Aalborg University (chr@cs.aau.dk)
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

import copy
import os
from Queue import Empty
import sys
if sys.platform.startswith('java'):
    # Jython specific code in jythonmultiprocessing
    import pygrametl.jythonmultiprocessing as multiprocessing
else:
    # Use (C)Python's std. lib.
    import multiprocessing

import pygrametl

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '2.2'
__all__ = ['splitpoint', 'endsplits', 'createflow', 'Decoupled',
           'shareconnectionwrapper', 'getsharedsequencefactory']


# Support for spawned processes to be able to terminate all related processes
# in case of an uncaught exception

_masterpid = os.getpid() # the first to import parallel
_toterminator = None
def _getexitfunction():
    """Return a function that halts the execution of pygrametl.

       pygrametl uses the function as excepthook in spawned processes such that
       an uncaught exception halts the entire execution.
    """
    # On Java, System.exit will do as there are no separate processes
    if sys.platform.startswith('java'):
        def javaexitfunction():
            import java.lang.System
            java.lang.System.exit(1)
        return javaexitfunction
    
    # else see if the os module provides functions to kill process groups;
    # this should be the case on UNIX.
    import signal
    if hasattr(os, 'getpgrp') and hasattr(os, 'killpg'):
        def unixexitfunction():
            procgrp = os.getpgrp()
            os.killpg(procgrp, signal.SIGTERM)
        return unixexitfunction

    # else, we are on a platform that does not allow us to kill a group.
    # We make a special process that gets the pids of all calls to
    # this procedure. The function we return, informs this process to kill
    # all processes it knows about.

    # set up the terminator
    global _toterminator
    if _toterminator is None:
        _toterminator = multiprocessing.Queue()
        def terminatorfunction():
            pids = set([_masterpid])
            while True:
                item = _toterminator.get()
                if type(item) == int:
                    pids.add(item)
                else:
                    # We take it as a signal to kill all
                    for p in pids:
                        os.kill(p, 9) # we don't know which signals exist; use 9
                    return

        terminatorprocess = multiprocessing.Process(target=terminatorfunction)
        terminatorprocess.daemon = True
        terminatorprocess.start()

    # tell the terminator about this process
    _toterminator.put(os.getpid())

    # return a function that tells the terminator to kill all known processes
    def exitfunction():
        _toterminator.put('TERMINATE')

    return exitfunction

def _getexcepthook():
    "Return a function that can be used as except hook for uncaught exceptions."
    if not sys.argv[0]:
        # We are in interactive mode and don't want to terminate
        return sys.excepthook
    # else create a function that terminates all spawned processes and this
    # in case of an uncaught exception
    exit = _getexitfunction()
    def excepthook(exctype, excvalue, exctraceback):
        import traceback
        sys.stderr.write(
            "An uncaught exception occured. Terminating pygrametl.\n")
        traceback.print_exception(exctype, excvalue, exctraceback)
        exit()
    return excepthook



# Stuff for @splitpoint 

splitno = None
def _splitprocess(func, input, output, splitid):
    # The target of a process created for a splitpoint
    global splitno
    splitno = splitid
    sys.excepthook = _getexcepthook() # To handle uncaught exceptions and halt
    (args, kw) = input.get()
    while True:
        res = func(*args, **kw)
        if output is not None:
            output.put(res)
        input.task_done()
        (args, kw) = input.get()

_splitpointqueues = []
def splitpoint(*arg, **kwargs):
    """To be used as an annotation to make a function run in a separate process.

       Each call of a @splitpoint annotated function f involves adding the 
       request (and arguments, if any) to a shared queue. This can be 
       relatively expensive if f only uses little computation time. 
       The benefits from @splitpoint are thus best obtained for a function f 
       which is time-consuming. To wait for all splitpoints to finish their
       computations, call endsplits().

       @splitpoint can be used as in the following examples:

       | @splitpoint
       | def f(args):

           `The simplest case. Makes f run in a separate process.
           All calls of f will return None immediately and f will be
           invoked in the separate process.`

       | @splitpoint()
       | def g(args):

           `With parentheses. Has the same effect as the previous example.`

       | @splitpoint(output=queue, instances=2, queuesize=200)
       | def h(args):

           `With keyword arguments. It is not required that
           all of keyword arguments above are given.`
          
       Keyword arguments:
       - output: If given, it should be a queue-like object (offering the
         .put(obj) method). The annotated function's results will then be put
         in the output
       - instances: Determines how many processes should run the function.
         Each of the processes will have the value parallel.splitno set to
         a unique value between 0 (incl.) and instances (excl.).
       - queuesize: Given as an argument to a multiprocessing.JoinableQueue
         which holds arguments to the annotated function while they wait for
         an idle process that will pass them on to the annotated function.
         The argument decides the maximum number of calls that can wait in the
         queue. 0 means unlimited. Default: 0
    """

    # We construct a function called decorator. We either return this
    # decorator or the decorator applied to the function to decorate.
    # It depends on the user's arguments what to do:
    #
    # When the user uses parentheses after the annotation (as in
    # "@splitpoint(output=x)" or even "@splitpoint()"), arg automatically
    # becomes empty, i.e., arg == (). In that case we return the created
    # decorator such that Python can use it to decorate some function (which
    # we don't know.
    #
    # When no arguments are given (as in "@splitpoint"), arg has a 
    # single element, namely the function to annotate, arg == (<function>,).
    # We then return decorator(function).

    for kw in kwargs.keys():
        if kw not in ('instances', 'output', 'queuesize'):
            raise TypeError, \
                "'%s' is an invalid keyword argument for splitpoint" % kw

    output = kwargs.get('output', None)
    instances = kwargs.get('instances', 1)
    queuesize = kwargs.get('queuesize', 0)

    def decorator(func):
        global _splitpointqueues
        if instances < 1:
            # A special case where there is no process so
            # we just call func directly
            def sillywrapper(*args, **kw):
                res = func(*args, **kw)
                if output is not None:
                    output.put(res)
            return sillywrapper
        # Else set up processes
        input = multiprocessing.JoinableQueue(queuesize)
        for n in range(instances):
            p = multiprocessing.Process(target=_splitprocess,\
                                            args=(func, input, output, n))
            p.name = 'Process-%d for %s' % (n, func.__name__)
            p.daemon = True
            p.start()
        _splitpointqueues.append(input)
        def wrapper(*args,**kw):
            input.put((args, kw))
        return wrapper

    if len(arg) == 0:
        return decorator
    elif len(arg) == 1:
        return decorator(*arg)
    else:
        raise ValueError, 'More than one *arg given'


def endsplits():
    """Wait for all splitpoints to finish"""
    global _splitpointqueues
    for q in _splitpointqueues:
        q.join()


# Stuff for (function) flows

def _flowprocess(func, input, output, inclosed, outclosed):
    sys.excepthook = _getexcepthook() # To handle uncaught exceptions and halt
    retryingafterclose = False
    while True:
        try:
            batch = input.get(True, 0.1)
            for args in batch:
                func(*args)
            input.task_done()
            output.put(batch)
        except Empty:
            if not inclosed.value:
                # A new item may be on its way, so try again
                continue
            elif not retryingafterclose:
                # After the get operation timed out, but before we got to here,
                # an item may have been added before the closed mark got set,
                # so we have to try again
                retryingafterclose = True
                continue
            else:
                # We have now tried get again after we saw the closed mark.
                # There is no more data.
                break
    output.close()
    outclosed.value = 1


class Flow(object):
    """A Flow consists of different functions running in different processes.
       A Flow should be created by calling createflow. 
    """
    def __init__(self, queues, closedarray, batchsize=1000):
        self.__queues = queues
        self.__closed = closedarray
        self.__batchsize = batchsize
        self.__batch = []
        self.__resultbatch = []

    def __iter__(self):
        try:
            while True:
                yield self.get()
        except Empty:
            return

    def __call__(self, *args):
        self.process(*args)
        
    def process(self, *args):
        "Insert arguments into the flow"
        self.__batch.append(args)
        if len(self.__batch) == self.__batchsize:
            self.__queues[0].put(self.__batch)
            self.__batch = []

    def __oneortuple(self, thetuple):
        # If there is only one element in the given tuple, return that element;
        # otherwise return the full tuple.
        if len(thetuple) == 1:
            return thetuple[0]
        return thetuple

    def get(self):
        """Return the result of a single call of the flow.

           If the flow was called with a single argument -- as in 
           flow({'foo':0, 'bar':1}) -- that single argument is returned (with
           the side-effects of the flow preserved).

           If the flow was called with multiple arguments -- as in
           flow({'foo'0}, {'bar':1}) -- a tuple with those arguments is
           returned (with the side-effects of the flow preserved).
        """
        if self.__resultbatch:
            return self.__oneortuple(self.__resultbatch.pop())
        
        # Else fetch new data from the queue
        retryingafterclose = False
        while True:
            try:
                tmp = self.__queues[-1].get(True, 0.1)
                self.__queues[-1].task_done()
                tmp.reverse()
                self.__resultbatch = tmp
                return self.__oneortuple(self.__resultbatch.pop())
            except Empty:
                # See explanation in _flowprocess
                if not self.__closed[-1].value:
                    continue
                elif not retryingafterclose:
                    retryingafterclose = True
                    continue
                else:
                    raise Empty

    def getall(self):
        """Return all results in a single list. 

           The results are of the same form as those returned by get.
        """
        res = []
        try:
            while True:
                res.append(self.get())
        except Empty:
            pass
        return res

    def join(self):
        "Wait for all queues to be empty, i.e., for all computations to be done"
        for q in self.__queues:
            q.join()

    def close(self):
        "Close the flow. New entries can't be added, but computations continue."
        if self.__batch:
            self.__queues[0].put(self.__batch)
            self.__batch = [] # Not really necessary, but ...
        self.__queues[0].close()
        self.__closed[0].value = 1

    @property
    def finished(self):
        "Tells if the flow is closed and all computations have finished"
        for v in self.__closed:
            if not v.value:
                return False
        return True


def _buildgroupfunction(funcseq):
    def groupfunc(*args):
        for f in funcseq:
            f(*args)
        groupfunc.__doc__ = 'group function calling ' + \
            (', '.join([f.__name__ for f in funcseq]))
    return groupfunc

        
def createflow(*functions, **options):
    """Create a flow of functions running in different processes.

       A Flow object ready for use is returned.

       A flow consists of several functions running in several processes.
       A flow created by 

           flow = createflow(f1, f2, f3) 

       uses three processes. Data can be inserted into the flow by calling it
       as in flow(data). The argument data is then first processed by f1(data),
       then f2(data), and finally f3(data). Return values from f1, f2, and f3 
       are *not* preserved, but their side-effects are. The functions in a flow
       should all accept the same number of arguments (*args are also okay).

       Internally, a Flow object groups calls together in batches to reduce
       communication costs (see also the description of arguments below).
       In the example above, f1 could thus work on one batch, while f2 works
       on another batch and so on. Flows are thus good to use even if there
       are many calls of relatively fast functions.

       When no more data is to be inserted into a flow, it should be closed
       by calling its close method.

       Data processed by a flow can be fetched by calling get/getall or simply
       iterating the flow. This can both be done by the process that inserted
       data into the flow or by another (possibly concurrent) process. All
       data in a flow should be fetched again as it otherwise will remain in 
       memory .

       Arguments:
       - *functions: A sequence of functions of sequences of functions.
         Each element in the sequence will be executed in a separate process.
         For example, the argument (f1, (f2, f3), f4) leads to that
         f1 executes in process-1, f2 and f3 execute in process-2, and f4
         executes in process-3.
         The functions in the sequence should all accept the same number of
         arguments.
       - **options: keyword arguments configuring details. The considered
         options are:

         - batchsize: an integer deciding how many function calls are "grouped
           together" before they are passed on between processes. The default
           is 500.
         - queuesize: an integer deciding the maximum number of batches
           that can wait in a JoinableQueue between two different processes. 
           0 means that there is no limit.
           The default is 25.
       
    """
    # A special case
    if not functions:
        return Flow([multiprocessing.JoinableQueue()],\
                        [multiprocessing.Value('b', 0)], 1)

    # Create functions that invoke a group of functions if needed
    resultfuncs = []
    for item in functions:
        if callable(item):
            resultfuncs.append(item)
        else:
            # Check the arguments
            if not hasattr(item, '__iter__'):
                raise ValueError, \
                    'An element is neither iterable nor callable'
            for f in item:
                if not callable(f):
                    raise ValueError, \
                        'An element in a sequence is not callable'
            # We can - finally - create the function
            groupfunc = _buildgroupfunction(item)
            resultfuncs.append(groupfunc)

    # resultfuncs are now the functions we need to deal with.
    # Each function in resultfuncs should run in a separate process
    queuesize = ('queuesize' in options and options['queuesize']) or 0
    batchsize = ('batchsize' in options and options['batchsize']) or 25
    if batchsize < 1:
        batchsize = 25
    queues = [multiprocessing.JoinableQueue(queuesize) for f in resultfuncs]
    queues.append(multiprocessing.JoinableQueue(queuesize)) # for the results
    closed = [multiprocessing.Value('b', 0) for q in queues] # in shared mem
    for i in range(len(resultfuncs)):
        p = multiprocessing.Process(target=_flowprocess, \
                                        args=(resultfuncs[i], \
                                                  queues[i], queues[i+1], \
                                                  closed[i], closed[i+1]))
        p.start()
        
    # Now create and return the object which allows data to enter the flow
    return Flow(queues, closed, batchsize)




### Stuff for Decoupled objects

class FutureResult(object):
    """Represent a value that may or may not be computed yet. 
       FutureResults are created by Decoupled objects.
    """

    def __init__(self, creator, id):
        """Arguments:
        - creator: a value that identifies the creator of the FutureResult.
          Use a primitive value.
        - id: a unique identifier for the FutureResult.
        """
        self.__creator = creator
        self.__id = id

    @property
    def creator(self):
        return self.__creator

    @property
    def id(self):
        return self.__id

    def __setstate__(self, state):
        self.__creator = state[0]
        self.__id = state[1]

    def __getstate__(self):
        return (self.__creator, self.__id)


# TODO: Add more documentation for developers. Users should use Decoupled 
# through its subclasses DecoupledDimension and DecoupledFactTable in 
# pygrametl.tables

class Decoupled(object):
    __instances = []

    def __init__(self, obj, returnvalues=True, consumes=(),
                 directupdatepositions=(),
                 batchsize=500, queuesize=200, autowrap=True):
        self.__instancenumber = len(Decoupled.__instances)
        self.__futurecnt = 0
        Decoupled.__instances.append(self)
        self._obj = obj
        if hasattr(obj, '_decoupling') and callable(obj._decoupling):
            obj._decoupling()
        self.batchsize = batchsize
        self.__batch = []
        self.__results = {}
        self.autowrap = autowrap
        self.__toworker = multiprocessing.JoinableQueue(queuesize)
        if returnvalues:
            self.__fromworker = multiprocessing.JoinableQueue(queuesize)
        else:
            self.__fromworker = None
        self.__otherqueues = dict([(dcpld.__instancenumber, dcpld.__fromworker)\
                                       for dcpld in consumes])
        self.__otherresults = {} # Will store dicts - see also __decoupledworker
        self.__directupdates = directupdatepositions
        
        self.__worker = multiprocessing.Process(target=self.__decoupledworker)
        self.__worker.daemon = True
        self.__worker.name = 'Process for %s object for %s' % \
            (self.__class__.__name__, getattr(obj, 'name', 'an unnamed object'))
        self.__worker.start()


    ### Stuff for the forked process

    def __getresultfromother(self, queuenumber, id):
        while True:
            if id in self.__otherresults[queuenumber]:
                return self.__otherresults[queuenumber].pop(id)
            # else wait for more results to become available
            self.__otherresults[queuenumber].update(
                self.__otherqueues[queuenumber].get())

    def __replacefuturesindict(self, dct):
        res = {}
        for (k, v) in dct.items():
            if isinstance(v, FutureResult) and v.creator in self.__otherqueues:
                res[k] = self.__getresultfromother(v.creator, v.id)
            elif isinstance(v, list):
                res[k] = self.__replacefuturesinlist(v)
            elif isinstance(v, tuple):
                res[k] = self.__replacefuturesintuple(v)
            elif isinstance(v, dict):
                res[k] = self.__replacefuturesindict(v)
            else:
                res[k] = v
        return res

    def __replacefuturesinlist(self, lst):
        res = []
        for e in lst:
            if isinstance(e, FutureResult) and e.creator in self.__otherqueues:
                res.append(self.__getresultfromother(e.creator, e.id))
            elif isinstance(e, list):
                res.append(self.__replacefuturesinlist(e))
            elif isinstance(e, tuple):
                res.append(self.__replacefuturesintuple(e))
            elif isinstance(e, dict):
                res.append(self.__replacefuturesindict(e))
            else:
                res.append(e)
        return res

    def __replacefuturesintuple(self, tpl):
        return tuple(self.__replacefuturesinlist(tpl))

    def __replacefuturesdirectly(self, args):
        for pos in self.__directupdates:
            if len(pos) == 2:
                x, y = pos
                fut = args[x][y]
                args[x][y] = self.__getresultfromother(fut.creator, fut.id)
            elif len(pos) == 3:
                x, y, z = pos
                fut = args[x][y][z]
                args[x][y][z] = self.__getresultfromother(fut.creator, fut.id)
            else:
                raise ValueError, 'Positions must be of length 2 or 3'

    def __decoupledworker(self):
        sys.excepthook = _getexcepthook()
        if hasattr(self._obj, '_decoupled') and callable(self._obj._decoupled):
            self._obj._decoupled()

        for (creatorid, queue) in self.__otherqueues.items():
            self.__otherresults[creatorid] = {}

        while True:
            batch = self.__toworker.get()
            ###
            if batch == 'STOP':
                self.__toworker.task_done()
                return
            ###
            resbatch = []
            for [id, funcname, args] in batch:
                if self.__otherqueues and args:
                    if self.__directupdates:
                        try:
                            self.__replacefuturesdirectly(args)
                        except KeyError:
                            args = self.__replacefuturesintuple(args)
                        except IndexError:
                            args = self.__replacefuturesintuple(args)
                    else:
                        args = self.__replacefuturesintuple(args)
                func = getattr(self._obj, funcname)
                res = func(*args) # NB: func's side-effects on args are ignored
                if id is not None:
                    resbatch.append((id, res)) 
            if self.__fromworker and resbatch:
                self.__fromworker.put(resbatch)
            self.__toworker.task_done()


    ### Stuff for the parent process

    def __getattr__(self, name):
        res = getattr(self._obj, name)
        if callable(res) and self.autowrap:
            def wrapperfunc(*args):
                return self._enqueue(name, *args)
            res = wrapperfunc
        setattr(self, name, res) # NB: Values are only read once...
        return res

    def _enqueue(self, funcname, *args):
        future = FutureResult(self.__instancenumber, self.__futurecnt)
        self.__futurecnt += 1
        self.__batch.append([future.id, funcname, args])
        if len(self.__batch) >= self.batchsize:
            self._endbatch()
        return future

    def _enqueuenoreturn(self, funcname, *args):
        self.__batch.append([None, funcname, args])
        if len(self.__batch) >= self.batchsize:
            self._endbatch()
        return None

    def _getresult(self, future):
        if self.__fromworker is None:
            raise RuntimeError, "Return values are not kept"
        if future.creator != self.__instancenumber:
            raise ValueError, "Cannot return results from other instances"
        # else find and return the result
        while True:
            if future.id in self.__results:
                return self.__results.pop(future.id)
            # else wait for results to become available
            self.__results.update(self.__fromworker.get())

    def _endbatch(self):
        if self.__batch:
            self.__toworker.put(self.__batch)
            self.__batch = []

    def _join(self):
        self._endbatch()
        self.__toworker.join()

    def shutdowndecoupled(self):
        """Let the Decoupled instance finish its tasks and stop it.

        The Decoupled instance should not be used after this.
        """
        self._join()
        self.__toworker.put('STOP')
        self.__toworker.join()
        self.__toworker.close()
        if self.__fromworker is not None:
            self.__fromworker.close()
        try:
            pygrametl._alltables.remove(self)
        except ValueError:
            pass



# SharedConnectionWrapper stuff

class SharedConnectionWrapperClient(object):
    """Provide access to a shared ConnectionWrapper.

    Users should not create a SharedConnectionWrapperClient directly, but
    instead use shareconnectionwrapper to do this.

    Each process should get its own SharedConnectionWrapper by calling
    the copy()/new() method.
    """

    def __init__(self, toserver, fromserver, freelines, connectionmodule,
                 userfuncnames=()):
        self.nametranslator = lambda s: s
        self.__clientid = None
        self.__toserver = toserver
        self.__fromserver = fromserver
        self.__freelines = freelines
        self.__connectionmodule = connectionmodule
        self.__userfuncnames = userfuncnames
        if pygrametl._defaulttargetconnection is None:
            pygrametl._defaulttargetconnection = self

    def __getstate__(self):
        res = self.__dict__.copy()
        res['_SharedConnectionWrapperClient__clientid'] = None
        return res

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__createalluserfuncs() # A new self exists now

    def __del__(self):
        if self.__clientid is not None:
            self.__freelines.put(self.__clientid)

    def __connecttoSCWserver(self):
        self.__clientid = self.__freelines.get()

    def __enqueue(self, method, *args):
        if self.__clientid is None:
            self.__connecttoSCWserver()
        self.__toserver.put((self.__clientid, method, args))

    def __getrows(self, amount):
        # TODO:Should exceptions be transferred to the client and received here?
        self.__enqueue('#get', amount)
        return self.__fromserver[self.__clientid].get()

    def __join(self):
        self.__toserver.join()

    def __createalluserfuncs(self):
        for funcname in self.__userfuncnames:
            setattr(self, funcname, self.__createuserfunc(funcname))

    def __createuserfunc(self, funcname):
        def userfunction(*args):
            self.__enqueue('_userfunc_' + funcname, *args)
            # Wait for the userfunc to finish...
            res = self.__fromserver[self.__clientid].get() # OK after __enqueue
            assert res == 'USERFUNC'
        return userfunction

    def copy(self):
        """ Create a new copy of the SharedConnectionWrapper (same as new) """
        return copy.copy(self)

    def new(self):
        """ Create a new copy of the SharedConnectionWrapper (same as copy) """
        return self.copy()

    def execute(self, stmt, arguments=None, namemapping=None, translate=True):
        """Execute a statement.

           Arguments:
           - stmt: the statement to execute
           - arguments: a mapping with the arguments (default: None)
           - namemapping: a mapping of names such that if stmt uses %(arg)s
             and namemapping[arg]=arg2, the value arguments[arg2] is used 
             instead of arguments[arg]
           - translate: decides if translation from 'pyformat' to the
             undlying connection's format should take place. Default: True
        """
        if namemapping and arguments:
            arguments = pygrametl.copy(arguments, **namemapping)
        elif arguments:
            arguments = arguments.copy()
        self.__enqueue('execute', stmt, arguments, None, translate)

    def executemany(self, stmt, params, translate=True):
        """Execute a sequence of statements."""
        self.__enqueue('executemany', stmt, params, translate)

    def rowfactory(self, names=None):
        """Return a generator object returning result rows (i.e. dicts)."""
        (srvnames, rows) = self.__getrows(0)
        if names is None:
            names = srvnames
        for r in rows:
            yield dict(zip(names, r))

    def fetchone(self, names=None):
        """Return one result row (i.e. dict)."""
        (rownames, row) = self.__getrows(1)
        return dict(zip(names or rownames, row))

    def fetchonetuple(self):
        """Return one result tuple."""
        (rownames, row) = self.__getrows(1)
        return row

    def fetchmanytuples(self, cnt):
        """Return cnt result tuples."""
        (rownames, rows) = self.__getrows(cnt)
        return rows

    def fetchalltuples(self):
        """Return all result tuples"""
        (rownames, rows) = self.__getrows(0)
        return rows
        
    def rowcount(self):
        """Not supported. Returns -1."""
        return -1

    def getunderlyingmodule(self):
        """Return a reference to the underlying connection's module."""
        return self.__connectionmodule

    def commit(self):
        """Commit the transaction."""
        pygrametl.endload()
        self.__enqueue('commit')
        self.__join()

    def close(self):
        """Close the connection to the database,"""
        self.__enqueue('close')

    def rollback(self):
        """Rollback the transaction."""
        self.__enqueue('rollback')
        self.__join()

    def setasdefault(self):
        """Set this ConnectionWrapper as the default connection."""
        pygrametl._defaulttargetconnection = self

    def cursor(self):
        """Return a cursor object. Optional method."""
        raise NotImplementedError

    def resultnames(self):
        (rownames, nothing) = self.__getrows(None)
        return rownames
###

class SharedConnectionWrapperServer(object):
    """Manage access to a shared ConnectionWrapper.

    Users should not create a SharedConnectionWrapperServer directly, but
    instead use shareconnectionwrapper to do this.
    """

    def __init__(self, wrapped, toserver, toclients):
        self.__toserver = toserver
        self.__toclients = toclients
        self.__wrapped = wrapped
        self.__results = [(None, None) for q in toclients] #as (names, [tuples])

    def __senddata(self, client, amount=0):
        # Returns (column names, rows)
        # amount: None: No rows are returned - instead an empty list is sent
        #         0: all rows in a list, 
        #         1: a single row (NOT in a list),
        #         other positive numbers: max. that number of rows in a list. 
        (names, data) = self.__results[client]
        if amount is None:
            rows = []
        elif amount == 1 and data:
            rows = data.pop(0)
        elif amount > 0 and data:
            rows = data[0:amount]
            del data[0:amount]
        else:
            rows = data[:]
            del data[:]

        self.__toclients[client].put((names, rows))

    def worker(self):
        sys.excepthook = _getexcepthook()
        # TODO: Improved error handling such that an exception can be passed on 
        # to the responsible client. It is, however, likely that we cannot
        # continue using the shared DB connection after the exception occured...
        while True:
            (client, method, args) = self.__toserver.get()
            if method == '#get':
                self.__senddata(client, *args)
            elif method.startswith('_userfunc_'):
                target = getattr(self, method)
                target(*args)
                self.__toclients[client].put('USERFUNC')
            elif method == 'close':
                target = getattr(self.__wrapped, method)
                target(*args) # Probably no arguments anyway, but ...
            else: # it must be a function from the wrapped ConnectionWrapper
                target = getattr(self.__wrapped, method)
                target(*args)
                res = self.__wrapped.fetchalltuples()
                if not type(res) == list:
                    # In __senddata we pop/del from a list so a tuple won't work
                    res = list(res)
                self.__results[client] = (self.__wrapped.resultnames(), res)
            self.__toserver.task_done()
            if method == 'close':
                return


def shareconnectionwrapper(targetconnection, maxclients=10, userfuncs=()):
    """Share a ConnectionWrapper between several processes/threads.

    When Decoupled objects are used, they can try to update the DW at the same 
    time. They can use several ConnectionWrappers to avoid race conditions, but
    this is not transactionally safe. Instead, they can use a "shared" 
    ConnectionWrapper obtained through this function.

    When a ConnectionWrapper is shared, it is executing in a separate process
    (or thread, in case Jython is used) and ensuring that only one operation
    takes place at the time. This is hidden from the users of the shared 
    ConnectionWrapper.  They see an interface similar to the normal 
    ConnectionWrapper.

    When this method is called, it returns a SharedConnectionWrapperClient
    which can be used as a normal ConnectionWrapper. Each process 
    (i.e., each Decoupled object) should, however, get a unique
    SharedConnectionWrapperClient by calling copy() on the returned
    SharedConnectionWrapperClient.

    Note that a shared ConnectionWrapper needs to hold the complete result of 
    each query in memory until it is fetched by the process that executed the
    query. Again, this is hidden from the users.

    It is also possible to add methods to a shared ConnectionWrapper when it 
    is created. When this is done and the method is invoked, no other 
    operation will modify the DW at the same time. If, for example,
    the functions foo and bar are added to a shared ConnectionWrapper (by
    passing the argument userfuncs=(foo, bar) to shareconnectionwrapper),
    the returned SharedConnectionWrapperClient will offer the methods
    foo and bar which when called will be running in the separate process
    for the shared ConnectionWrapper. This is particularly useful for
    user-defined bulk loaders as used by BulkFactTable:

    def bulkload():
       `DBMS-specific code here. 
       No other DW operation should take place concurrently`

    scw = shareconnectionwrapper(ConnectionWrapper(...), userfuncs=(bulkload,))

    facttbl = BulkFact(..., bulkloader=scw.copy().bulkload)

    .. Note:: The SharedConnectionWrapper must be copied using .copy(). 

    Arguments:
    - targetconnection: a pygrametl ConnectionWrapper
    - maxclients: the maximum number of concurrent clients. Default: 10
    - userfuncs: a sequence of functions to add to the shared 
      ConnectionWrapper. Default: ()
    """
    toserver = multiprocessing.JoinableQueue(5000)
    toclients = [multiprocessing.Queue() for i in range(maxclients)]
    freelines = multiprocessing.Queue()
    for i in range(maxclients):
        freelines.put(i)
    serverCW = SharedConnectionWrapperServer(targetconnection, toserver, 
                                             toclients)
    userfuncnames = []
    for func in userfuncs:
        if not (callable(func) and hasattr(func, 'func_name') and \
                    not func.func_name == '<lambda>'):
            raise ValueError, "Elements in userfunc must be callable and named"
        if hasattr(SharedConnectionWrapperClient, func.func_name):
            raise ValueError, "Illegal function name: " + func.func_name
        setattr(serverCW, '_userfunc_' + func.func_name, func)
        userfuncnames.append(func.func_name)
    serverprocess = multiprocessing.Process(target=serverCW.worker)
    serverprocess.name = 'Process for shared connection wrapper'
    serverprocess.daemon = True
    serverprocess.start()
    module = targetconnection.getunderlyingmodule()
    clientCW = SharedConnectionWrapperClient(toserver, toclients, freelines,
                                             module, userfuncnames)
    return clientCW




# Shared sequences

def getsharedsequencefactory(startvalue, intervallen=5000):
    """ Creates a factory for parallel readers of a sequence.
    
        Returns a callable f. When f() is called, it returns a callable g.
        Whenever g(*args) is called, it returns a unique int from a sequence
        (if several g's are created, the order of the calls may lead to that
        the returned ints are not ordered, but they will be unique). The
        arguments to g are ignored, but accepted. Thus g can be used as
        idfinder for [Decoupled]Dimensions.

        The different g's can be used safely from different processes and 
        threads.

        Arguments:
        - startvalue: The first value to return. If None, 0 is assumed.
        - intervallen: The amount of numbers that a single g from above
          can return before synchronization is needed to get a new amount.
          Default: 5000.
    """
    if startvalue is None:
        startvalue = 0

    # We use a Queue to ensure that intervals are only given to one deliverer
    values = multiprocessing.Queue(10)

    # A worker that fills the queue
    def valuegenerator(nextval):
        sys.excepthook = _getexcepthook()
        while True:
            values.put((nextval, nextval + intervallen))
            nextval += intervallen

    p = multiprocessing.Process(target=valuegenerator, args=(startvalue,))
    p.daemon = True
    p.start()

    # A generator that repeatedly gets an interval from the queue and returns 
    # all numbers in that interval before it gets a new interval and goes on ...
    def valuedeliverer():
        while True:
            interval = values.get()
            for i in range(*interval):
                yield i

    # A factory method for the object the end-consumer calls
    def factory():
        generator = valuedeliverer() # get a unique generator
        # The method called (i.e., the g) by the end-consumer
        def getnextseqval(*ignored):
            return generator.next()
        return getnextseqval

    return factory



