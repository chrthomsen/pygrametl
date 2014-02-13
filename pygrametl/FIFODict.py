"""A simple mapping between keys and values, but with a limited capacity. When
   the max. capacity is reached, the first inserted key/value pair is deleted
"""

# Copyright (c) 2009-2014, Aalborg University (chr@cs.aau.dk)
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

from collections import deque

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '2.2'
__all__ = ['FIFODict']


class FIFODict:
    """A simple FIFO mapping between keys and values.
       When the max. capacity is reached, the key/value pair that has been in
       the dict the longest time is removed.
    """

    def __init__(self, size, finalizer=None):
        """Create a FIFODict with the given maximum size. 
           
           Arguments:
           - size: Determines the maximum size of the dict.
           - finalizer: If finalizer is given, it must be a callable 
             f(key, value). It is then called, when a item is removed due to 
             the size of the dict reaching the maximum (finalizer is NOT called 
             when an item is explicitly deleted with del d[key] or when the 
             dict is cleared).
        """
        if not type(size) == type(0):
            raise TypeError, "size must be an int"
        if not size > 0:
            raise ValueError, "size must be positive"
        if finalizer is not None and not callable(finalizer):
            raise TypeError, "finalizer must be None or a callable"

        self.__size = size
        self.__data = {}
        self.__order = deque()
        self.__finalizer = finalizer

    def add(self, key, val):
        """Add a key/value pair to the dict. 

           If a pair p with the same key already exists, p is replaced by the 
           new pair n, but n gets p's position in the FIFO dict and is deleted
           when the old pair p would have been deleted. When the maximum 
           capacity is reached, the pair with the oldest key is deleted 
           from the dict. 

           The argument key is the key and the argument val is the value."""
        if key in self.__data:
            self.__data[key] = val # Replace old value
        elif len(self.__order) < self.__size:
            # The dict is not full yet. Just add the new pair.
            self.__order.append(key)
            self.__data[key] = val
        else:
            # The dict is full. We have to delete the oldest item first.
            delKey = self.__order.popleft()
            if self.__finalizer:
                self.__finalizer(delKey, self.__data[delKey])
            del self.__data[delKey]
            self.__order.append(key)
            self.__data[key] = val
                
    def get(self, key, default=None):
        """Find and return the element a given key maps to.

        Look for the given key in the dict and return the associated value 
        if found. If not found, the value of default is returned."""
        return self.__data.get(key, default)

    def clear(self):
        """Delete all key/value pairs from the dict"""
        self.__data = {}
        self.__order = []
        

    def __setitem__(self, key, item):
        self.add(key, item)

    def __getitem__(self, key):
        return self.__data[key]

    def __len__(self):
        return len(self.__data)

    def __str__(self):
        allitems = []
        for key in self.__order:
            val = self.__data[key]
            item = "%s: %s" % (str(key), str(val))
            allitems.append(item)
        return "{%s}" % ", ".join(allitems)

    def __contains__(self, item):
        return (item in self.__data)

    def __delitem__(self, item):
        if item not in self.__data:
            raise KeyError, item

        del self.__data[item]
        self.__order.remove(item)

    def __iter__(self):
        for k in self.__order:
            yield k
