"""Automatic addition of additional markup to the doc strings used by pygrametl,
   which should allow them to be readable in the source code and in the 
   documentation after Sphinx has processed them.
"""

# Copyright (c) 2014, Aalborg University (chr@cs.aau.dk)
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

import re
import sys

def correct_docstring(app, what, name, obj, options, lines):
    """Makes some correction to the markup, this should keep it readable in
        the source files, and having the output formatted using Sphinx.
    """

    # Iteration is immutable to prevent lines from being skipped
    for index, value in enumerate(lines):

        # Adds additional backslashes to keep escape sequences as text
        if '\\t' in value or '\\n' in value:
            lines[index] = lines[index].replace("\\", "\\\\")

        # Escapes * in argument descriptions to stop Sphinx using them as markup
        if '*' in value:
            lines[index] = escape_star(value)

        # Formatting of the arguments header with bold and a newline
        if value == 'Arguments:' or value == 'Keyword arguments:':
            lines[index] = '**' + value + '**'
            lines.insert(index+1, '')

def escape_star(line):
    """Escape all unmatched stars (*) so Sphinx know they aren't markup"""
    line_split = line.split()

    for index, value in enumerate(line_split):
        # Star is only added to the end of the word, if the are used for markup
        if not value.endswith('*'):
            line_split[index] = line_split[index].replace("*", "\\*")

    return ' '.join(line_split)


def correct_signature(app, what, name, obj, options, signature,
                      return_annotation):
    """Makes some correction to the markup, to prevent Sphinx from using escape
        sequences instead of just printing them"""

    # Returns the signature are empty, instead of doing None checks everywhere
    if not signature:
        return(signature, return_annotation)

    # Adds additional backslashes to keep escape sequences as text
    if '\\t' in signature or '\\n' in signature:
        signature = signature.replace("\\", "\\\\")

    # Removes the address added by Sphinx if a function pointer have defaults
    if "<function" in signature:
        signature = correct_function_pointers(obj, signature)

    # Side effects are discarded, so we have to return a tuple with new strings
    return(signature, return_annotation)

def correct_function_pointers(obj, signature):
    """Manuel mapping of function pointers with addresses to their original
        names, needed until Sphinx Issue #759 has been resolved.
    """
    
    # We start by splitting the signature into a list of parameters, as it might
    # contain multiple parameters using functions as default parameters.
    signature_split = signature.split(',')
    source_code_line = get_source_code_line(obj, signature)
    source_code_line_split = source_code_line.split(',')

    # We reduce the source code line to just the brackets and the paramters 
    param_start_index = source_code_line_split[0].find('(')
    source_code_line_split[0] = source_code_line_split[0][param_start_index:]
    source_code_line_split[-1] = source_code_line_split[-1][0:-1]
    
    # Finally we substitute the pointers with the matching line from source code 
    result_string_list = []
    for sig, source in zip(signature_split, source_code_line_split):
        if '<function ' in sig:
            result_string_list.append(source)
        else:
            result_string_list.append(sig)

    # The function pointer block is just replaced with the function_name 
    return ','.join(result_string_list)

def get_source_code_line(obj, signature):
    """Extracts the original line of source code from the python file, and
        throws a StandardError if it cannot be found, which properly is a bug in
        this function, as Sphinx found it in that file.
    """

    #TODO: should be extended to ensure that the line we read is inside the
    # correct class, to prevent matching of functions that have the same name
    # and naming of the parameters but placed in another class, as the function
    # we are looking for

    # The list of parameter names is computed and used to identify the function
    # in conjunction with the name, just to be absolutely sure when we pick
    signature_split = signature.split(',')
    for index, line in enumerate(signature_split):
        # Strips the memory addresses of the function pointer parameters
        if '<function ' in line:
            line = line[0 : line.find('=')]
            signature_split[index] = ''.join(ch for ch in line if ch.isalnum())

    # Function pointers used as default parameters result in a signature in the
    # format <function ymdhmsparser at 0x2f45668>, so we extract the name
    function_pointer_name = signature[signature.find('<function ')+10 :
                              signature.rfind('at 0x')-1]

    # If the element we are formatting is a object are looking for the __init__,
    # method, otherwise is it a function or method and we just look for the name
    if type(obj) is type:
        function_name = 'def __init__'
        # The Self pointer is explicit in python, but Sphinx skips it
        signature_split[0] = signature_split[0][1:]
        signature_split.insert(0, '(self')
    else:
        function_name = 'def ' + obj.__name__

    # The original source code is read in order to find the original source line
    source_code_file_name = sys.modules.get(obj.__module__).__file__
    source_code_file = open(source_code_file_name, 'r')

    #NOTE: changing accumulator to a list might increase performance
    accumulator = ''
    correct_method_name = False
    for line in source_code_file:

        if correct_method_name:
            accumulator += line
        elif function_name in line:
            correct_method_name = True
            accumulator += line
        
        if accumulator and '):' in accumulator:
            if (all(param in accumulator for param in signature_split)
                and function_pointer_name in accumulator):
                source_code_line = ' '.join(accumulator.split())
                break
            accumulator = ''
            correct_method_name = False

    source_code_file.close()
    if not source_code_line:
        raise StandardError("Could not find the function in the source code")

    return source_code_line.replace('(self, ', '(')


def setup(app):
    """Initial setup that connects the plug-in to Sphinx"""

    # Connection of functions to events raised by Sphinx's autodoc plug-in
    # Documentation: http://sphinx-doc.org/ext/autodoc.html
    app.connect('autodoc-process-docstring', correct_docstring)
    app.connect('autodoc-process-signature', correct_signature)
