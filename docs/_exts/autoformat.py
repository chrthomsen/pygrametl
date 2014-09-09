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
        names, it is needed until Sphinx Issue #759 have been resolved.
    """

    # Signatures can belong to either a function, method or object, depending
    # on what version of python is used. Extration of docstrings from objects
    # does in some versions of python require accessing the method first.
    if hasattr(obj, "func_defaults"):
        filename = obj.__code__.co_filename
        lineno = obj.__code__.co_firstlineno
        source_code_line = read_function_signature(filename, lineno)
    elif hasattr(obj, "__code__"):
        filename = obj.__code__.co_filename
        lineno = obj.__code__.co_firstlineno
        source_code_line = read_function_signature(filename, lineno)
    else:
        filename = obj.__init__.__code__.co_filename
        lineno = obj.__init__.__code__.co_firstlineno
        source_code_line = read_function_signature(filename, lineno)

    # The line of source code read from the file, and the original signature, is
    # split into a list of parameters, allowing the function names from the line
    # of source code read to easily substitute the memory addresses present in
    # the original signature given by Sphinx
    signature_split = signature.split(',')
    source_code_line_split = source_code_line.split(',')

    # Function name, def, self, and the ending colon are stripped to match the
    # original signature read by Sphinx, making substituting each part trivial
    param_start_index = source_code_line_split[0].find('(')
    source_code_line_split[0] = source_code_line_split[0][param_start_index:]
    source_code_line_split[-1] = source_code_line_split[-1][0:-1]

    if source_code_line_split[0] == '(self':
         del(source_code_line_split[0])
         source_code_line_split[0] = '(' + source_code_line_split[0]

    # Finally we substitute the pointers with the matching line from source code
    result_string_list = []
    for sig, source in zip(signature_split, source_code_line_split):
        if '<function ' in sig:
            result_string_list.append(source)
        else:
            result_string_list.append(sig)

    # The function pointer block is just replaced with the function_name
    return ','.join(result_string_list)

def read_function_signature(filename, lineno):

    # The line number is subtracted by one to make it match the one produced by
    # the enumerator, as the line number starts from one and the enumerator
    # from zero
    lineno = lineno - 1

    # We read through the file until we line number passed, the reader is then
    # "activated" and we make a copy of all lines read until we match a ":"
    # indicating the end of the function signature which is all we need.
    function_signature = ""
    file_handle = open(filename)
    reached_function_signature = False
    for file_index, line in enumerate(file_handle):

        if file_index == lineno:
            reached_function_signature = True

        if reached_function_signature:
            function_signature += line.strip()

            if line.endswith(':\n'):
                file_handle.close()
                break

    # Finally the all white space is removed from the signature to make it
    # simpler to process in "correct_function_pointers(obj, signature)"
    return function_signature#.strip()

def setup(app):
    """Initial setup that connects the plug-in to Sphinx"""

    # Connection of functions to events raised by Sphinx's autodoc plug-in
    # Documentation: http://sphinx-doc.org/ext/autodoc.html
    app.connect('autodoc-process-docstring', correct_docstring)
    app.connect('autodoc-process-signature', correct_signature)
