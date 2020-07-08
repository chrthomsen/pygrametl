"""Script to automatically format a drawn table testing table."""

# Copyright (c) 2020, Aalborg University (pygrametl@cs.aau.dk)
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
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

# Example function for calling this script from Emacs
# (defun dtt-align-table ()
#   "Format the table at point using an external Python script."
#   (interactive)
#   (save-buffer)
#   (shell-command
#    (concat "python3 formattable.py " (buffer-file-name)
#            " " (number-to-string (line-number-at-pos))))
#   (revert-buffer :ignore-auto :noconfirm))


# Example function for calling this script from Vim
#function! DTTAlignTable()
#    write
#    call system("python3 formattable.py " . expand('%:p') . " " . line('.'))
#    edit!
#endfunction

import sys
import drawntabletesting as dtt

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '2.7.0'


if len(sys.argv) != 3:
    print("usage: " + sys.argv[0] + " file line")
    sys.exit(1)
path = sys.argv[1]
point = int(sys.argv[2]) - 1  # Expected to be one-based

# Extracts the table from the document
with open(path, 'r') as f:
    lines = f.readlines()
    length = len(lines)

    start = point
    while start >= 0 and '|' in lines[start]:
        start -= 1
    start += 1  # Do not include the header

    end = point
    while end < length and '|' in lines[end]:
        end += 1
    end -= 1  # Do not include the delimiter

# The table indention must be taken into account
table = ''.join(lines[start:end + 1])
first_char = table.find('|')
last_char = table.rfind('|')
prefix = table[:first_char]
suffix = table[last_char + 1:]
table = table[first_char:last_char + 1]

# The indention must be added for each line
table = dtt.Table('', table, testconnection=object())
table = str(table).split('\n')

write = 0
indention = '\n' + ' ' * first_char
for output in range(start,end):
    lines[output] = indention + table[write]
    write += 1
lines[start] = prefix + table[0]
lines[end] = indention + table[-1] + suffix

# The file is updated to format the table
with open(path, 'w') as f:
    f.writelines(lines)
