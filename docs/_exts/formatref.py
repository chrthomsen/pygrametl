"""Replaces references to files with a relative reference to a local file when
   the documentation is exported to HTML and an absolute reference to the file
   on pygrametl.org when the documentation is exported to a PDF.
"""

# Copyright (c) 2022, Aalborg University (pygrametl@cs.aau.dk)
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

from pathlib import Path
from docutils import nodes
from sphinx.util import logging

def role(name, rawtext, text, lineno, inliner, options={}, content=[]):
    # text is the roles input, i.e., file name in text <relative file path>
    start_of_path = text.index('<') + 1
    end_of_path = text.rindex('>')
    file_name = text[:start_of_path - 1].strip()
    file_path = text[start_of_path:end_of_path]

    # References the file in an appropriate manner for the output format
    global sphinx_app
    if sphinx_app.builder.format == 'html':
        # For HTML :formatref:` <>` links to the local file like ` <>`_
        node = nodes.reference(rawtext, file_name, 
                refuri=str(file_path), **options)
    elif sphinx_app.builder.format == 'latex':
        # For PDF :formatref:` <>` links to www.pygrametl.org/doc/<file path>
        # Thus, the file path is converted so it is relative to the root of the
        # HTML documentation instead of the folder containing the source file
        source = get_attribute('source', inliner.document.attlist())
        absolute_file_path = (Path(source).parent / file_path).resolve()
        absolute_file_path_parts = absolute_file_path.parts
        index_of_docs = absolute_file_path_parts.index('docs')
        web_file_path = "/".join(absolute_file_path_parts[index_of_docs + 1:])
        uri = "www.pygrametl.org/doc/%s" % web_file_path
        node = nodes.reference(rawtext, file_name, refuri=uri, **options)
    else:
        raise ValueError("Only HTML and LaTeX is supported")
    return [node], []

def get_attribute(name, attributes):
    for attribute in attributes:
        if attribute[0] == name:
            return attribute[1]

def setup(app):
    app.add_role('formatref', role)

    # app is saved as the format builder is not available until role() is run
    global sphinx_app
    sphinx_app = app

    # If multiple formats are produced, e.g., make latexpdf html, Sphinx only
    # stores the name of the first format. Also, Sphinx caches the output of
    # role, so make clean must always be run before make latexpdf or make html
    logger = logging.getLogger(__name__)
    logger.warning('make clean must be run before make html and make latexpdf')
