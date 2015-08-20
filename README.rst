pygrametl (pronounced py-gram-e-t-l) is a Python framework which offers commonly used functionality for development of Extract-Transform-Load (ETL) processes.

pygrametl allows developers to code the ETL process in Python code, instead of drawing it using a graphical user interface. In order to facilitate this, pygrametl provides object oriented abstractions for commonly used operations, such as providing a uniform interface to data from various sources, performing data processing in parallel, maintaining slowly changing dimensions, or creating snowflake schemas.

Providing these abstractions as a framework instead of as an integrated application, allows pygrametl to seamlessly integrate with other Python code. This allows developers to quickly create ETL flows using the abstractions provided, and have direct access to a complete programming language if more complex operations are needed.

**Note:** The parallel capabilities of pygrametl are currently considered experimental and subject to change, and will in many cases give better results if Jython is used instead of CPython, due to its lack of GIL.
