# Package Structure

+ bqt
    * lib   # holds functionality shared across classes
        - connection.py  # wrapper around connection, handles concurrency, job manager, ...
        - table_range.py  # wrapper around representing a table (range, all, one)
    * ``__init__``  # this would the factory for all things below
    * reader
    * writer
    * updater  # both alter table and delete
    * auto_yaml
    * gdpr_tools
    * ...
+ tests  # exact same structure as above, with tests per class/functionality
+ examples  # maybe by use case as iPython notebooks

# Factory API
the `__init__.py` will be the factory so you can import that package as:
```python
from bqt import bqt
```

## API Pattern
`__init__.py` is a wrapper that gives access to each class:

```python
bqt.query(...)  # uses bqt.reader.query
bqt.delete(...)  # uses bqt.updater.delete
```
