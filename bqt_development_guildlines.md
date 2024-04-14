# Development Guildlines
These are a set of suggestions for contribution in this package intended to make the package structure and code consistent and clean.

**As a rule of thumb** When it doubt check the rest of the code base for how things are done.

## Google Style Guild
Please read and comply with [Google Style Guild](https://google.github.io/styleguide/pyguide.html).

PEP8 is also your friend!

## Dates
I suggest using [Arrow](http://arrow.readthedocs.io/en/latest/) as the main date management. You can basically get most string representations as an arrow object using:
```python
arrow.get(date_string)
```

and working the object is then easy:
```python
arrow.format('YYYYMMDD')  # format back into a string
arrow.shift(days=-30)  # take the date back 30 days
arrow.floor('month')  # get the date back to the first of the month
```

## Connections and Concurrency
### Connections
Always create connections through `BqConnection` which can handle all the details and keeps track of them automatically.

### Concurrency
Always use the `JobManager` to queue and wait on jobs, this way multiple parts of the code can queue and wait and the same time. it also makes it easier to track things.

## Table representation
Use `BqTable` to represent a table and when possible accept it as an input along side a string table name. In fact you don't have to worry about that if you write your code as:
```python

def my_awesome_function(self, table):
    for t in BqTable(table):
        # do what your function was supposed to do
```

this way you function by default can accept, one table a range or an entire table with its partitions.

## Tests
All tests are in the `tests` folder. We use `pytest` for writing them. Make sure when you introduce changes you add tests for it.

**If you change a function that doesn't have a test, add one for it ;)**
