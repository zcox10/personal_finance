# How to use `bqt` command line tools:
once the package is intalled, you get access to a command called `bqt`.

**Note:** if you're using this in a Science Box environment that's accessible as `sb bqt`

For most cases you can see help messages and documentation by issuesing `bqt --help` or to get command specific help `bqt COMMAND --help` (e.g. `bqt delete-table --help`)

## Delete Tables
This utility can delete a single table or a range of tables, from the docs:
```
$ bqt delete-table --help
Usage: bqt delete-table [OPTIONS] NAME

Options:
  --start DATE      start date (inclusive). only if deleting table partitions
  --end DATE        end date (inclusive). only if deleting table partitions
  --all-partitions  delete all paritions. only if deleting table paritions
  --all-yes         Say yes to everything (careful!)
  --help            Show this message and exit.
```

### Examples
Delete one table:
```
$ bqt delete-table my_project:my_dataset.my_table
```

Delete a specific range from a table:
```
$ bqt delete-table my_project:my_dataset.my_table_YYYYMMDD --start=2018-01-01 --end=2018-01-02
```
**Note:** This ranges are inclusive on both ends.

Delete an entire table and all it's partitions:
```
$ bqt delete-table my_project:my_dataset.my_table_YYYYMMDD --all-partitions
```

## Altering Tables
This utility allows you to alter a table (or a range) and:
* Add a new column with a specific definition
* Remove an existing column
* Rename a column
* Cast a column from one data type to another

From the docs:
```
$ bqt alter-table --help
Usage: bqt alter-table [OPTIONS] NAME

Options:
  --rename from:to          Rename column from first value to second value
  --cast col_name:sql_type  Cast column from first value to type in second
                            value
  --add name:definition     Add a new column (first value) using expression in
                            the second value
  --delete TEXT             Delete the provided column
  --start DATE              start date (inclusive). only if altering range of
                            partitions
  --end DATE                end date (inclusive). only if altering range of
                            partitions
  --all-partitions          Apply to all partitions of the table
  --all-yes                 Say yes to everything (careful!)
  --help                    Show this message and exit.
```

### Examples
To add a new column (called `new_col`) with value 2 to all partitions of a table:
```
$ bqt alter-table my_project:my_dataset.my_table_YYYYMMDD --add new_col 2 --all-partitions
```
