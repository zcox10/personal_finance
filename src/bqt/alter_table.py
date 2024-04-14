from bqt.lib.print_tools import PrintTools
from bqt.lib.table import BqTable, BqSingleTable
from bqt.lib.job import BqJobManager
from bqt.writer import Writer


class AlterTable(object):
    """Class to provide a set of tools similar to ALTER TABLE in SQL
    """
    def __init__(self, bqt_obj, dataset, table, start=None, end=None):
        """
        Args:
            table_name (string, BQTable): table(s) to alter
            dataset (string): dataset that table(s) belong to
            project (string): project that dataset belongs to
        """
        self.bqt_obj = bqt_obj
        self.tables = BqTable(
            table, project=bqt_obj.bq_client.project, dataset=dataset,
            start=start, end=end
        )

        self.renames = set()
        self.casts = set()
        self.deletes = set()
        self.adds = set()

    def rename_column(self, old_name, new_name):
        """Rename `old_name` to `new_name`"""
        self.renames.add((old_name, new_name))
        return self

    def cast_column(self, col_name, new_type):
        """change the data type of `col_name` to `new_type`"""
        self.casts.add((col_name, new_type))
        return self

    def delete_column(self, name):
        """delete column `name`"""
        self.deletes.add(name)
        return self

    def add_column(self, name, expression):
        """add a new column named `name` using `expression`

        Example:
            add_column('new_column', '2 * 2 + 5')
            add_column('new_column', 'col_1 + col_2')
        """
        self.adds.add((name, expression))
        return self

    def apply(self, confirm=True):
        """Commits any changes made specified to this object to BQ

        Args:
            concurrency (int): how many jobs to run at once
        """
        self.bqt_obj.print_msg(
            "Going to apply following changes:\n%s" % self.human_summary()
        )
        if confirm and not PrintTools.confirm_action(
                "Are you sure you want to apply?"):
            return

        # first clear out the job queue
        self.bqt_obj.print_progress("Waiting for any pending jobs to finish")
        BqJobManager.wait_for_jobs(halt_on_error=False)

        self.bqt_obj.print_progress("Creating temporary tables with changes")
        writer = Writer(self.bqt_obj)
        # create new tables
        for table in self.tables:
            new_table = BqSingleTable(
                table.project, table.dataset, 'bqttemp_' + str(table)
            )
            if new_table.exists:
                continue
            current_columns = table.columns
            query = self._query(current_columns).format(table=repr(table))
            writer.create_table_async(
                query, new_table.dataset, str(new_table)
            )

        self.bqt_obj.print_progress("Waiting for temp. table jobs to finish")
        BqJobManager.wait_for_jobs()

        self.bqt_obj.print_progress("Replacing original tables")
        # copy the new tables
        for table in self.tables:
            writer.delete_table(table, confirm=False)
            intermediate_table = BqSingleTable(
                table.project, table.dataset, 'bqttemp_' + str(table)
            )
            copy_job = writer.copy_table(intermediate_table, table)
            # need to make sure copy finishes to account for race condition
            # delete is a blocking call so all good there
            if copy_job:
                BqJobManager.wait_for_job(copy_job[0])
            writer.delete_table(intermediate_table, confirm=False)

        BqJobManager.wait_for_jobs()

    def _query(self, current_columns):
        """Turns all the changes to this object into a SQL query"""
        if not self._changes_are_valid():
            raise AssertionError("Changes collide with each other")
        all_cols = []
        col_sql = []
        for old_name, new_name in self.renames:
            col_sql.append("`%s` AS `%s`" % (old_name, new_name))
            all_cols.append(old_name)

        for col_name, new_type in self.casts:
            col_sql.append("CAST(`%s` AS %s) AS `%s`" % (
                col_name, new_type, col_name
            ))
            all_cols.append(col_name)

        for col_name in self.deletes:
            all_cols.append(col_name)

        for col_name, expression in self.adds:
            col_sql.append("%s AS `%s`" % (expression, col_name))

        if all_cols and set(current_columns) - set(all_cols):
            col_sql.append("* EXCEPT(%s)" % ', '.join(
                ['`%s`' % c for c in all_cols]
            ))
        elif not all_cols:
            col_sql.append("*")

        return "SELECT %s FROM `{table}`" % ', '.join(col_sql)

    def _changes_are_valid(self):
        """Some sanity checks on the changes"""
        all_cols = (
            [r[1] for r in self.renames] +
            [d for d in self.deletes] +
            [a[0] for a in self.adds]
        )
        return len(all_cols) == len(set(all_cols))

    def human_summary(self):
        """Create a human readable summary of changes"""
        summary = []
        for old_name, new_name in self.renames:
            summary.append("* Renaming `%s` to `%s`" % (old_name, new_name))

        for col_name, new_type in self.casts:
            summary.append("* Casting `%s` to `%s`" % (col_name, new_type))

        for col_name in self.deletes:
            summary.append("* Deleting `%s`" % (col_name))

        for col_name, expression in self.adds:
            summary.append("* Adding `%s` with definition `%s`" % (
                col_name, expression)
            )

        return "\n".join(summary)
