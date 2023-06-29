import luigi
import json
import mysql.connector
from datetime import date

# Source database
source_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'wt_library'
}

# Date encoder so that it can be stored in JSON
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


# Extract Task
class ExtractTask(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return []

    def run(self):
        source_conn = mysql.connector.connect(**source_db_config)
        source_cursor = source_conn.cursor()

        # Load the previously extracted data from file
        previous_data = self.load_previous_data()

        # Select the columns that we want to extract
        tables = {
            'employee': 'id',
            'book': 'id, writer, avg_score',
            'keyword': 'id',
            'keyword_books': 'books_id, keywords_id',
            'copy': 'id, book_id',
            'loan': 'id, loan_date, return_date, copy_id, employee_id',
            'review': 'id, rating, book_id, employee_id'
        }

        extracted_data = {}

        # Extract the data for the current version
        for table, columns in tables.items():
            query = f"SELECT {columns} FROM {table}"
            source_cursor.execute(query)
            data = source_cursor.fetchall()

            new_data = self.extract_new_data(table, data, previous_data.get(table, []))
            extracted_data[table] = new_data

        source_conn.close()

        with self.output().open('w') as output_file:
            json.dump(extracted_data, output_file, cls=DateEncoder)

    # Load the previously extracted data from file
    def load_previous_data(self):
        try:
            with self.output().open('r') as input_file:
                return json.load(input_file)
        except FileNotFoundError:
            return {}

    # Compare the current data with the previous data and extract new entries
    def extract_new_data(self, table, current_data, previous_data):
        if table == 'loan':
            # For the 'loan' table, consider the 'id' column for comparison
            previous_ids = set(entry[0] for entry in previous_data)
            new_data = [entry for entry in current_data if entry[0] not in previous_ids]
        else:
            # For other tables, compare the entire entry
            previous_entries = set(tuple(entry) for entry in previous_data)
            new_data = [entry for entry in current_data if tuple(entry) not in previous_entries]

        return new_data

    def output(self):
            filename = f'extracted_data_v{self.version}.json'
            return luigi.LocalTarget(filename)