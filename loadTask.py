import luigi
import json
import mysql.connector
from transformTask import TransformTask

''' 
    Variables
'''

# Target database
target_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'wt_recommender'
}

''' 
    Global functions
'''

# Function to load the data from the previous version
def load_previous_data(version):
    previous_version = version - 1
    filename = f'transformed_data_v{previous_version}.json'
    previous_data = {}
    try:
        previous_target = luigi.LocalTarget(filename)
        if previous_target.exists():
            with previous_target.open('r') as input_file:
                previous_data = json.load(input_file)
    except FileNotFoundError:
        pass
    return previous_data

''' 
    Load tasks

    Every table that is loaded is split into a different task.
    This way we can parallel run multiple tasks using Luigi's requirement
    design pattern.
'''

# Load User_DIM 
class LoadUserDim(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return TransformTask(version=self.version)

    def run(self):
        with self.input().open('r') as input_file:
            transformed_data = json.load(input_file)

        # Load the previously loaded data from file
        previous_data = load_previous_data(self.version)

        destination_conn = mysql.connector.connect(**target_db_config)
        destination_cursor = destination_conn.cursor()

        # Load data into User_DIM table with new surrogate keys for new entries
        self.load_user_dim(destination_cursor, transformed_data['User_DIM'], previous_data.get('User_DIM', []))

        destination_conn.commit()  # Commit the changes
        destination_conn.close()  # Close the connection

        # Make a .txt to log the version
        with self.output().open('w') as output_file:
            output_file.write(str(self.version))

    # Load data into User_DIM table with new surrogate keys for new entries
    def load_user_dim(self, cursor, data, previous_data):
        previous_user_ids = set(entry[0] for entry in previous_data)
        values = []
        for row in data:
            if row[0] not in previous_user_ids:
                values.append(row[0])

        # Combine all the to be inserted values into one SQL statement
        if values:
            placeholders = ', '.join(['(%s)'] * len(values))
            sql = f"INSERT INTO User_DIM (user_id) VALUES {placeholders}"
            print(sql)
            cursor.execute(sql, values)

    def output(self):
        filename = f'user_load_complete_v{self.version}.txt'
        return luigi.LocalTarget(filename)


# Load Keyword_DIM 
class LoadKeywordDim(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return TransformTask(version=self.version)

    def run(self):
        with self.input().open('r') as input_file:
            transformed_data = json.load(input_file)

        # Load the previously loaded data from file
        previous_data = load_previous_data(self.version)

        destination_conn = mysql.connector.connect(**target_db_config)
        destination_cursor = destination_conn.cursor()

        # Load data into Keyword_DIM table with new surrogate keys for new entries
        self.load_keyword_dim(destination_cursor, transformed_data['Keyword_DIM'], previous_data.get('Keyword_DIM', []))

        destination_conn.commit()  # Commit the changes
        destination_conn.close()  # Close the connection

        # Make a .txt to log the version
        with self.output().open('w') as output_file:
            output_file.write(str(self.version))

    # Load data into Keyword_DIM table with new surrogate keys for new entries
    def load_keyword_dim(self, cursor, data, previous_data):
        previous_keyword_ids = set(entry[0] for entry in previous_data)
        values = []
        for row in data:
            if row[0] not in previous_keyword_ids:
                values.append(row[0])

        # Combine all the to be inserted values into one SQL statement
        if values:
            placeholders = ', '.join(['(%s)'] * len(values))
            sql = f"INSERT INTO Keyword_DIM (keyword_id) VALUES {placeholders}"
            cursor.execute(sql, values)

    def output(self):
        filename = f'keyword_load_complete_v{self.version}.txt'
        return luigi.LocalTarget(filename)


# Load Book_DIM 
class LoadBookDim(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return TransformTask(version=self.version)

    def run(self):
        with self.input().open('r') as input_file:
            transformed_data = json.load(input_file)

        # Load the previously loaded data from file
        previous_data = load_previous_data(self.version)

        destination_conn = mysql.connector.connect(**target_db_config)
        destination_cursor = destination_conn.cursor()

        # Load data into Book_DIM table with new surrogate keys for new entries
        self.load_book_dim(destination_cursor, transformed_data['Book_DIM'], previous_data.get('Book_DIM', []))

        destination_conn.commit()  # Commit the changes
        destination_conn.close()  # Close the connection

        # Make a .txt to log the version
        with self.output().open('w') as output_file:
            output_file.write(str(self.version))

    # Load data into Book_DIM table with new surrogate keys for new entries
    def load_book_dim(self, cursor, data, previous_data):
        previous_book_ids = {entry[0] for entry in previous_data}
        insert_values = []
        update_values = []

        for row in data:
            book_id = row[0]
            writer = row[1]
            avg_score = row[2]

            # If the book exists in previous_data, check if writer has changed
            if book_id in previous_book_ids:
                for prev_row in previous_data:
                    if prev_row[0] == book_id:
                        if prev_row[1] != writer:
                            update_values.append((writer, book_id))
                        break
            else:
                insert_values.append((book_id, writer, avg_score))

        # Insert new books
        if insert_values:
            placeholders = ', '.join(['(%s, %s, %s)'] * len(insert_values))
            insert_sql = f"INSERT INTO Book_DIM (book_id, writer, avg_score) VALUES {placeholders}"
            insert_values_flat = [value for row in insert_values for value in row]
            cursor.execute(insert_sql, insert_values_flat)

        # Update writer of edited books new value
        for writer, book_id in update_values:
            cursor.execute("UPDATE Book_DIM SET writer = %s WHERE book_id = %s",
                        (writer, book_id))

    def output(self):
        filename = f'book_load_complete_v{self.version}.txt'
        return luigi.LocalTarget(filename)


# Load Book_Keyword_FACT 
class LoadBookKeywordFact(luigi.Task):
    version = luigi.IntParameter(default=1)

    # The new books and keywords have to be loaded to access the surrogate keys
    def requires(self):
        return [TransformTask(version=self.version), LoadBookDim(version=self.version),LoadKeywordDim(version=self.version)]

    def run(self):
        with self.input()[0].open('r') as input_file:
            transformed_data = json.load(input_file)

        # Load the previously loaded data from file
        previous_data = load_previous_data(self.version)

        destination_conn = mysql.connector.connect(**target_db_config)
        destination_cursor = destination_conn.cursor()

        # Load data into Book_Keyword_FACT table with new surrogate keys and foreign key mappings
        self.load_book_keyword_fact(destination_cursor, transformed_data['Book_Keyword_FACT'], previous_data.get('Book_Keyword_FACT', []))

        destination_conn.commit()  # Commit the changes
        destination_conn.close()  # Close the connection

        # Save the current version in log when complete
        with self.output().open('w') as output_file:
            output_file.write(str(self.version))

    # Load data into Book_Keyword_FACT table with new surrogate keys and foreign key mappings
    def load_book_keyword_fact(self, cursor, data, previous_data):
        previous_entries = set(tuple(entry) for entry in previous_data)
        insert_values = []

        for row in data:
            if tuple(row) not in previous_entries:
                book_id = row[0]
                keyword_id = row[1]
                insert_values.append((book_id, keyword_id))

        # MySQL has a statement limit of 8,192 characters. Set the maximum values to 7000 to keep some play.
        max_values_per_insert = 7000

        # Split the insert_values into chunks of maximum size
        value_chunks = [insert_values[i:i+max_values_per_insert] for i in range(0, len(insert_values), max_values_per_insert)]

        # Execute the INSERT statements for each value chunk
        for values in value_chunks:
            placeholders = ', '.join(['((SELECT book_key FROM Book_DIM WHERE book_id = %s), '
                                    '(SELECT keyword_key FROM Keyword_DIM WHERE keyword_id = %s))'] * len(values))
            insert_sql = f"INSERT INTO Book_Keyword_FACT (book_key, keyword_key) VALUES {placeholders}"
            insert_values_flat = [value for row in values for value in row]
            cursor.execute(insert_sql, insert_values_flat)

    def output(self):
        filename = f'bookKeywordFactLoaded_v{self.version}.txt'
        return luigi.LocalTarget(filename)


# Load Review_FACT 
class LoadReviewFact(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return [TransformTask(version=self.version), LoadBookDim(version=self.version),LoadUserDim(version=self.version)]

    def run(self):
        with self.input()[0].open('r') as input_file:
            transformed_data = json.load(input_file)

        # Load the previously loaded data from file
        previous_data = load_previous_data(self.version)

        destination_conn = mysql.connector.connect(**target_db_config)
        destination_cursor = destination_conn.cursor()

        # Load data into Review_FACT table with new surrogate keys and foreign key mappings
        self.load_review_fact(destination_cursor, transformed_data['Review_FACT'], previous_data.get('Review_FACT', []))

        destination_conn.commit()  # Commit the changes
        destination_conn.close()  # Close the connection

        with self.output().open('w') as output_file:
            output_file.write(str(self.version))

    # Load data into Review_FACT table with new surrogate keys and foreign key mappings
    def load_review_fact(self, cursor, data, previous_data):
        previous_entries = set(tuple(entry) for entry in previous_data)
        insert_values = []

        for row in data:
            if tuple(row) not in previous_entries:
                user_id = row[0]
                book_id = row[1]
                rating = row[2]
                insert_values.append((user_id, book_id, rating))

        # Combine all the to be inserted values into one SQL statement
        if insert_values:
            placeholders = ', '.join(['((SELECT user_key FROM User_DIM WHERE user_id = %s), '
                                    '(SELECT book_key FROM Book_DIM WHERE book_id = %s), '
                                    '%s)'] * len(insert_values))
            insert_sql = f"INSERT INTO Review_FACT (user_key, book_key, rating) VALUES {placeholders}"
            insert_values_flat = [value for row in insert_values for value in row]
            cursor.execute(insert_sql, insert_values_flat)

    def output(self):
        filename = f'reviewFactLoaded_v{self.version}.txt'
        return luigi.LocalTarget(filename)


# Load Loan_FACT
class LoadLoanFact(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return [TransformTask(version=self.version), LoadBookDim(version=self.version),LoadUserDim(version=self.version)]

    def run(self):
        with self.input()[0].open('r') as input_file:
            transformed_data = json.load(input_file)

        # Load the previously loaded data from file
        previous_data = load_previous_data(self.version)

        destination_conn = mysql.connector.connect(**target_db_config)
        destination_cursor = destination_conn.cursor()

        # Load data into Review_FACT table with new surrogate keys and foreign key mappings
        self.load_loan_fact(destination_cursor, transformed_data['Loan_FACT'], previous_data.get('Loan_FACT', []))

        destination_conn.commit()  # Commit the changes
        destination_conn.close()  # Close the connection

        with self.output().open('w') as output_file:
            output_file.write(str(self.version))

    # Load data into Loan_FACT table with new surrogate keys and foreign key mappings
    def load_loan_fact(self, cursor, data, previous_data):
        previous_entries = {tuple(entry) for entry in previous_data}
        insert_values = []
        update_values = []

        for row in data:
            # If loan exists in previous_data, check if return_date has changed
            if tuple(row) in previous_entries:
                for prev_row in previous_data:
                    if tuple(prev_row) == tuple(row):
                        if prev_row['return_date'] != row['return_date']:
                            update_values.append((row['return_date'], row['loan_id']))
                        break
            # If it is a new loan, add it to the to be inserted list
            else:
                insert_values.append((
                    row['loan_id'],
                    row['loan_date'],
                    row['return_date'],
                    row['employee_id'],
                    row['book_id']
                ))

        # Insert new loans
        if insert_values:
            placeholders = ', '.join(['(%s, %s, %s, '
                                    '(SELECT user_key FROM User_DIM WHERE user_id = %s), '
                                    '(SELECT book_key FROM Book_DIM WHERE book_id = %s))'] * len(insert_values))
            insert_sql = f"INSERT INTO Loan_FACT (loan_id, loan_date, return_date, user_key, book_key) VALUES {placeholders}"
            insert_values_flat = [value for row in insert_values for value in row]
            cursor.execute(insert_sql, insert_values_flat)

        # Update edited loans
        if update_values:
            update_sql = "UPDATE Loan_FACT SET return_date = %s WHERE loan_id = %s"
            cursor.executemany(update_sql, update_values)

    def output(self):
        filename = f'loanFactLoaded_v{self.version}.txt'
        return luigi.LocalTarget(filename)