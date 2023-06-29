import luigi
import json
from extractTask import ExtractTask

class TransformTask(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return ExtractTask(version=self.version)

    def run(self):

        # Load the extracted data
        with self.input().open('r') as input_file:
            extracted_data = json.load(input_file)

        # Directly pass on the data that doesn't need to be transformed
        userDim = extracted_data["employee"]
        bookDim = extracted_data["book"]
        keywordDim = extracted_data["keyword"]
        bookKeywordFact = extracted_data["keyword_books"]
        reviewFact = extracted_data["review"]
        
        # Define variables for data that needs to be transformed
        loans = extracted_data["loan"]
        copies = extracted_data["copy"]

        # Helper variable for the mapping from copy to book to eliminate copies in the target db
        copy_to_book = {}
        for loan in loans:
            copy_id = loan[3]
            for copy in copies:
                if(copy_id == copy[0]):
                    book_id = copy[1]
                    copy_to_book[copy_id] = book_id

        # Create loanFact by combining loan and book information
        loanFact = []
        for loan in loans:

            loan_fact_entry = {
                'loan_id': loan[0],
                'loan_date': loan[1],
                'return_date': loan[2],
                'book_id': copy_to_book[loan[3]],
                'employee_id': loan[4]
            }
            loanFact.append(loan_fact_entry)

        # Store transformed data in a dictionary
        transformed_data = {
            'User_DIM': userDim,
            'Book_DIM': bookDim,
            'Keyword_DIM': keywordDim,
            'Book_Keyword_FACT': bookKeywordFact,
            'Review_FACT' : reviewFact,
            'Loan_FACT': loanFact
        }

        # Save the extracted data as a JSON file
        with self.output().open('w') as output_file:
            json.dump(transformed_data, output_file)

    def output(self):
        filename = f'transformed_data_v{self.version}.json'
        return luigi.LocalTarget(filename)