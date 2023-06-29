import luigi
import os
from flask import Flask
from loadTask import LoadKeywordDim, LoadBookKeywordFact, LoadLoanFact
from luigiServer import scheduler

# API server to make/get commands
app = Flask(__name__)

# Global version control variables
current_version = 0
previous_version = 0

# Main task to run the whole ETL process
class MainTask(luigi.Task):
    version = luigi.IntParameter(default=1)

    def requires(self):
        return [LoadKeywordDim(version=self.version), LoadBookKeywordFact(version=self.version), LoadLoanFact(version=self.version)]

    def run(self):
        global current_version, previous_version  # Access the global version control variables

        # Increment the version
        previous_version = current_version
        current_version += 1

        pass

# Function to run the Luigi task from an external api command
@app.route('/trigger_etl')
def trigger_etl():
    global current_version
    global previous_version
    previous_version = current_version
    current_version += 1

    # Delete logs for versions other than the current or previous versions
    files = os.listdir()
    for file in files:
            if (
                file != f"extracted_data_v{current_version}.json" and
                file != f"extracted_data_v{previous_version}.json" and
                file != f"transformed_data_v{current_version}.json" and
                file != f"transformed_data_v{previous_version}.json" and
                file != f"book_load_complete_v{current_version}.txt" and
                file != f"book_load_complete_v{previous_version}.txt" and
                file != f"user_load_complete_v{current_version}.txt" and
                file != f"user_load_complete_v{previous_version}.txt" and
                file != f"keyword_load_complete_v{current_version}.txt" and
                file != f"keyword_load_complete_v{previous_version}.txt" and
                file != f"bookKeywordFactLoaded_v{current_version}.txt" and
                file != f"bookKeywordFactLoaded_v{previous_version}.txt" and
                file != f"reviewFactLoaded_v{current_version}.txt" and
                file != f"reviewFactLoaded_v{previous_version}.txt" and
                file != f"loanFactLoaded_v{current_version}.txt" and
                file != f"loanFactLoaded_v{previous_version}.txt" and
                file != "main.py" and
                file != "luigiServer.py" and
                file != "extractTask.py" and
                file != "transformTask.py" and
                file != "loadTask.py" and
                file != "__pycache__"
            ):
                os.remove(file)

    luigi.build([MainTask(version=current_version)], workers=2, local_scheduler=scheduler)
    return f'ETL process triggered for version {current_version}.'

# Start the Luigi task runner
if __name__ == '__main__':
    app.run()