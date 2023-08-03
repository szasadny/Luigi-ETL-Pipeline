# Luigi ETL pipeline for a library recommender system

This pipeline extracts the relevant data from the source library database and then transforms and loads it into the target database used by a recommender system. Using version control it keeps track of which data is already loaded for increased efficiency. It's also able to alter the relevant data that was changed in the source database by comparing it to the previous version.


Run luigiServer.py to run the monitoring server.


Run main.py to run the script. You can then externally trigger a trigger for the pipeline by accessing the API at {HOST}/trigger_etl.


### Requirement design pattern
Luigi works by the requirement design pattern where a task won't run unless it's dependencies are completed. This is communicated by writing an output file at the end of the previous task. Below is the dependency pattern for this particular pipeline. By splitting the load into multiple tasks, we are able to have multiple workers work parallel at the same time.

![ETL drawio (1)](https://github.com/szasadny/Luigi-ETL-Pipeline/assets/23632768/6ee9bb2f-6fb4-4de1-a2c9-90cf0455a50b)


## Source database

![ERD Bibliotheek Database drawio](https://github.com/szasadny/Luigi-ETL-Pipeline/assets/23632768/f3551e9f-262a-4ba8-b281-984de666bd86)


## Target database 

![ERD Recommender applicatie drawio](https://github.com/szasadny/Luigi-ETL-Pipeline/assets/23632768/15f045bf-391c-48c5-a152-9fe9f976ba59)
