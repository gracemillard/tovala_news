# tovala_news

this is a template I use for READMEs<br />
a few files I used are templates I recycled from other projects (like the .gitnore file at the root and the requirements.test files)
  


## Initialize Environment

```shell
conda create -n takehome python=3.8
conda activate takehome
cd src
pip install -r requirements-dev.txt -i https://pypi.org/simple
pip install -r handlers/requirements.txt -i https://pypi.org/simple
```

## About the code

Part of the ask was to stage the data in s3 before bringing it into snowflake,
I wasnt provided tovala s3 credentials and I really didnt want to use my personal aws account so I came up with a work around using snowflake staging , I hope this is ok. 
..... If I were to use it like this on the job, I would add some kind of retirement process to purge the staging area every once in awhile+

I designed this with flexibility and scalability in mind.
- Allowing every keyword used to be tracked and reused (via the keywords table)

- Allowing the user to delete all of the sored keywords if they want to start from scratch or ignore the stored keywords and just run their custom list (via the self titled comand line arguments)

- I also give the user several options for how the data can be injested. There is an append option (actually an upsert) to bring data in and make updates to existing data (if nessesary). A full-refresh option, if you want to get rid of everything in the old table and replace it with new data fresh from the api. And there is a refresh-range which is when you want to completly refresh the data within a certain daterange and keyword range. These last two options are good for when the data provider (in this case news-api) messes up and you need to backfill the database.

-I also have the keywords getting chunked into smaller batches before they get processed, with the idea that you could horozontally scale this process by dropping the chunks into a q (like aws sqs) and have lambdas running the news_processor consuming those chunks in parellel. This would speed up the process quite a bit (I have more notes in the code abput how this would work)

## Considerations

- I would like to use some kind of caching flow to make sure I dont make unnessesary api calls (for my 'append' insertion option).
  I'm not imagining a real cache but rather a making query to grab all the distinct keywords and dates in the keyword_news_raw table for the date range and seeing if the range im about to fetch is already covered or can be shortened because it is partially accounted for in the db

- As another effort to make sure I dont blow thru all the api credits at once, I would like to do a request for each keyword thats going to be chunked (before the chunking), to calculate the total number of api_calls that will need to be made, then if its over the daily limit, alert the user before continuing with processing

- I also might need to change my code to make sure I dont lose any data when the api limit is hit, I dont remember if I handled that. 

- If I do parellelize these processes, I will have to keep track of all of their processing status' because I would like to run a process to create/update some tables with aggregate measures (or maybe refresh a materialized view), and I will need to create an event to kick off those downstream processes once I know all the keywords have been processed


## The Deployment Plan

make a request to trigger lambda 
or set up api gateway to receive api call and forward to lambda 
(depends on who the user is, and how the user wants to use the service)
run news_handler.py in a lambda (triggered by api gateway or request) and register each batches chunks in a status table to indicate that they exist and are processing, handler blobs get fed to sqs q, lambdas running news_processor.py consume json blobs with the dates and keyword lists of the q when they have completed the write_to_db function the status of that blob/chunk will get updated to complete then a check will be made (in the processor file) to see if all of the chunks from that batch are done if the check is succesful then the catch number will be sent to another lambda which will run a diffent file (that I havnt written yet) containing the logic to upsert entries into a table of aggrgate measures. 

- I would configure this with a cdk script, should probally dockerize too 


## How to run test

-put your credentials into the secrets.json in the resources dir

```shell
cd src
python3 -m handlers.tests.e2e_test
```

## How to run the cli app

-put your credentials into the secrets.json in the resources dir

```shell
cd src
python3 -m handlers.news_handler
```

## Aditional Consideraions

-- How often will this be used (daily? weekly? adhoc?) ... shall it be scheduled or triggered<br />

-- Who will use this? Are they capable of triggering it themselves, do they need a ui? <br />

-- What questions is this data supposed to answer? Whats the relevant time frame? <br />

-- How many keywords do they need to track <br />

-- Whats the avg # of news articles get written per day (whats the volume of data coming in) <br/>



## TO DOs
- I need to write more tests (e2e and unit) <br />
- I need to hide the secrets, or make the secrets an arg passed in thru the command line <br />
- figure out runtime complexity <br />
- precalculate how many querries it will take to exceed api limit for each keyword (make inital querry for each keyword and date range and divide total articles by 100 to get number of querries, make a counter, if counter exceeded then raise error record stats on volume for each keyword, return to user and tell user to exclude some keywords or limit pages collected (introduce new cmd line args to exclude keywords or set limit))
