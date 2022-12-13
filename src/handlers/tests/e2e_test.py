import json
from datetime import datetime, timedelta
from handlers.common.db import get_db_engine
from handlers.common.logging import get_logger
from handlers.processors.news_processor import NewsAPIProcessor
from handlers.orm.keywords import Keywords
import pandas as pd
import sys
import ast



def main(d):
    logger = get_logger()

    logger.info("STARTING PROCESSING")

    sample_input={"start_date":"2022-12-10",
    "end_date":"2022-12-12",
    "new_keywords":["meal-kit"],
    "drop_all_old_keywords":False,
    "skip_keywords_not_input_in_this_form":False,
    "insertion_type":"full-refresh"}

    logger.info(f"input looks like : {d}")

    metadata=d

    """
    First I'm checking to make sure the paramaters are valid, 
    assigning some default params for missing stuff
    """
    try:
        datetime_start = datetime.strptime(metadata.get("start_date","2022-12-10"), "%Y-%m-%d")
        datetime_end = datetime.strptime(metadata.get("end_date","2022-12-12"), "%Y-%m-%d")
    except:
        logger.error("dates are formatted incorrectly, try something like this <2022-12-10> (year-month-day)")

    free_api_backfill_limit=(datetime.today() - timedelta(days=30))

    if free_api_backfill_limit>datetime_start:
        raise ValueError("the free api only allows data from 30 days ago max, change date start")

    if datetime_end > datetime.today():
        raise ValueError("The date end was set to a date in the future, change it to todays date or a past date")
    
    keywords = metadata.get("new_keywords","[None]")
    keywords=ast.literal_eval(keywords)

    insertion_type = str(metadata.get("insertion_type",'append'))

    if insertion_type not in ["full-refresh", "append", "refresh-range"]:
        raise ValueError("These are the only insertion methods supported (full-refresh, append, refresh-range), pick one or None (default is append)")
    else:
        prepare_for_insertion_by_method(insertion_type, datetime_start,datetime_end,keywords)

    skip_keywords= metadata.get("skip_keywords_not_input_in_this_form",False)
    drop_old_keywords=metadata.get("drop_all_old_keywords",False)

    """
    I am setting it up as chunks so processing could run in parellel if the process ever needed to be sped up
    """
    add_new_keywords_to_db(keywords, drop_old_keywords)
    logger.info("wrote any new keywords to the db")

    keyword_chunks=create_keyword_chunks(skip_keywords,keywords)

    for i in keyword_chunks:
    
        processor = NewsAPIProcessor(i, insertion_type, datetime_start, datetime_end )
        processor.process()


def add_new_keywords_to_db(keywords,drop_param):
    if drop_param:
        query="""delete from keywords"""
        try:
            engine = get_db_engine()
            con=engine.connect()
            con.execute(query)
            
        finally:
            if engine:
                engine.dispose()


    if isinstance(keywords, list):
        if len(keywords)>0:
            engine = get_db_engine(db_secret_name="DbSecretName")
            keywords_df=pd.DataFrame(keywords,columns=['keyword'])
            format=f"{datetime.now():%Y-%m-%d}"
            keywords_df['date_added']=  format
            """
            This is basically an upset, snowflake dosnt really call upserts 'upserts' tho
            """
            Keywords.upsert(engine,keywords_df)
        else:
            pass
    else:
        raise ValueError("not a list")


def get_all_keywords_from_db():

    query="""
    select keyword 
    from keywords;
    """
    try:
        engine = get_db_engine()
        df = pd.read_sql(query, engine)
    finally:
        if engine:
            engine.dispose()

    return list(df['keyword'])


def create_keyword_chunks(skip_param,keywords):
    if skip_param:
        all_keywords=keywords
    else:
        all_keywords=get_all_keywords_from_db()

    chunk_size=3
    chunks = [all_keywords[i : i + chunk_size] for i in range(0, len(all_keywords), chunk_size)]

    return chunks


def prepare_for_insertion_by_method(method, datetime_start,datetime_end,keywords):

    if method == "append":
        pass
    else:

        if method == 'full-refresh':
            query="""delete from keyword_news_raw"""

        elif method == "refresh-range":
            query=f"""delete from keyword_news_raw where (date_published between '{datetime_start}' and '{datetime_end}') and (keyword in {tuple(keywords)}));"""

        try:
            engine = get_db_engine()
            con=engine.connect()
            con.execute(query)
       
            
        finally:
            if engine:
                engine.dispose()
        pass


if __name__ =="__main__":

    print("This is the app to process news-api data into snowflake")
    print("please fill out the following")
    print("You can just hit <ENTER> if you dont know what you want there are default values")
    trick_question= input("Do you understand, the instructions printed abouve? ")
    start_date = input("Enter start date (like: year-month-day) ")
    end_date = input("Enter end date ")
    new_keywords=input("Enter any new keywords as a list (example: ['apple','orange'])  ")
    drop_all_old_keywords=input("Do you want to drop the keywords currently in the db (options are: True, False)? ")
    skip_keywords_not_input_in_this_form=input("Do you want to skip over keywords currently in the db and only use your list (options are: True, False)? ")
    insertion_type=input("How do you want to insert your data (options are: append, full-refresh, range-refresh)? ")
    
    conv = lambda i : i or False

    d={}
    if conv(start_date):
        d["start_date"]=start_date
    if conv(end_date):
        d["end_date"]=end_date
    if conv(new_keywords):
        d["new_keywords"]=new_keywords
    if conv(drop_all_old_keywords):
        d["drop_all_old_keywords"]=drop_all_old_keywords
    if conv(skip_keywords_not_input_in_this_form):
        d["skip_keywords_not_input_in_this_form"]=skip_keywords_not_input_in_this_form
    if conv(insertion_type):
        d["insertion_type"]=insertion_type

    print(d)
    main(d)
