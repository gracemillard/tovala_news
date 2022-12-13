import ast
import time
from collections import OrderedDict
from datetime import date, datetime, timedelta
from typing import List
import requests
import time
import numpy as np
import pandas as pd
from newsapi import NewsApiClient
from handlers.common.secrets import get_news_api_key
from handlers.common.db import get_db_engine
from handlers.common.logging import get_logger
from handlers.orm.news import KeywordNews




class NewsAPIProcessor():
    LOCAL_PATH: str = ""
    S3_ROOT_PATH = "s3://tovala-de-coding-challenge/Grace-Millard"


    def __init__(
        self,
        keywords_list: list ,
        insertion_type: str,
        date_start: datetime,
        date_end: datetime,

    ):
        self._logger = get_logger()
        self.insertion_method=insertion_type
        self.date_start=date_start 
        self.date_end=date_end
        self.keywords_list=keywords_list  

        
        #TODO : make a cache class and then instantiate cache class

    def process(self):
        for i in range(len(self.keywords_list)):
        
            word= self.keywords_list[i]

            try:

                df_for_keyword_daterange = self._news_get_apiresult(word)
               # self._logger.info(f"df looks like {df_for_keyword_daterange}")
                self._write_file_toDB(df_for_keyword_daterange)

            except Exception as e:
                self._logger.error(f"issue with keyword {word} in range {self.date_end} to {self.date_start}: {e}")

        pass

         #IDEA
        #I will create a status for the chunk in a new table and say chunk is processing - to do
        #for each keyword I will go thru every date in daterange , 
        # I will collect all the data for every record and add cols for keyword and date
        # For each keyword I will write the data to s3 as json as an external stage then use snowflake merge to do a bulk upsert to a table in snowflake
        #when im out of keywords I will change the chunk processing status table to reflect that its done processing
        #then I will check to see if all the chunks are done processing
        #if they are I will trigger the next handler (I should prob write a process to materialize a view of aggrgations off the data coming in) - to do 


    



    def _news_get_apiresult(self, word):
        def process_page(all_articles):
            p_list=[]
            self._logger.info("process page starting")
            self._logger.info(len(all_articles['articles']))
            for g in range(len(all_articles['articles'])):
                source=all_articles['articles'][g]['source']['name'] or "NA"
                source_id=all_articles['articles'][g]['source']['id'] or "NA"
                author=all_articles['articles'][g]['author'] or "NA"
                title=all_articles['articles'][g]['title'] or "NA"
                description=all_articles['articles'][g]['description'] or "NA"
                url=all_articles['articles'][g]['url'] or "NA"
                url_img=all_articles['articles'][g]['urlToImage'] or "NA"
                date=all_articles['articles'][g]['publishedAt'] or "NA"
                content=all_articles['articles'][g]['content'] or "NA"
                p_list.append([source,source_id,author,title,description,url,url_img, date, content])
            df=pd.DataFrame(p_list, columns=['source', 'source_id','author','title','description','url','url_img','date_published','content'])
            return df

        newsapi = NewsApiClient(api_key=get_news_api_key())
        all_articles = newsapi.get_everything(q=f'{word}',from_param=self.date_start,to=self.date_end,language='en',sort_by='relevancy',page=1)

        if all_articles['status'] != "ok":
            raise ValueError("something wrong with request params")
        else:
            inital_df=process_page(all_articles) #breaks json down and reads into df
            if int(all_articles['totalResults']) >100:
                total_pages=int(int(all_articles['totalResults']) / 100)
                self._logger.info(f"total pages are {total_pages}")
                for i in range(2,total_pages+1):
                    self._logger.info(f"on page {i}")
                    try:
                        json= newsapi.get_everything(q=word,from_param=self.date_start,to=self.date_end,language='en',sort_by='relevancy',page=i)
                    except:
                        self._logger.error("something's up with the api call")
                    next_df=process_page(json)
                    self._logger.info(f"len of next df : {len(next_df)}")
                    inital_df=pd.concat((inital_df, next_df), axis = 0)
                    self._logger.info(f"here is len of new df {len(inital_df)}")
            inital_df['keyword']=word
            return inital_df
            
        

    def _write_file_toDB(self, df):

        if len(df)>0:

            engine = get_db_engine(db_secret_name="DbSecretName")
            format=f"{datetime.now():%Y-%m-%d}"
            df['inserted_to_db_at']=  format
            df['updated_at']=  format


            self._logger.info("about to write to table: keyword_news_raw")
            start= time.time()
            """
            This is basically an upset, snowflake dosnt really call upserts 'upserts' tho
            """
            KeywordNews.upsert(engine,df)
            self._logger.info(f"wrote to table: keyword_news_raw, it took {time.time()-start} seconds.")

            

        else:
                self._logger.info("empty df")
                pass
   


        

  


    

   