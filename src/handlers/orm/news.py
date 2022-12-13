from typing import List
import pandas as pd
import sqlalchemy
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import Column, String, Date, DECIMAL, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from datetime import datetime
from snowflake.sqlalchemy import MergeInto
import os
from uuid import uuid4
from .base import Serializable

Base = declarative_base()


class KeywordNews(Base, Serializable):
    __tablename__ = "keyword_news_raw"
    __table_args__ = (
        PrimaryKeyConstraint("source","title","keyword"),
    )
    source = Column(String)
    source_id = Column(String)
    author= Column(String)
    title = Column(String)
    description = Column(String)
    url = Column(String)
    url_img= Column(String)
    date_published = Column(Date)
    content=Column(String)
    keyword=Column(String)
    inserted_to_db_at=Column(Date)
    updated_at=Column(Date)

    ['source','source_id','author','title','description','url','url_img','date_published','content','keyword','inserted_to_db_at','updated_at']


    @staticmethod
    def upsert(conn: sqlalchemy.engine.Engine, df):
        if not database_exists(conn.url):
            create_database(conn.url)

        try:
            Session = sessionmaker(bind=conn)
            session = Session()
            
            KeywordNews.__table__.create(bind=conn, checkfirst=True)

            if len(df) == 0:
                session.close()
                return
            df = df.dropna(subset=["source","title","keyword"])
            df = df.drop_duplicates(subset=["source","title","keyword"], keep="last")
            rand_id = str(uuid4())
            """
            this rand_id is so i wont accidently overwrite things when i dont mean to if some parelel work is happening
            """
            filename = f"keyword_news_{rand_id}.json"
            values_=df.to_json(filename,orient='records',lines=True,date_unit='s')
            filepath = os.path.abspath(filename)
           
            query="""
            CREATE OR REPLACE STAGE keyword_news_stage FILE_FORMAT=(TYPE='JSON');
            """
            insert_columns= ['source','source_id','author','title','description','url','url_img','date_published','content','keyword','inserted_to_db_at','updated_at']
            id_columns=["source","title","keyword"]
            update_columns= ['source_id','author','description','url','url_img','date_published','content','updated_at']

            session.execute(query)
            session.execute("alter session set timezone='UTC';")
            session.execute(f"put file://{filepath} @keyword_news_stage overwrite=true;")

            session.execute(f"""merge into keyword_news_raw
						using (select {','.join([f'$1:{col} as {col}' for col in insert_columns])}
							from @keyword_news_stage/{filename}) t
						on ({' and '.join([f't.{col} = keyword_news_raw.{col}' for col in id_columns])})
						when matched then
							update set {','.join([f'{col}=t.{col}' for col in update_columns])}
						when not matched then insert ({','.join(insert_columns)})
						values ({','.join([f't.{col}' for col in insert_columns])});""")

            session.commit()
        finally:
            if session:
                session.close()
        
    @staticmethod
    def full_refresh(conn: sqlalchemy.engine.Engine, df):
        if not database_exists(conn.url):
            create_database(conn.url)

        try:
            Session = sessionmaker(bind=conn)
            session = Session()
            
            KeywordNews.__table__.create(bind=conn, checkfirst=True)

            if len(df) == 0:
                session.close()
                return
            df = df.dropna(subset=["source","title","keyword"])
            df = df.drop_duplicates(subset=["source","title","keyword"], keep="last")
            rand_id = str(uuid4())
            """
            this rand_id is so i wont accidently overwrite things when i dont mean to if some parelel work is happening
            """
            filename = f"keyword_news_{rand_id}.json"
            values_=df.to_json(filename,orient='records',lines=True,date_unit='s')
            filepath = os.path.abspath(filename)
           
            query="""
            CREATE OR REPLACE STAGE keyword_news_stage FILE_FORMAT=(TYPE='JSON');
            """
            
            """"
            logic here
            
            """

            session.commit()
        finally:
            if session:
                session.close()

    @staticmethod
    def refresh_range(conn: sqlalchemy.engine.Engine, df):
        if not database_exists(conn.url):
            create_database(conn.url)

        try:
            Session = sessionmaker(bind=conn)
            session = Session()
            
            KeywordNews.__table__.create(bind=conn, checkfirst=True)

            if len(df) == 0:
                session.close()
                return
            df = df.dropna(subset=["source","title","keyword"])
            df = df.drop_duplicates(subset=["source","title","keyword"], keep="last")
            rand_id = str(uuid4())
            """
            this rand_id is so i wont accidently overwrite things when i dont mean to if some parelel work is happening
            """
            filename = f"keyword_news_{rand_id}.json"
            values_=df.to_json(filename,orient='records',lines=True,date_unit='s')
            filepath = os.path.abspath(filename)
           
            query="""
            CREATE OR REPLACE STAGE keyword_news_stage FILE_FORMAT=(TYPE='JSON');
            """
            
            """"
            logic here
            
            """

            session.commit()
        finally:
            if session:
                session.close()

    

