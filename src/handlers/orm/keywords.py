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
from .base import Serializable

Base = declarative_base()


class Keywords(Base, Serializable):
    __tablename__ = "keywords"
    __table_args__ = (
        PrimaryKeyConstraint("keyword"),
    )
    date_added = Column(Date)
    keyword = Column(String)

    @staticmethod
    def upsert(conn: sqlalchemy.engine.Engine, keywords_df):
        if not database_exists(conn.url):
            create_database(conn.url)

        try:
            Session = sessionmaker(bind=conn)
            session = Session()
            

            Keywords.__table__.create(bind=conn, checkfirst=True)

            if len(keywords_df) == 0:
                session.close()
                return

            
            df = keywords_df.dropna(subset=["keyword"])
            df = df.drop_duplicates(subset=["keyword"], keep="last")
            filename = "keywords.json"
            values_=df.to_json(filename,orient='records',lines=True,date_unit='s')
            filepath = os.path.abspath(filename)
           

            query="""
            CREATE OR REPLACE STAGE keyword_stage FILE_FORMAT=(TYPE='JSON');
            """
            insert_columns=['keyword','date_added']
            id_columns=['keyword']
            update_columns=['keyword','date_added']

    
            session.execute("alter session set timezone='UTC';")
            session.execute(f"put file://{filepath} @keyword_stage overwrite=true;")

            session.execute(f"""merge into keywords
						using (select {','.join([f'$1:{col} as {col}' for col in insert_columns])}
							from @keyword_stage/{filename}) t
						on ({' and '.join([f't.{col} = keywords.{col}' for col in id_columns])})
						when matched then
							update set {','.join([f'{col}=t.{col}' for col in update_columns])}
						when not matched then insert ({','.join(insert_columns)})
						values ({','.join([f't.{col}' for col in insert_columns])});""")


            session.commit()
        finally:
            if session:
                session.close()
