from os import walk
import pandas as pd
import json
# from sqlalchemy import create_engine
import sqlalchemy as sa

DIR_PATH = "C:/Users/029338502/Desktop/Data_Download_Files"


class DatabaseHandler:

    def __init__(self):
        self.df = pd.DataFrame()
        self.engine = sa.create_engine('postgresql+psycopg2://postgres:postgres@localhost/postgres')
        # self.engine = sa.engine.URL.create(
        #     "postgresql+psycopg2",
        #     username="postgres",
        #     password="Nielsen@1234",
        #     host="localhost,5432",
        #     database="postgres"
        # )

        self.connection = self.engine.connect()

        # self.engine = create_engine("postgresql+psycopg2",
        #                             username="postgres",
        #                             password="Nielsen@1234",
        #                             host="localhost,5432",
        #                             database="postgres")

    def insert_data_to_df(self, file):
        data = json.loads(file.read())
        for i in range(len(data['hits']['hits'])):
            self.df = self.df.append(pd.json_normalize(data['hits']['hits'][i]['_source']))

    def insert_data_to_db(self):
        filenames = next(walk(DIR_PATH), (None, None, []))[2]  # [] if no file
        for name in filenames:
            file = open(DIR_PATH + "/" + name, "r")
            self.insert_data_to_df(file)
        self.df.to_sql(name='consumer_complaints', con=self.connection, if_exists='append')


database_handler = DatabaseHandler()
database_handler.insert_data_to_db()
# df_temp = database_handler.df
# print(df_temp['date_received'])
