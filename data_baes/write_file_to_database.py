import pandas as pd
import pyodbc
import psycopg2
from pathlib import Path
import pprint


class DatabaseConnection:

    def __init__(self):


        try:
            self.connection = psycopg2.connect(
                "dbname = 'sales' user = 'postgres' host = 'localhost' password = 'admin' port = '5432'")
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
            #This is used to connect to the database
        except:
            pprint('cannot connect to database')

    def create_table(self):
        self.cursor.execute('CREATE TABLE train(passengerid integer NOT NULL primary key, survived character varying(300), pclass character varying(300), name character varying(300), sex character varying(300),\
        age character varying(300), sibsp character varying(300), parch character varying(300), ticket character varying(300), fare character varying(300), cabin character varying(300),\
        embarked character varying(300))')


    def clean_data(self):
        base_path = Path(__file__).parent
        file_path = (base_path / "../resources/train.csv").resolve()

        data = pd.read_csv(file_path)
        df = pd.DataFrame(data,columns=['PassengerId', 'Survived', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch','Ticket', 'Fare', 'Cabin', 'Embarked'])
        mean = df["Age"].mean()
        df['Age'] = df["Age"].fillna(mean, inplace=False)
        df['Embarked'] = df["Embarked"].fillna("null", inplace=False)
        df['Cabin'] = df["Cabin"].fillna("null", inplace=False)

        df["Name"] = df["Name"].map(lambda x: x.replace("'", ""))

        #due to the errors  encountered when reading to the database i cleaned the data; replaced empty values and syntaxerrror

        self.insert_new_record(df)




    def insert_new_record(self, df):

        for index,row in df.iterrows():
            insert_command = "INSERT INTO train (PassengerId, Survived, Pclass, Name, Sex, Age,SibSp, Parch,ticket, fare, cabin, embarked)\
             VALUES (%s,'%s','%s','%s','%s',%s,'%s','%s','%s','%s','%s','%s')" % (row['PassengerId'], row['Survived'], row['Pclass'], row["Name"], row['Sex'], row['Age'],row['SibSp'], row['Parch'], row['Ticket'], row['Fare'], row['Cabin'], row['Embarked'])
            self.cursor.execute(insert_command)


    def drop_table(self):
        drop_table_command = "DROP TABLE train"
        self.cursor.execute(drop_table_command)




if __name__ =='__main__':
    database_connection = DatabaseConnection()
    #database_connection.create_table()
    database_connection.clean_data()
    #database_connection.insert_new_record()
    #database_connection.drop_table()

