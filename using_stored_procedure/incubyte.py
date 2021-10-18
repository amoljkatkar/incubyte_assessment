import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


def createTable(engine, tablename):
    try:
        with engine.connect() as con:
            con.execute("call createCountryTable(\"" + tablename + "\")")
    except Exception as e:
        print(e)


def createTables(engine, inspector, db, distinct_countries, existing_tables):
    for table in distinct_countries:
        if table in existing_tables:
            # using append can cause primary key error
            # thus its a good idea to drop tables first
            print(table + " already exists")
    
        print("trying to create " + table)
        try:
            createTable(engine, table)
            print("Created")
        except Exception as e:
            print()
            


def getTables(engine):
    inspector = inspect(engine)
    all_tables = [tbl for tbl in inspector.get_table_names(schema=db)]
    return all_tables, inspector


df = pd.read_csv('patients.txt', sep="|", header=None)

is_header = df.iloc[0, 1]

if is_header == 'H':
    df.drop(df.head(1).index, inplace=True)


df.columns = ["N","D",
              "customerName", "customerID",
              "customerOpenDate", "lastConsultedDate",
              "vaccinationType", "doctorConsulted",
              "state", "country","dateofBirth",
              "activeCustomer"]

del df['D']
del df['N']

#df['customerID'] = df['customerID'].apply(np.int64)
df.set_index('customerID')

try:
    df['customerOpenDate'] = pd.to_datetime(
        df['customerOpenDate'], format='%Y%m%d')
    df['lastConsultedDate'] = pd.to_datetime(
        df['lastConsultedDate'], format='%Y%m%d')
    df['dateofBirth'] = pd.to_datetime(
        df['dateofBirth'], format='%d%m%Y')
except Exception as e:
    print(e)

print(df)

df['country'] = df['country'].str.lower()
distinct_countries = df['country'].drop_duplicates()

db = "incubyte"
try:
    engine = create_engine(
        "mysql+mysqlconnector://root:12345@localhost:3306/" + db)
    engine.connect()
    print("Database Connected")
except Exception as e:
    print(e)

existing_tables, inspector = getTables(engine)
createTables(engine, inspector, db, distinct_countries, existing_tables)
existing_tables, inspector = getTables(engine)

for country in distinct_countries:
    my_filt = (df['country'] == country)
    try:
        print("Inserting Records in " + country)
        if country in existing_tables:
            df[my_filt].to_sql(
                name=country, con=engine,
                if_exists='replace', index=False)
    except Exception as e:
        print(e)
