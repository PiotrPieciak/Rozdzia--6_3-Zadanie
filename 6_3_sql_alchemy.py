import csv
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, Float

#____________PROGRAM GŁÓWNY__________________________
if __name__ == "__main__":

#Utworzenie/otwarcie pliku stations.db
    engine = create_engine('sqlite:///stations.db')
    meta = MetaData()

#Utworzenie tabeli stations
    stations = Table(
    'stations', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String),
    )

#Utworzenie tabeli measures
    measures = Table(
    'measures', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
    )

#kopiowanie danych z pliku clean_stations.csv do tabeli stations w stations.db
    ins = stations.insert()
    ins.compile().params
    conn = engine.connect()

    with open("clean_stations.csv") as csv_file1:
        reader = csv.reader(csv_file1)
        for line in reader:
            if line[0] == 'station':
                continue
            conn.execute(ins, {'station' : line[0],'latitude' : line[1],'longitude' : line[2],'elevation' : line[3],'name' : line[4],'country' : line[5],'state' : line[6]} )

#kopiowanie danych z pliku clean_measure.csv do tabeli measures w stations.db
#________________UWAGA w pliku jest ok 19500 wpisów - to może zająć kilka minut_____________
    ins = measures.insert()
    ins.compile().params

    print("Please wait, it can takes few minutes...")
    with open("clean_measure.csv") as csv_file2:
        reader = csv.reader(csv_file2)
        for line in reader:
            if line[0] == 'station':
                continue
            conn.execute(ins, {'station' : line[0],'date' : line[1],'precip' : line[2],'tobs' : line[3]} )

#Wydrukowanie końcowych komunikatów
    print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())
    print("stations.db was created and all data from files clean_measure.csv and clean_stations.csv were included to database. First five rows from table 'stations' were dislpayed")
