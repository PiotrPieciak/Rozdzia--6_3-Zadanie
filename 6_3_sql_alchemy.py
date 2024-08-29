import csv
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float

def advanced_data_display(show_it_to_me):
    print(show_it_to_me)

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
    
#Zaznaczanie (select) wierszy z id < 4
    sel = stations.select().where(stations.c.id < 4)
    #print(conn.execute(sel).fetchall())
    advanced_data_display(conn.execute(sel).fetchall())

#Wykonanie update na tabeli stations, dla id == 4 zmieniamy name na Warszawa
    update_st = stations.update().where(stations.c.id == 4).values(name="Warszawa")
    conn.execute(update_st)

#Wykonanie delete dla id = 2
    delete_st = stations.delete().where(stations.c.id == 2)
    conn.execute(delete_st)

#Wydrukowanie końcowych komunikatów
    advanced_data_display(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())
    advanced_data_display("stations.db was created and all data from files clean_measure.csv and clean_stations.csv were included to database. First five rows from table 'stations' were dislpayed. Functions like select, update and delete were included. ")
