import csv
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float

#Funkcja do wyświetlania wyników 
def advanced_data_display(show_it_to_me):
    print(show_it_to_me)

#Funkcja wykonująca działanie na bazie danych
def action_on_db(param):
    conn.execute(param)

#Funkcja kopiująca dane z plików csv do bazy danych
def copy_csv_to_db(file_name):
    if file_name == "clean_stations.csv":
        ins = stations.insert()
        ins.compile().params
    else:
        ins = measures.insert()
        ins.compile().params

    with open(file_name) as csv_file:
        reader = csv.reader(csv_file)
        for line in reader:
            if line[0] == 'station':
                continue
            elif file_name == "clean_stations.csv":
                conn.execute(ins, {'station' : line[0],'latitude' : line[1],'longitude' : line[2],'elevation' : line[3],'name' : line[4],'country' : line[5],'state' : line[6]} )
            else:
                conn.execute(ins, {'station' : line[0],'date' : line[1],'precip' : line[2],'tobs' : line[3]} )

#____________PROGRAM GŁÓWNY__________________________
if __name__ == "__main__":

#Utworzenie/otwarcie pliku stations.db
    engine = create_engine('sqlite:///stations.db')
    meta = MetaData()

#definiujemy tabele stations
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

#definiujemy tabele measures
    measures = Table(
    'measures', meta,
    Column('id', Integer, primary_key=True),
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
    )

#Tworzymy tabele w bazie danych
    meta.create_all(engine)

#Pod zmienną conn podłączmy naszą bazę danych 
    conn = engine.connect()

#kopiowanie danych z pliku clean_stations.csv do tabeli stations w stations.db
    copy_csv_to_db("clean_stations.csv")

#kopiowanie danych z pliku clean_measure.csv do tabeli measures w stations.db
#________________UWAGA w pliku jest ok 19500 wpisów - to może zająć kilka minut_____________
    copy_csv_to_db("clean_measure.csv")

#Zaznaczanie (select) wierszy z id < 4
    sel = stations.select().where(stations.c.id < 4)
    advanced_data_display(conn.execute(sel).fetchall())

#Wykonanie update na tabeli stations, dla id == 4 zmieniamy name na Warszawa
    update_st = stations.update().where(stations.c.id == 4).values(name="Warszawa")
    action_on_db(update_st)
    
#Wykonanie delete dla id = 2
    delete_st = stations.delete().where(stations.c.id == 2)
    action_on_db(delete_st)

#Wydrukowanie końcowych komunikatów
    advanced_data_display(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())
    advanced_data_display("stations.db was created and all data from files clean_measure.csv and clean_stations.csv were included to database. First five rows from table 'stations' were dislpayed. Functions like select, update and delete were included. ")