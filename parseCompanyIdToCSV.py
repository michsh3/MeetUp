import psycopg2
import psycopg2.extras
import csv

HOST="unomy-newbeta-pg10.cq3osgn0otff.us-east-1.rds.amazonaws.com"
DATABASE="unomy_data_flat"
USER="stats2018"
PASSWORD="stats2018"

conn = None

def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # Define our connection string
        conn_string = "host="+HOST+" dbname="+DATABASE+" user="+USER+" password="+PASSWORD

        # print the connection string we will use to connect
        print("Connecting to database\n	->%s" % (conn_string))

        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        print("Connected!\n")

        return conn


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

conn = connect()
cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

with open("FinTech-Aviv_companies.csv", "w") as csv_file:
    fieldnames = ['First_name', 'Last_name','Title','URL']
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(fieldnames)

    with open('FinTech-Aviv.txt', "r") as f:
        for line in f:
            for word in line.split('"'):
                name = word.split()
                print("name: ", name)
                if (len(name) == 2):
                    print(name)
                    first_name = name[0].replace("'", "")
                    last_name = name[1].replace("'", "")
                    cursor.execute(
                        "select pd.first_name,pd.last_name,pd.title,cd.url from person_data_flat as pd inner join company_data_flat as cd on pd.company_id=cd.id where pd.first_name = '%s' and pd.last_name='%s' and location_country= 'ISR';" % (
                        first_name, last_name)
                    )

                    csv_writer.writerows(cursor)


