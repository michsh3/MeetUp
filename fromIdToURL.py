import psycopg2
import psycopg2.extras

#USED!!!!!


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
dict={}
with open('FinTech-Aviv_companies.txt',"r") as f:
   for line in f:
       print (line)
       cursor.execute("select url from company_data_flat where id = '%s';"%(line))
       for row in cursor:
            dict[line]=row[0]

       if 'str' in line:
           break

with open('FinTech-Aviv_companies_URL.txt', 'a+') as f:
        for comp in dict:
            f.write("%s\n" %dict[comp])