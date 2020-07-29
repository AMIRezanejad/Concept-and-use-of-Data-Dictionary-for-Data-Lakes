import mysql.connector
import mysql
import numpy as np

def fetch_db():
    rd_conn_result = mysql.connector.connect( # connecting to local host
        host= "127.0.0.1",
        user="root",
        passwd="2222",
        port=3308)                            # relational database  port
    rdcursur = rd_conn_result.cursor()
    rdcursur.execute("SHOW DATABASES")
    f_schema=list(rdcursur.fetchall())
    num_rows=int(rdcursur.rowcount)
    x=map(list,list(f_schema))# change the type
    x=sum(x,[]) #flaten list
    print(x)
    return(x)
def fetch_table(f_schema):                    # fetching the list of table belong to DB
    c_schema=0
    db=[]
    f_db=dict()
    for sch in f_schema:
        rd_conn_result = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            passwd="2222",
            port=3308,
            database=sch)
        rdcursur = rd_conn_result.cursor()
        rd_db = rdcursur.execute("SHOW TABLES")      #fetching tabels name and put into list
        db=list(rdcursur.fetchall())
        x = map(list, list(db))                      # change the type
        x = sum(x, [])                               # flatten list
        f_db[sch]=x
    print(f_db)
    return(f_db)
def db_request(db_names):
    for index in range(len(db_names)):
        print("{0}-{1}".format(index,db_names[index]))
    db_names=input("please choose the number of Database :")
                                                    # try catch needs for correctness of table index
    db_names =db_names.split()
    l_db_names=[]
    for i in db_names:
       l_db_names.append(int(i))
    print(l_db_names)
    return(l_db_names)
def table_request(ind_schema,schema_name,table_names):
    r_db_t = dict()
    for index in ind_schema:
                                                     #the name of Table
        key=schema_name[index]
        print("{0}-{1}\n".format(index,key))
                                                     #show the table of Database
        value=table_names[schema_name[index]]
        print("{}\n".format(value))
        table_num=input("please choose the number of Tables : ")
        table_num=table_num.split()
        x = map(list, list(table_num))                # change the type
        x = sum(x, [])
                                                      #build new dictionary of List with the name of database and tables numbers
        list1 = []
        for i in x:
            list1.append(value[int(i)])
        r_db_t[key]=list1
    print(r_db_t)
    return(r_db_t)

f_schema=fetch_db()
f_dbase=fetch_table(f_schema)
                                                      #request indices of the Database name in the Dictionary
r_db_indices=db_request(f_schema)
                                                      #recieve the the requested Table
end_table_re=table_request(r_db_indices,f_schema,f_dbase)
rd_conn_result = mysql.connector.connect(             #connectiong to the Data Dictionary server
        host="127.0.0.1",
        user="root",
        passwd="2222",
        port=3308)
rdcursur = rd_conn_result.cursor()                             # def build the DD
for key,values in end_table_re.items():
    rdcursur.execute("CREATE DATABASE DD_RD_{}".format(key))   # create database in data dictionary
    rd_conn_result = mysql.connector.connect(                  # connect to the new created database
        host="127.0.0.1",
        user="root",
        passwd="2222",
        port=3308,
        database="DD_RD_{}".format(key))
    rdcursur = rd_conn_result.cursor()
    for v in values:                                           # create data dictionary of table in form of table
        rdcursur.execute("CREATE TABLE dd_rd_%s SELECT table_schema,table_name,column_name,column_type "
                         "FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='%s' AND TABLE_SCHEMA='%s'"%(v,v,key))
print("done")


