import pymongo
import json
from pymongo import MongoClient
from bson import json_util
import pprint
import mysql.connector
import mysql
import re

# recursive parse the Json database
def pars_doc(key,schema_str,ch_list):
    v=""
    exist = False
    if (isinstance(schema_str,dict)):
        # when there is not any dictionary creat Dictionarydeep.append(key)# check when there is not such a dictionary build it
        path_d = path(key, ch_list)
        for k, value in schema_str.items():
            if (isinstance(value,(list,dict))):
                ch_list = False
                pars_doc(k, value,ch_list)
            else:
                if schema_f[path_d]:  # if the dictionary exist and is empty
                    schema_f[path_d][k] = "{}".format(type(value))
                else:
                    for i in schema_f[path_d].keys:
                        if (i == k):
                            exist = True
                    if(exist==False):
                        schema_f[path_d][k] = "{}".format(type(value))
                    exist = False
            # last key in the dictionary
            if len(deep)!=0:
             if list(schema_str)[-1]==k:
                deep.pop()
    elif(isinstance(schema_str,list)):
        path_l = path(key,ch_list)
        # Recursive  function to parse the Json
        ch_list = False  #  check there is list of dictionary or not
        for element in schema_str:
            if (isinstance(element,dict)):
                ch_list=True
                key_ch = "list"
                pars_doc(key_ch, element,ch_list)  # recursive
            else:
                ch_list=False
                v = v+"{}".format(type(element))
            if len(schema_f)!=0:
                if (schema_str[-1]) == element:
                    deep.pop()
        if schema_f[path_l]:
            schema_f[path_l][key] = "{}".format(v)
        else:
            for k in schema_f[path_l].keys:
                if (k == key):
                    exist = True
                if exist==False:
                    schema_f[path_l][key] = "{}".format(v)

#change the Deep (list)
#build the key of schema
def path(key,ch_list):
    deep_path = ""
    for i in range(len(deep)):
        if i==0:
            deep_path=deep[i]  #add deep[i]
        else:
            deep_path = deep_path+"_{}".format(deep[i]) #problem
    exist = False
    # should be distiguish-- has it key or not?
    if ch_list==True:
        deep_path = "{0}_{1}".format(deep_path, key)
    deep.append(deep_path)  # check when there is not such a dictionary build it
    for i in schema_f.keys():
        if i == deep_path:
            exist = True
            schema_f[deep_path]['object_counter']+= 1
        # adding the key  if doese'nt Exist and the connection of keys are not same
    if exist == False:
        schema_f[deep_path] = {}
        schema_f[deep_path]['object_counter'] = 1
    exist = False
    return deep_path

#fetch all of collection of all databases
def fetch_collection(db_name):
 for index in db_name:
    client = pymongo.MongoClient("localhost", 27017, maxPoolSize=50)
    d = dict((index, [collection for collection in client[index].list_collection_names()])
             for index in client.list_database_names())
 return(d)
# python concole to get the selected  ***** database *****  for processing the schema extraction
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
# python concole to get the selected  ****  colllections *** for processing the schema extraction
def collection_request(ind_schema,schema_name,table_names):
    r_db_t = dict()
    for index in ind_schema:
        key=schema_name[index]
        print("{0}-{1}\n".format(index,key))
        value=table_names[schema_name[index]]
        print("{}\n".format(value))
        table_num=input("please choose the number of collection : ")
        table_num=table_num.split()
        x = map(list, list(table_num))  # change the type
        x = sum(x, [])
        #build new dictionary of List with the name of database and tables numbers
        list1 = []
        for i in x:
            list1.append(value[int(i)])
        r_db_t[key]=list1

    print(r_db_t)
    return(r_db_t)

################ main ###############
conn = MongoClient("localhost", 27017)
db =conn.list_database_names()
db_collection=fetch_collection(db)
r_db_indices=db_request(db)
#recieve the the requested Table
end_table_re=collection_request(r_db_indices,db,db_collection)

for key, values in end_table_re.items():
    # rdcursur.execute("CREATE DATABASE DD_RD_{}".format(key))
    conn = MongoClient("localhost", 27017)
    for coll in values:
        collection = conn[key][coll]
        projects = collection.find()
        json_projects = []
        for project in projects:
            json_projects.append(project)
        json_s = str(json.dumps(json_projects,default=json_util.default))
        json_str=json.loads(json_s)   #raw json do0cument
        ############## global variable
        deep =[]
        #deep.append(coll)
        schema_f = {}
        schema_f[coll] = {}
        pars_doc(coll,json_str,True)
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(schema_f)
        coll=coll.lower()  #make all character lowercase
        key=key.lower()    #because of namespacing in the mysql
        rd_conn_result = mysql.connector.connect(       # converting extracted schema into DD
            host="127.0.0.1",
            user="root",
            password="2222",
            port=3308)
        rdcursur = rd_conn_result.cursor()
        db_name='mdb_{0}_{1}'.format(key,coll)
        db_name=re.sub("-","_",db_name)
        query="create database %s"%db_name    # create database sql query
        print(query)
        try:
            rdcursur.execute(query)
        except:
            pass
    for n,l in schema_f.items():
        rd_conn_result = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            passwd="2222",
            database="%s"%db_name,
            port=3308)
        rdcursur = rd_conn_result.cursor()
        t_name="{}".format(n)
        t_name=re.sub("-","_",t_name)
        print(t_name)
        query="CREATE TABLE %s (Property VARCHAR(45),TYPE_property VARCHAR(45))"%t_name
        rdcursur.execute(query)
        for m,p in schema_f[n].items():
            if isinstance(p,str):
                p=re.sub("<class '",'',p)
                p=re.sub("'>", '', p)
            p="{}".format(p)
            rd_conn_result = mysql.connector.connect(
                host="127.0.0.1",
                user="root",
                passwd="2222",
                database="%s" % db_name,
                port=3308)
            rdcursur = rd_conn_result.cursor()
            query = "INSERT INTO %s.%s (Property, TYPE_property) VALUES('%s','%s')" % (db_name, t_name, m, p)
            rdcursur.execute(query)
            rd_conn_result.commit()
            print(query)





