from neo4j import GraphDatabase, basic_auth
import neo4j
import collections
import mysql.connector
from mysql.connector import Error

def node_labels():
    query = "match (n) return distinct labels(n)"
    with driver.session() as session:
        end = session.run(query)
    labels = []
    for i in end:
        label = list(set(i["labels(n)"]))
        label = label.pop()  # this assumes only a single relationship type, but that's ok since that's how neo4j works
        labels.append(label)
    return labels
def nodes_of_lables(label):
    query = "match (n) where n:{} return n".format(label[0])
    with driver.session() as session:
        end = session.run(query)
    for i in end:
        label = list(set(i["n"]))
        label = label.pop()
        labels.append(label)
    return labels
def count_nodes(label):
    query = "match (n) where n:{} return count(n)".format(label)
    with driver.session() as session:
        end = session.run(query)
    return end.single()["count(n)"]

def property_labels(label):
   rdcursor.execute("CREATE TABLE IF NOT EXISTS "
                     "property_node_%s(Node VARCHAR(80), Property VARCHAR(80))" % j)
   query = "MATCH (n:{})-[r]->(m) RETURN distinct(labels(n)), keys(n)".format(label)
   with driver.session() as session:
      end = session.run(query)
   for record in end:
      s_node = list(set(record["(labels(n))"]))
      s_properties =list(set(record["keys(n)"]))
      s_p=""
      for i in range(len(s_properties)):
          if i==0:
              s_p=s_properties[i]
          s_p="%s,%s"%(s_p,s_properties[i])
      q_sql="INSERT INTO dd_graph_database. property_node_%s(Node,Property) VALUES('%s','%s')" % (j,s_node[0],s_p)
      rdcursor.execute(q_sql)
      rd_conn_result.commit()

def relationship_types():
    query = "match ()-[r]->() return distinct type(r)"
    with driver.session() as session:
        end = session.run(query)
    end = [i["type(r)"] for i in end]
    for j in range(len(end)):
        print(end[j])
    return end
"""def relationship_name(rel):
    query = "match (n)-[r:{}]->(m) return distinct type(r)".format(rel[0])
    with driver.session() as session:
        end = session.run(query)
    for i in end:
        label = list(set(i["type(r)"]))
        label = label.pop()
        labels.append(label)
    return labels"""
def relationship_count(rel):
    query = "match ()-[r:{}]->() return count(type(r))".format(rel)
    with driver.session() as session:
        end = session.run(query)
    return end.single()["count(type(r))"]

def relationship_properties(relationship):
    rdcursor.execute("CREATE TABLE IF NOT EXISTS "
                     "property_relationship_%s(Relationship VARCHAR(80), Property VARCHAR(80))" % relationship)
    query = "MATCH (n)-[r:{}]->(m) RETURN distinct(type(r)), keys(r)".format(relationship)
    with driver.session() as session:
        end = session.run(query)
    for record in end:
        s_r = list(set(record["(type(r))"]))
        s_pro = list(set(record["keys(r)"]))
        s_p = ""
        for i in range(len(s_pro)):
            if i == 0:
                s_p = s_pro[i]
            s_p = "%s,%s" % (s_p, s_pro[i])
        q_sql = "INSERT INTO dd_graph_database.property_relationship_%s(Relationship,Property) VALUES('%s','%s')" % (relationship,relationship,s_p)
        rdcursor.execute(q_sql)
        rd_conn_result.commit()

def all_property_outgoing(label):
    query = "MATCH (n:{})-[r]->(m) " \
            "RETURN labels(n), keys(n), size(keys(n)), count(*)," \
            "type(r),keys(r),size(keys(r))," \
            "labels(m),keys(m),size(keys(m)) " \
            "ORDER BY size(keys(n)) DESC".format(label)
    with driver.session() as session:
        end = session.run(query)
    for record in end:
        s_node = record["labels(n)"]
        s_properties = record["keys(n)"]
        s_number_of_properties = record["size(keys(n))"]
        number_of_nodes = record["count(*)"]
        rel_name = record["type(r)"]
        relation_properties = record["keys(r)"]
        number_of_relation = record["size(keys(n))"]
        t_node = record["labels(m)"]
        t_properties = record["keys(m)"]
        t_number_of_properties = record["size(keys(m))"]
        print("%s %s %n %n %s %s %n %s %s" % (s_node, s_properties, s_number_of_properties,
                                              number_of_nodes, rel_name, relation_properties,
                                              number_of_relation,
                                              t_node, t_properties))

############          main     #################
rd_conn_result = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        passwd="2222",
        port=3308)
try:
    rdcursor = rd_conn_result.cursor()
    rdcursor.execute("CREATE DATABASE IF NOT EXISTS DD_Graph_database CHARACTER SET utf8") #  build neo4j data dictionary
except:
    pass
rd_conn_result = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        passwd="2222",
        database="DD_Graph_database",
        port=3308)
rdcursor = rd_conn_result.cursor()

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1111"), encrypted=False)
session = driver.session()
###################### node #################
labels = node_labels()
for i in labels:
    nodes = []
    nodes = nodes_of_lables(i)
rdcursor.execute("CREATE TABLE IF NOT EXISTS list_of_nodes(Name VARCHAR(80), Number INT(20))")
for j in nodes:
    name = str(j)
    count = count_nodes(j)  # count nodes of lable
    rdcursor.execute("INSERT INTO list_of_nodes(NAME, Number) VALUES('%s',%s)" % (name, count))
    rd_conn_result.commit()
    property_l=property_labels(j)
driver.close()
################## relationship #######################
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1111"), encrypted=False)
session = driver.session()
relation= relationship_types()
rdcursor.execute("CREATE TABLE IF NOT EXISTS list_of_relationship(Name VARCHAR(80), Number INT(20))")
for k in relation:
    name_rel = str(k)
    count_r = relationship_count(k)  # count nodes of lable
    query="INSERT INTO list_of_relationship(NAME, Number) VALUES('%s',%s)" % (name_rel, count_r)
    rdcursor.execute(query)
    rd_conn_result.commit()
    relationship_properties(name_rel)



