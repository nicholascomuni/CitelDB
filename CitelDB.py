import mysql.connector
import pandas as pd
from util import cname
import time

class DB:
    def __init__(self,host,port,user,pswd):
        self.host = host
        self.port = port
        self.user = user
        self.pswd = pswd
        self.conn = None

    def connect(self):
        #print("Connecting...")
        conn = mysql.connector.connect(host=self.host, user=self.user, password=self.pswd, port=self.port, db="AUTCOM")
        self.conn = conn

    def is_connected(self):
        return type(self.conn) == mysql.connector.connection_cext.CMySQLConnection


    def query(self,query):
        if self.is_connected() == False:
            self.connect()

        cursor = self.conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        return result



    def get_item_info(self, codigo, campo="*", table = "ITEGER", empresa = 13):
        if self.is_connected() == False:
            self.connect()

        if campo == "Descrição":
            table = "CADITE"
        if table == "CADITE":
            string = f"SELECT {cname[campo]} FROM {table} WHERE ITE_CODITE = {codigo};"
        else:
            if empresa == 13:
                string = f"SELECT {cname[campo]} FROM {table} WHERE ITE_CODITE = {codigo} AND ITE_CODEMP = 013;"
            elif empresa == "todas":
                string = f"SELECT {cname[campo]} FROM {table} WHERE ITE_CODITE = {codigo};"
            else:
                raise

        dtf = pd.read_sql(string, self.conn)

        if len(dtf) > 1:
            return dtf
        else:
            return dtf[cname[campo]][0]

    def get_multiple_info(self, codigos):
        if self.is_connected() == False:
            self.connect()

        string = "SELECT ITE_CODITE, ITE_PREVE1 FROM ITEGER WHERE ITE_CODEMP = 013 AND ("
        prm = True
        for i in codigos:
            if prm == True:
                string += f"ITE_CODITE = {i} "
                prm = False
            else:
                string += f"OR ITE_CODITE = {i} "

        string += ");"

        dtf = pd.read_sql(string, self.conn)

        return dtf


    def search_item(self, text):
        if self.is_connected() == False:
            self.connect()

        #string = f"SELECT ITE_DESITE, ITE_CODITE FROM CADITE WHERE ITE_DESITE LIKE '%{text}%'"
        string = f"SELECT CADITE.ITE_DESITE, CADITE.ITE_CODITE, ITEGER.ITE_PREVE1 FROM CADITE INNER JOIN ITEGER ON CADITE.ITE_CODITE = ITEGER.ITE_CODITE WHERE (CADITE.ITE_DESITE LIKE '%{text}%') AND (ITEGER.ITE_CODEMP = 013);"
        dtf = pd.read_sql(string, self.conn)

        return dtf
    
    def get_table_dtf(self,string):
        if self.is_connected() == False:
            self.connect()

        
        #string = "SELECT ITE_CODITE, ITE_PREVE1 FROM ITEGER WHERE ITE_CODEMP = 013"
        #string = f"SELECT ITE_CODITE,ITE_ULTALT FROM ITEGER;"

        dtf = pd.read_sql(string, self.conn)

        return dtf
    
    def busca_ref(self, ref):
        if self.is_connected() == False:
            self.connect()
      
        string = f"SELECT CADITE.ITE_CODITE, CADITE.ITE_REFERE, CADITE.ITE_CODBAR, CADITE.ITE_REFERE, CADITE.ITE_CODFOR, ITEGER.ITE_PRECOM FROM CADITE INNER JOIN ITEGER ON CADITE.ITE_CODITE = ITEGER.ITE_CODITE WHERE (CADITE.ITE_REFERE = '{ref}') AND (ITEGER.ITE_CODEMP = 001);"  
        dtf = pd.read_sql(string, self.conn)
        return dtf
    
    
    
    
    def busca_ref_multiple(self, referencias):
        if self.is_connected() == False:
            self.connect()
            
        
        string = f"SELECT CADITE.ITE_CODITE, CADITE.ITE_CODBAR, CADITE.ITE_REFERE, CADITE.ITE_CODFOR, ITEGER.ITE_PRECOM, ITEGER.ITE_PREVE1 FROM CADITE INNER JOIN ITEGER ON CADITE.ITE_CODITE = ITEGER.ITE_CODITE WHERE (ITEGER.ITE_CODEMP=001) AND ("  
        prm = True
        for i in referencias:
            if prm == True:
                string += f"CADITE.ITE_REFERE = '{i}' "
                prm = False
            else:
                string += f"OR CADITE.ITE_REFERE = '{i}' "
        
        string += ");"
        dtf = pd.read_sql(string, self.conn)
        dtf.ITE_REFERE = dtf.ITE_REFERE.apply(lambda x:x.strip())
        return dtf
    
def search(text):
    db = DB()
    db.connect()
    resultado = db.search_item(text)
    db.conn.close()
    return resultado
