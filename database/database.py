import psycopg2

class DataBase:
    connection = None

    def __init__(self):
        self.connection = psycopg2.connect(host='localhost', database='transito', user='postgres', password='12345')

    def __selectTable(self, table = '', coluns=["*"], where=[]):
        where.append("1=1")
        cur = self.connection.cursor()
        cur.execute(f"SELECT {','.join(coluns)} FROM {table} WHERE {' AND '.join(where)}")
        rs = cur.fetchall()
        cur.close()
        return rs

    def getEstados(self, coluns=["*"], where=[]):
        return self.__selectTable(table='estado', coluns=coluns,where=where)

    def getMunicipios(self, coluns=["*"], where=[]):
        return self.__selectTable(table='municipio', coluns=coluns, where=where)

    def getRodoviasFederais(self, coluns=["*"], where=[]):
        return self.__selectTable(table='rodovias_federais', coluns=coluns, where=where)

    def closeConnection(self):
        self.connection.close()

    def salvarClassificacao(self, tweets = []):
        if len(tweets) > 0:
            sql = "CREATE TABLE IF NOT EXISTS dados_prf(" \
                  "id VARCHAR(255)," \
                  "ufbr VARCHAR(10)," \
                  "km_trunc INTEGER," \
                  "dia_semana_num INTEGER," \
                  "turno_simples INTEGER," \
                  "tipo_pista_simples INTEGER," \
                  "categoria_sentido_via INTEGER," \
                  "tracado_via_simples INTEGER," \
                  "condicao_metereologica_simples INTEGER," \
                  "tipo_acidente_simples INTEGER," \
                  "classe INTEGER)"
            cur = self.connection.cursor()
            cur.execute(sql)
            self.connection.commit()
            for tw in tweets:
                if not len(self.getDadosPRF(where=[f"id = '{tw['id']}'"])) > 0:
                    sql = f"INSERT INTO dados_prf (id,ufbr,km_trunc,dia_semana_num,turno_simples,tipo_pista_simples,categoria_sentido_via,tracado_via_simples," \
                          f"condicao_metereologica_simples,tipo_acidente_simples,classe)" \
                          f" VALUES ('{tw['id']}','{tw['ufbr']}',{tw['km_trunc']},{tw['dia_semana_num']},{tw['turno_simples']}," \
                          f"{tw['tipo_pista_simples']},{tw['categoria_sentido_via']},{tw['tracado_via_simples']}," \
                          f"{tw['condicao_metereologica_simples']},{tw['tipo_acidente_simples']},{tw['classe']})"
                    cur.execute(sql)
            self.connection.commit()

    def getNotificacoes(self, coluns=["*"], where=[]):
        return self.__selectTable(table='notificacoes', coluns=coluns, where=where)

    def salvarNotificacao(self, notificacao):
        sql = "CREATE TABLE IF NOT EXISTS notificacoes(" \
              "id VARCHAR(255) PRIMARY KEY," \
              "uf VARCHAR(2) NOT NULL ," \
              "br INTEGER NOT NULL," \
              "km INTEGER NOT NULL," \
              "data DATE)"
        cur = self.connection.cursor()
        cur.execute(sql)
        self.connection.commit()
        rodovias = self.getRodoviasFederais(where=[f"vl_br='{notificacao['br']}'", f"vl_km_inic<={notificacao['km']}", f"vl_km_fina>={notificacao['km']}", f"LOWER(sg_uf)='{notificacao['uf']}'"])
        if not len(self.getNotificacoes(where=[f"id = '{notificacao['id']}'"])) > 0 and len(rodovias) > 0:
            sql = f"INSERT INTO notificacoes (id,uf, br, km, data)" \
                  f" VALUES ('{notificacao['id']}','{notificacao['uf']}','{notificacao['br']}',{notificacao['km']},'{notificacao['date']}')"
            cur.execute(sql)
        self.connection.commit()


    def salvarTwitters(self, tweets):
        print('Salvando Twitters')
        cont = 0
        sql = "CREATE TABLE IF NOT EXISTS twitter( " \
              "usuario VARCHAR(100)," \
              "local VARCHAR(100)," \
              "texto VARCHAR(500) NOT NULL," \
              "data VARCHAR(20)," \
              "id BIGINT," \
              "CONSTRAINT tweet_pk PRIMARY KEY(id))"
        cur = self.connection.cursor()
        cur.execute(sql)
        self.connection.commit()
        for tw in tweets:
            if not len(self.getTwitters(where=[f"id = '{tw['id']}'"])) > 0:
                sql = f"INSERT INTO twitter (id, usuario, local, texto, data) VALUES ('{tw['id']}','{tw['usuario']}','{tw['local']}','{tw['texto']}','{tw['data']}')"
                try:
                    cur.execute(sql)
                    cont += 1
                    for link in tw['links']:
                        sql = f"INSERT INTO link (id_tw, endereco) VALUES ('{tw['id']}','{link}')"
                        cur.execute(sql)
                except Exception as err:
                    print("Error: {0}".format(err))
                    pass
        self.connection.commit()
        print(f'Twitters salvos: {cont}')
