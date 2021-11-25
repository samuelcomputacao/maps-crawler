import os
import json
from datetime import datetime, timedelta
from database.database import DataBase
from twitte.tweetAPI import TweetAPI
from machineLearning.model import classificate
from extract.extract import getData
from util.stringUtil import cleanText

os.environ['TZ'] = 'America/Sao_Paulo'

# API TWEET CREDENTIALS
CONSUMER_KEY = 'UZLEno0H4UqDnMbzp06lAO4K4'
CONSUMER_SECRET = '8Q60W8qUu0ZqGEMO7zpeD9MoGZD6HFTTVgrJa72VVnykVCoFwv'
ACCESS_TOKEN = '1071095366-zFazweFNeItzWbqbgojoBQNEKVUfFA1GOSyRivt'
ACCESS_TOKEN_SECRET = 'YNBChOqrv805TEW8uBD5HsagHSOeX6ufZzzPFBuNPLgx5'

# DATABASE CREDENTIALS
HOST = 'localhost'
DATABASE = 'transito'
USER = 'postgres'
PASSWORD = '12345'


def findFriends(api):
    return api.get_friends(count=200)


def findTwittes(api, friends=[]):
    date = datetime.today()
    date = datetime(year=date.year, month=date.month, day=date.day)
    date = date - timedelta(days=1)
    print(f'Buscando Twitters em {date}')
    return api.search_by_friends(keywords=keywords, date=date, friends=friends, count=5)


keywords = ['ferid', 'acidente', 'mort', 'choque', 'trajédia', 'atropel',
            'vitima', 'colisão', 'virada', 'virou', 'engavetamento', 'abalroamento', 'batida', 'capota',
            'incendio', 'capotamento'
                        'morre', 'faleceu', 'saiu da pista', 'tombamento', 'tombou', 'bateu']

twittes = [{'user': 'g1parana', 'local': '',
            'text_standart': 'parana registra mais 29 mortes por covid e 704 casos confirmados indica boletim  g1 g1pr ',
            'text': 'Paraná registra mais 29 mortes por Covid e 704 casos confirmados, indica boletim https://t.co/EcXB7Zz80R #g1 #g1pr https://t.co/duhNyuCXIZ',
            'date': '17/11/2021 19:11:53', 'id': 1461094770621784077}, {'user': 'transitorec_', 'local': 'Recife / Pe',
                                                                        'text_standart': 'pina lt acidente gt   acidente em frente ao clube cabanga sentido agamenon na faixa da esquerda transito parado transitorecife recife  ',
                                                                        'text': 'PINA &lt; ACIDENTE &gt; - Acidente em frente ao Clube Cabanga, sentido Agamenon, na faixa da esquerda. Trânsito parado. #transitoRecife #Recife @cbntransito @CTTU_Recife',
                                                                        'date': '04/11/2021 09:29:22',
                                                                        'id': 1456237130565443586},
           {'user': 'transitorec_', 'local': 'Recife / Pe',
            'text_standart': 'viaduto capitao temudo atropelamento de animal deixa transito muito complicado no sentido boa viagem evite se puder transitorecife',
            'text': 'Viaduto Capitão Temudo, atropelamento de animal deixa trânsito muito complicado no sentido Boa Viagem. Evite se puder #transitoRecife',
            'date': '20/10/2021 08:34:29', 'id': 1450787502529388544}, {'user': 'transitorec_', 'local': 'Recife / Pe',
                                                                        'text_standart': 'via mangue sentido boa viagem acidente no inicio da via mangue deixa transito complicado na area transitorecife',
                                                                        'text': 'VIA MANGUE, sentido Boa Viagem, acidente no início da Via Mangue deixa trânsito complicado na área. #transitoRecife',
                                                                        'date': '13/10/2021 12:16:56',
                                                                        'id': 1448306767767744514},
           {'user': 'portalcorreio', 'local': 'João Pessoa, PB',
            'text_standart': 'doencas cardiovasculares sao a causa de morte nao oncologica mais comum nesse publico saude portalcorreio  ',
            'text': 'Doenças cardiovasculares são a causa de morte não-oncológica mais comum nesse público #Saúde #PortalCorreio\n\nhttps://t.co/98Bs7ZhAlW',
            'date': '17/11/2021 15:04:39', 'id': 1461032550370095119},
           {'user': 'pmscrodoviaria', 'local': 'Santa Catarina, Brasil',
            'text_standart': 'motocicleta pega fogo na saida da curva do elevado dias velho que da acesso a ponte colombo salles na capital na tarde desta quarta 17   o condutor que teve apenas ferimentos leves perdeu o controle da moto e acabou caindo com o atrito a moto se incendiou ',
            'text': '🔥Motocicleta pega fogo na saída da curva do Elevado Dias Velho que dá acesso à Ponte Colombo Salles, na Capital, na tarde desta quarta, 17. \n\nO condutor, que teve apenas ferimentos leves, perdeu o controle da moto e acabou caindo. Com o atrito, a moto se incendiou! https://t.co/HQ8oglIbov',
            'date': '17/11/2021 19:21:03', 'id': 1461097074917793804}, {'user': 'prf_pb', 'local': '',
                                                                        'text_standart': '18h28   pista parcialmente interditada no km 15 da br 101 no sentido natal joao pessoa com apenas a faixa esquerda livre devido a tombamento de uma carreta dirija com atencao transitojampa',
                                                                        'text': '18h28 - Pista parcialmente interditada no km 15 da BR-101, no sentido Natal-João Pessoa, com apenas a faixa esquerda livre, devido a tombamento de uma carreta. Dirija com atenção! #transitojampa',
                                                                        'date': '14/11/2021 18:30:07',
                                                                        'id': 1459997096136916997},
           {'user': 'prf_pb', 'local': '',
            'text_standart': '13h34 em joao pessoa no km 24 da br 230 viaduto cristo redentor sentido cabedelo transito com retencao de fluxo em virtude de acidente de transito dirija com atencao transitojampa',
            'text': '13h34: Em João Pessoa, no km 24 da BR 230, viaduto Cristo Redentor, sentido Cabedelo, trânsito com retenção de fluxo, em virtude de acidente de trânsito. Dirija com atenção.\n#transitojampa',
            'date': '12/11/2021 13:35:28', 'id': 1459198168307056649}, {'user': 'abcr_rodovias', 'local': '',
                                                                        'text_standart': ' viasul nao registra mortes em suas rodovias durante o feriado   ',
                                                                        'text': '.@GrupoCCROficial ViaSul não registra mortes em suas rodovias durante o feriado.\n\nhttps://t.co/XGISbTNEe0 https://t.co/AAyzpZjK7K',
                                                                        'date': '17/11/2021 12:49:32',
                                                                        'id': 1460998549588660236},
           {'user': 'g1', 'local': 'Brasil',
            'text_standart': 'brasil registra 374 mortes por covid em 24 horas media movel de casos e a menor em mais de um ano e meio  g1 covid ',
            'text': 'Brasil registra 374 mortes por Covid em 24 horas; média móvel de casos é a menor em mais de um ano e meio https://t.co/2X75MuMr9J #g1 #covid https://t.co/HuUB4JvL09',
            'date': '17/11/2021 20:04:08', 'id': 1461107919861211137}, {'user': 'JornalDaGlobo', 'local': '',
                                                                        'text_standart': 'a empresa pediu aos reguladores dos eua nesta terca 16 autorizacao para uso emergencial de sua pilula contra a covid 19 em testes o remedio reduziu a hospitalizacao ou morte em quase 90 entre pacientes de alto risco recem infectados  jg',
                                                                        'text': 'A empresa pediu aos reguladores dos EUA, nesta terça (16), autorização para uso emergencial de sua pílula contra a Covid-19. Em testes, o remédio reduziu a hospitalização ou morte em quase 90% entre pacientes de alto risco recém-infectados: https://t.co/zrDy2WlAZd #JG',
                                                                        'date': '17/11/2021 01:34:15',
                                                                        'id': 1460828606662426627},
           {'user': 'PRFBrasil', 'local': 'Brasil',
            'text_standart': ' comparado com o mesmo feriadao de 2019 tivemos 3 menos mortes este ano o feriadao da proclamacao da republica teve mais de 700 acidentes uma reducao de 12 incluindo 210 graves que cairam 9 em relacao a 2019 ',
            'text': '📉 Comparado com o mesmo feriadão de 2019, tivemos 3% menos mortes. Este ano, o feriadão da Proclamação da República teve mais de 700 acidentes, uma redução de 12%, incluindo 210 graves, que caíram 9%, em relação a 2019. https://t.co/qrTpGDie4A',
            'date': '16/11/2021 18:43:10', 'id': 1460725155773177860}, {'user': 'PRFBrasil', 'local': 'Brasil',
                                                                        'text_standart': ' encerramos a operacao proclamacao da republica com reducao no numero de mortes e acidentes nas estradas mas ainda assim flagramos muita imprudencia nas rodovias e estradas federais do pais  ',
                                                                        'text': '👮🏼\u200d♀️👮🏼 Encerramos a Operação Proclamação da República, com redução no número de mortes e acidentes nas estradas! Mas, ainda assim, flagramos muita imprudência nas rodovias e estradas federais do país. 🚔 https://t.co/wrXtUWBDsl',
                                                                        'date': '16/11/2021 18:43:08',
                                                                        'id': 1460725147531366420},
           {'user': 'ecovia', 'local': '',
            'text_standart': 'trafego esta em meia pista no km 42 da br 277 sentido litoral regiao da serra do mar devido colisao traseira',
            'text': 'Tráfego está em meia pista no km 42 da BR-277 sentido Litoral, região da Serra do Mar devido colisão traseira.',
            'date': '17/11/2021 14:13:47', 'id': 1461019751166140424},
           {'user': 'prf_pi', 'local': 'Teresina - Piaui - Brasil',
            'text_standart': 'caminhoneiro se envolve em acidente grave e prf encontra 137 unidades de anfetaminas na cabine do veiculo ',
            'text': 'Caminhoneiro se envolve em acidente grave e PRF encontra 137 unidades de anfetaminas na cabine do veículo https://t.co/VWfOLaRjnV',
            'date': '16/11/2021 10:00:31', 'id': 1460593623788052482},
           {'user': 'PRF191PE', 'local': 'Recife, Pernambuco, Brazil.',
            'text_standart': 'balanco do dia 16112021  010 acidentes  017 veiculos envolvidos  008 feridos  000 morto  268 pessoas fiscalizadas  235 veiculos fiscalizados  219 autos de infracao  017 veiculos recolhidos  001 veiculo recuperado 005 testes de alcoolemia  003 pessoas detidas',
            'text': 'BALANÇO DO DIA 16/11/2021 \n010 ACIDENTES \n017 VEÍCULOS ENVOLVIDOS \n008 FERIDOS \n000 MORTO \n268 PESSOAS FISCALIZADAS \n235 VEÍCULOS FISCALIZADOS \n219 AUTOS DE INFRAÇÃO \n017 VEÍCULOS RECOLHIDOS \n001 VEÍCULO RECUPERADO\n005 TESTES DE ALCOOLEMIA \n003 PESSOAS DETIDAS',
            'date': '17/11/2021 01:17:52', 'id': 1460824484974366722},
           {'user': 'PRF191PE', 'local': 'Recife, Pernambuco, Brazil.',
            'text_standart': 'balanco do dia 11112021  003 acidentes  004 veiculos envolvidos  001 ferido  001 morto  373 pessoas fiscalizadas  291 veiculos fiscalizados  236 autos de infracao  012 veiculos recolhidos',
            'text': 'BALANÇO DO DIA 11/11/2021 \n003 ACIDENTES \n004 VEÍCULOS ENVOLVIDOS \n001 FERIDO \n001 MORTO \n373 PESSOAS FISCALIZADAS \n291 VEÍCULOS FISCALIZADOS \n236 AUTOS DE INFRAÇÃO \n012 VEÍCULOS RECOLHIDOS',
            'date': '12/11/2021 01:32:09', 'id': 1459016138059362331},
           {'user': 'PRF191PE', 'local': 'Recife, Pernambuco, Brazil.',
            'text_standart': 'balanco do dia 10112021   008 acidentes  014 veiculos envolvidos  009 feridos  001 morto  289 pessoas fiscalizadas  258 veiculos fiscalizados  371 autos de infracao  020 veiculos recolhidos  001 pessoa detida',
            'text': 'BALANÇO DO DIA 10/11/2021\n \n008 ACIDENTES \n014 VEÍCULOS ENVOLVIDOS \n009 FERIDOS \n001 MORTO \n289 PESSOAS FISCALIZADAS \n258 VEÍCULOS FISCALIZADOS \n371 AUTOS DE INFRAÇÃO \n020 VEÍCULOS RECOLHIDOS \n001 PESSOA DETIDA',
            'date': '11/11/2021 00:59:31', 'id': 1458645537725501443},
           {'user': 'PRF191PE', 'local': 'Recife, Pernambuco, Brazil.',
            'text_standart': 'balanco do dia 09112021   007 acidentes  012 veiculos envolvidos  007 feridos  001 morto  598 pessoas fiscalizadas  624 veiculos fiscalizados  210 autos de infracao  024 veiculos recolhidos  017 testes de alcoolemia 002 pessoas detidas',
            'text': 'BALANÇO DO DIA 09/11/2021\n \n007 ACIDENTES \n012 VEÍCULOS ENVOLVIDOS \n007 FERIDOS \n001 MORTO \n598 PESSOAS FISCALIZADAS \n624 VEÍCULOS FISCALIZADOS \n210 AUTOS DE INFRAÇÃO \n024 VEÍCULOS RECOLHIDOS \n017 TESTES DE ALCOOLEMIA\n002 PESSOAS DETIDAS',
            'date': '10/11/2021 01:51:44', 'id': 1458296293592285186},
           {'user': 'PRF191PE', 'local': 'Recife, Pernambuco, Brazil.',
            'text_standart': 'balanco do dia 08112021  006 acidentes  008 veiculos envolvidos  005feridos  002 mortos  375 pessoas fiscalizadas  301 veiculos fiscalizados  354 autos de infracao  019 veiculos recolhidos  006 testes de alcoolemia 001 pessoas detidas',
            'text': 'BALANÇO DO DIA 08/11/2021 \n006 ACIDENTES \n008 VEÍCULOS ENVOLVIDOS \n005FERIDOS \n002 MORTOS \n375 PESSOAS FISCALIZADAS \n301 VEÍCULOS FISCALIZADOS \n354 AUTOS DE INFRAÇÃO \n019 VEÍCULOS RECOLHIDOS \n006 TESTES DE ALCOOLEMIA\n001 PESSOAS DETIDAS',
            'date': '09/11/2021 01:50:31', 'id': 1457933597097275392},
           {'user': 'LitoralNorte', 'local': 'Bahia, Brasil',
            'text_standart': '1700 atencao em caso de acidente ou emergencia ligue 0800 071 3233 telefone atendimento 24hs',
            'text': '17:00 Atenção! Em caso de acidente ou emergência ligue 0800 071 3233. Telefone atendimento 24hs.',
            'date': '17/11/2021 17:00:02', 'id': 1461061586563522563},
           {'user': 'rdasbandeiras', 'local': 'Itatiba - SP',
            'text_standart': ' d pedro i em atibaia tem congestionamento de 6km sentido campinas pouco apos a praca de pedagio por conta de um acidente no km 87 pista esta bloqueada e desvio foi implantado pelas alcas do dispositivo sem alterar o percurso nao ha previsao para normalizacao do trafego',
            'text': '❌ D. Pedro I, em Atibaia, tem congestionamento de 6km sentido Campinas, pouco após a praça de pedágio, por conta de um acidente no km 87. Pista está bloqueada e desvio foi implantado pelas alças do dispositivo, sem alterar o percurso. Não há previsão para normalização do tráfego',
            'date': '17/11/2021 20:13:18', 'id': 1461110224044142595},
           {'user': 'rdasbandeiras', 'local': 'Itatiba - SP',
            'text_standart': 'transito e intenso neste fim de tarde no anel viario e no entroncamento da d pedro i com a anhanguera em campinas e tambem no trecho final da joao cereser em jundiai  excesso de veiculos e tipico do horario nao ha registro de acidentes ',
            'text': 'Trânsito é intenso neste fim de tarde no anel viário e no entroncamento da D. Pedro I com a Anhanguera, em Campinas, e também no trecho final da João Cereser, em Jundiaí. 🚗🚙 Excesso de veículos é típico do horário. Não há registro de acidentes. https://t.co/Rmpiofb85W',
            'date': '17/11/2021 17:58:57', 'id': 1461076416183812105},
           {'user': 'rdasbandeiras', 'local': 'Itatiba - SP',
            'text_standart': ' tarde comeca com tempo bom e pistas livres no corredor dom pedro mesmo nos trechos em obras tambem nao ha acidentes  pontos em obras d pedro i nazare paulista km 43 sentido jacarei valinhos km 119 sentido jacarei  joao cereser jundiai km 65 e 63 sentido anhanguera ',
            'text': '😎 Tarde começa com tempo bom e pistas livres no Corredor Dom Pedro, mesmo nos trechos em obras. Também não há acidentes\n\nPontos em obras\n📍D. Pedro I\nNazaré Paulista: km 43, sentido Jacareí\nValinhos: km 119, sentido Jacareí\n📍 João Cereser\nJundiaí: km 65 e 63, sentido Anhanguera https://t.co/A5kX9TOXHD',
            'date': '17/11/2021 12:11:02', 'id': 1460988859999825927},
           {'user': 'rdasbandeiras', 'local': 'Itatiba - SP',
            'text_standart': 'transito flui bem nesta manha mesmo nos trechos com interdices dia e ensolarado e sem acidentes   pontos em obras hoje   d pedro i nazare paulista km 43 sentido jacarei valinhos km 119 sentido jacarei   joao cereser jundiai km 65 e 63 sentido anhanguera ',
            'text': 'Trânsito flui bem nesta manhã, mesmo nos trechos com interdições. Dia é ensolarado e sem acidentes. \n\nPontos em obras hoje:\n\n📍 D. Pedro I\nNazaré Paulista: km 43, sentido Jacareí\nValinhos: km 119, sentido Jacareí\n\n📍 João Cereser\nJundiaí: km 65 e 63, sentido Anhanguera https://t.co/5S8NX0SWZF',
            'date': '17/11/2021 09:50:30', 'id': 1460953492437311488},
           {'user': 'rdasbandeiras', 'local': 'Itatiba - SP',
            'text_standart': ' transito e mais carregado somente na chegada ao trevo de barao geraldo para o motorista que vem de paulinia nao ha registro de acidentes no corredor dom pedro apenas o excesso de veiculos do horario ',
            'text': '🚗🚚 Trânsito é mais carregado somente na chegada ao Trevo de Barão Geraldo, para o motorista que vem de Paulínia. Não há registro de acidentes no Corredor Dom Pedro, apenas o excesso de veículos do horário. https://t.co/jhoZ8DRxzc',
            'date': '17/11/2021 08:25:28', 'id': 1460932093328908290}]


def execute(api, dataBase, MUNICIPIOS, ESTADOS):
    # friends = findFriends(api)
    # twittes = findTwittes(api, friends)
    for tw in twittes:
        if classificate(tw):
            data = getData(tw, MUNICIPIOS, ESTADOS)
            if data is not None:
                ponto = dataBase.calculaKM(data)
                if ponto is not None:
                    ponto = json.loads(ponto)['coordinates']
                    data['longitude'] = ponto[0]
                    data['latitude'] = ponto[1]
                    data['texto'] = tw['text']
                    dataBase.salvarNotificacao(data)

dataBase = DataBase(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
api = TweetAPI(consumer_key=CONSUMER_KEY, consumer_secret=CONSUMER_SECRET,
               access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET)

ESTADOS = {}
MUNICIPIOS = {}
for mun in dataBase.getMunicipios(coluns=['nm_mun', 'sigla_uf']):
    MUNICIPIOS[cleanText(mun[0].lower())] = mun[1].lower()
for uf in dataBase.getEstados(coluns=['nm_uf', 'sigla_uf']):
    ESTADOS[cleanText(uf[0].lower())] = uf[1].lower()

execute(api, dataBase, MUNICIPIOS, ESTADOS)
