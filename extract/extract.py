from re import findall
from util.stringUtil import cleanText, incluso
from functools import cmp_to_key


def is_number(char):
    return str(char) in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']


def extract_numerical(texto):
    retorno = ''
    flag = False
    for i in range(len(texto)):
        if is_number(texto[i]) and not flag:
            retorno += texto[i]
        else:
            flag = True
    return retorno


def extract_km(tw):
    texto = tw['text_standart']
    tokens = findall(r'km[s]*[ ]*\d+', texto)
    if len(tokens) > 0:
        km = extract_numerical(tokens[0][::-1])[::-1]
        if len(km) > 0:
            try:
                int(km)
                return km
            except ValueError:
                print("Erro ao converter km!")
                pass
    return None


def extract_br(tw, prefixo):
    texto = tw['text_standart']
    tokens = findall(r'%s[ ]*\d+' % prefixo, texto)
    if len(tokens) > 0:
        br = extract_numerical(tokens[0][::-1])[::-1]
        if len(br) > 0:
            try:
                int(br)
                return br
            except ValueError:
                print("Erro ao converter br!")
    return None


def sizeComp(x, y):
    return len(y) - len(x)


def extract_uf(tw, MUNICIPIOS, ESTADOS):
    local = cleanText(tw['local'])
    usuario = cleanText(tw['user'])

    estados = sorted(list(filter(lambda x: incluso(x, usuario), ESTADOS.keys())) + list(
        filter(lambda x: incluso(x, local), ESTADOS.keys())), key=cmp_to_key(sizeComp))
    muncs = sorted(list(filter(lambda x: incluso(x, usuario), MUNICIPIOS.keys())) + list(
        filter(lambda x: incluso(x, local), MUNICIPIOS.keys())), key=cmp_to_key(sizeComp))
    ufs = list(filter(lambda x: incluso(x, usuario), ESTADOS.values())) + list(
        filter(lambda x: incluso(x, local), ESTADOS.values()))

    if len(ufs) > 0:
        return ufs[0]
    elif len(estados) > 0:
        return ESTADOS[estados[0]]
    elif len(muncs) > 0:
        return MUNICIPIOS[muncs[0]]
    return None


def extract_ufbr(tw, municipios, estados):
    uf = extract_uf(tw, municipios, estados)
    br = extract_br(tw, 'br')
    if br is None and uf is not None:
        br = extract_br(tw, uf)
    if br is not None and uf is not None:
        return (uf, br)
    return None


def extract_basic(tw, municipios, estados):
    ufbr = extract_ufbr(tw, municipios, estados)
    km = extract_km(tw)
    if ufbr is not None and km is not None:
        uf, br = ufbr
        return {"km": km,
                "id": tw['id'],
                "uf": uf,
                "br": br,
                "date": tw['date']}
    return None


def valida_tw(tw):
    for key in tw.keys():
        if tw[key] is None:
            return False
    return True


def not_valida_tw(tw):
    return not valida_tw(tw)


def getData(tw, municipios, estados):
    data = extract_basic(tw, municipios, estados)
    if valida_tw(tw):
        return data
    return None
