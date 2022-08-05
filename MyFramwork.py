
import json
import re
dataA = json.loads('[{"name":"ItemId","type":"INTEGER","mode":"NULLABLE"},{"name":"ProjectName","type":"STRING","mode":"NULLABLE"}]')
dataB = json.loads('[{"name":"ItemId","type":"INTEGER","mode":"NULLABLE"},{"name":"ProjectId","type":"STRING","mode":"NULLABLE"}]')

def mergejson (_jsonA, _jsonB):
    _jsonfinal=_jsonA
    for x in _jsonB:
        if not any([obj.get('name') == x["name"] for obj in _jsonA]):
            _jsonfinal.append(x)
    # print(_jsonfinal)

    return _jsonfinal


def replacesmartappname (_rawstring):
    _replacedstring = re.sub('[^a-zA-Z0-9]', '_', _rawstring)
    _replacedstring = re.sub('_+', '_', _replacedstring)
    return _replacedstring

# mergejson(dataA,dataB)
#
# print(replacesmartappname('Table1$ Name & id'))