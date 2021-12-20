import json

#dosyayi acar
f = open('data.json', 'r+')

#data degiskenine dosya icerigini aktarir
data = json.load(f)

#pizzalarin un ozelliklerini bastirir
for i in data['pizzas']:
    print(i["flour"])

#dosyayi kapatir
f.close()

#dosyayi overwrite yapmak icin acar
f = open('data.json', 'wt')

#margarita pizzasinin un miktarini gunceller
for i in data['pizzas']:
    if i['name'] == 'margarita':
        i['flour'] = 'gr:300'

#dosyayi gunceller
json.dump(data, f, indent = 2)

#un miktarlarini bastirir
for i in data['pizzas']:
    print(i["flour"])

#dosyayi kapatir
f.close()