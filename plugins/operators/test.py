abc = {}

abc['region'] = ['us-east-1']
abc['number'] = [434]

region = 'bom'
number = 123 

abc['region'].append(region)
abc['number'].append(number)

# print(str(abc['region'][1]) + ' ' + str(abc['number'][1]))

keyslist = list(abc.keys())
print(len(keyslist))

print(str(abc))