# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 12:54:18 2018

@author: rudas
"""

import json
import csv
with open('scraping_2.csv', 'w') as csvfile:
    fieldnames = [""]
    writer = csv.writer(csvfile,delimiter=',', quoting=csv.QUOTE_MINIMAL)

    #writer.writeheader()
    #writer.writerow({'first_name': 'Baked', 'last_name': 'Beans'})
    
    
    filepath = 'scraping_2.txt'  
    with open(filepath) as fp:  
       line = fp.readline()
       cnt = 1
       while line:
    #       print("Line {}: {}".format(cnt, line.strip()))
           line = fp.readline()
           cnt += 1
           if "var stores" in line:
               line_v = line
    
    var = line_v.find('=')
    print(var)  
    
    var_2 = line_v.find('];')
    print(var_2)  
    
    json_data = line_v[var + 1 : var_2 + 1 ]
    #print(json_data)
    data=json.loads(json_data)
    
    for d in data:
        #print ("StoreName "+":"+d['StoreName']),
        #print ("StoreID "+":"+str(d['StoreID'])),
        #print ("LocationName "+":"+d['LocationName']),
        #print ("State "+":"+d['State']),
        #print ("Zipcode "+":"+d['Zipcode']),
        services=json.loads(data[0]['Services'])
        for s in services['Services']:
            #print("StoreName "+":"+d['StoreName'],+","+ "  StoreID "+":"+str(d['StoreID']), "LocationName "+":"+d['LocationName'],+","+ "  State "+":"+d['State'],+","+ "  Zipcode "+":"+d['Zipcode']+"Service Name "+":"+s['Name']+"Service Value "+":"+s['Value'])
            print("StoreName "+":"+d['StoreName']+","+" StoreID "+":"+str(d['StoreID'])+","+"LocationName "+":"+d['LocationName']+","+" State "+":"+d['State']+","+" Zipcode "+":"+d['Zipcode']+","+"Service Name "+":"+s['Name']+","+"Service Value "+":"+s['Value'])
            writer.writerow(["StoreName "+":"+d['StoreName']," StoreID "+":"+str(d['StoreID']),"LocationName "+":"+d['LocationName']," State "+":"+d['State']," Zipcode "+":"+d['Zipcode'],"Service Name "+":"+s['Name'],"Service Value "+":"+s['Value']])
            #print ("Service Name "+":"+s['Name']),
            #print ("Service Value "+":"+s['Value']),
        #print ()
        
