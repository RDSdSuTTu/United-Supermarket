#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 11:37:54 2018

@author: rudas
"""

import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from sqlalchemy import create_engine
from bson import json_util, ObjectId
from pandas.io.json import json_normalize
from datetime import timedelta
import calendar
import json
from bson import json_util, ObjectId
from pandas.io.json import json_normalize

def connectToMongo(hostname,port):
    """
    This function estalishes connection to Mongo DB and returns connection object
    """
    mc = MongoClient(host='127.0.0.1',port=27017)
    return mc

def getCollection(database, collection, mongo_conn):
    """
    This function gets data for Store Location
    """
    db = mongo_conn.get_database(database)
    lists = db.get_collection(collection).find({},{'_id':0})
    return lists

def getSalesTrx(sDate,eDate,store,database, collection, mongo_conn):
    """
    This function gets data for Store Location
    """
    db = mongo_conn.get_database(database)
    lists = db.get_collection(collection).find({'StoreNum':store,'TransDatetime(Local)':{'$lt': eDate, '$gte': sDate}},{'_id':0})
    return lists

def convertToDF(lists):
    """
    This function converts lists of data extracted from mongo to Data Frames.
    """
    df= pd.DataFrame(list(lists))
    print("Number of rows : " + str(len(df)))
    return df

def connectMySQL(username, password, hostname, db):
    """
    This function estalishes connection to MySQL and returns connection object
    """
    #mysql+mysqlconnector://[username]:[password]@localhost/[database]
    link = 'mysql+mysqlconnector://'+username+':'+password+'@'+hostname+'/'+db
    engine = create_engine(link)
    return engine

def performHousekeeping(engine):
    """
    This function deletes all data records from the database table
    """
    list1 = ['DateDim','TimeDim','ItemListDim','ItemJunkDim','ItemHierarchyDim','StoreJunkDim',
             'StoreLocationDim','SalesJunkDim','CustomerDim','ItemAttributesDim','StoreServiceDim',
             'trans_fact']
    for i in list1:
        engine.execute('delete from '+i)

def getEmbedded(mongo_data):
    """
    This function is to convert nested documents from mongoDB to DataFrames
    """
    sanitized = json.loads(json_util.dumps(mongo_data))
    normalized = json_normalize(sanitized)
    df = pd.DataFrame(normalized)
    return df

if __name__ == '__main__':
    #Creating a mongoDB connection object
    conn_obj = connectToMongo(hostname='127.0.0.1',port=27017)

    unknowncustrecord = dict({"LoyaltyCardNum":-999,"HouseholdNum":-999,"MemberFavStore":-999,"City":'-999',"State":'-999',"ZipCode":'-999'})
    unknowncustomer = conn_obj.get_database('BIProject').get_collection('Customer').insert_one(unknowncustrecord)


    """
    Get data from Mongo
    """
    #Collect data from StoreLocation collection at mongoDB
    storeDF = convertToDF(getCollection('BIProject','StoreLocation',conn_obj))
    print(storeDF.dtypes)
    print(storeDF.isna().sum())

    #Collect data from ItemAttribute collection at mongoDB
    itemAttrDF = convertToDF(getCollection('BIProject','ItemAttribute',conn_obj))
    print(itemAttrDF.dtypes)
    print(itemAttrDF.isna().sum())

    #Collect data from ItemList collection at mongoDB
    itemListDF = convertToDF(getCollection('BIProject','ItemList',conn_obj))
    print(itemListDF.dtypes)
    print(itemListDF.isna().sum())

    #Collect data from Customer collection at mongoDB
    customerDF = convertToDF(getCollection('BIProject','Customer',conn_obj))
    print(customerDF.dtypes)
    print(customerDF.isna().sum())

    #Collect data from SalesTrx collection at mongoDB between specific transaction dates at a particular store
    start = '2014-02-22 00:00:00'
    end = '2014-02-23 00:00:00'
    store = 562
    saleDF = convertToDF(getSalesTrx(start, end,store,'BIProject','SalesTrx',conn_obj))
    salejunkDF = saleDF[['StoreNum','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType']]
    print(saleDF.dtypes)
    print(saleDF.isna().sum())

    #Extracting scraped data at mongoDB
    DF = getEmbedded(getCollection('BIProject','StoreScraped', conn_obj))

    scrapedDF = DF[['Service.Alcohol','Service.Amarillo National Bank','Service.Angus Beef',
                  'Service.Bakery','Service.Bill Pay','Service.Boars Head','Service.Bulk Foods',
                  'Service.Check Cashing','Service.City Bank','Service.Clear Talk','Service.Coffee Shop',
                  'Service.Concierge','Service.DMV Registration','Service.Deli','Service.Dish Gift Center',
                  'Service.First Financial Bank','Service.Floral','Service.Full Service Seafood','Service.Herring National Bank',
                  'Service.Hot Deli','Service.Keva Juice','Service.Living Well Dept','Service.Lottery','Service.Meals For Two','Service.Meat Market',
                  'Service.Red Box','Service.Restaurant','Service.Rug Doctor','Service.Salad Bar','Service.Sushi','Service.Team Spirit Shop','Service.Ticket Sales','Service.Walk-in Clinic',
                  'Service.Wells Fargo Bank','Service.Western Union','StoreId','StoreName']]

    scrapedDF.rename(columns={'Service.Alcohol':'Alcohol','Service.Amarillo National Bank':'AmarilloNationalBank','Service.Angus Beef':'AngusBeef',
                  'Service.Bakery':'Bakery','Service.Bill Pay':'BillPay','Service.Boars Head':'BoarsHead','Service.Bulk Foods':'BulkFoods',
                  'Service.Check Cashing':'CheckCashing','Service.City Bank':'CityBank','Service.Clear Talk':'ClearTalk','Service.Coffee Shop':'CoffeeShop',
                  'Service.Concierge':'Concierge','Service.DMV Registration':'DMVregistration','Service.Deli':'Deli','Service.Dish Gift Center':'DishGiftCenter',
                  'Service.First Financial Bank':'FirstFinancialBank','Service.Floral':'Floral','Service.Full Service Seafood':'FullServiceSeafood','Service.Herring National Bank':'HerringNationalBank',
                  'Service.Hot Deli':'HotDeli','Service.Keva Juice':'KevaJuice','Service.Living Well Dept':'LivingWellDept','Service.Lottery':'Lottery','Service.Meals For Two':'MealsForTwo','Service.Meat Market':'MeatMarket',
                  'Service.Red Box':'RedBox','Service.Restaurant':'Restaurant','Service.Rug Doctor':'RugDoctor','Service.Salad Bar':'SaladBar','Service.Sushi':'Sushi','Service.Team Spirit Shop':'TeamSpiritShop','Service.Ticket Sales':'TicketSales','Service.Walk-in Clinic':'WalkInClinic',
                  'Service.Wells Fargo Bank':'WellsFargoBank','Service.Western Union':'WesternUnion','StoreId':'StoreNum','StoreName':'StoreType'},inplace=True)

    scrapedDF.drop_duplicates(keep='first')


    """
    Create a mysql database connection
    """
    engine = connectMySQL('username', 'password', 'localhost', 'sls_tran_sch1')

    #Delete all data from all tables in the database
    performHousekeeping(engine)

    """
    Inserting data values into Dimensions of the mysql database
    """

    #Inserting values into sales junk dimension
    #Merging customerDF with saleDF to get only those customers who are in our sales transaction file
    cust_sale = pd.merge(customerDF, saleDF, left_on='LoyaltyCardNum', right_on='LoyaltyCardNumber',how = 'inner').drop_duplicates(keep='first')

    #Inserting data into CustomerDim
    CustomerDim = cust_sale[['LoyaltyCardNum','HouseholdNum','MemberFavStore','City','State','ZipCode']].drop_duplicates(keep='first').drop_duplicates(keep='first')
    CustomerDim.to_sql('CustomerDim', engine, if_exists='append', index=False)

    #Inserting data into StoreJunkDim
    storeDF[['StoreNum','StoreName','ActiveFlag','SqFoot','ClusterName']].to_sql('StoreJunkDim', engine, if_exists='append', index=False)

    #Inserting data into StoreLocationDim
    StoreLocationDim = storeDF[['Region','StateCode','City','ZipCode','AddressLine1']].drop_duplicates(keep='first')
    StoreLocationDim.to_sql('StoreLocationDim', engine, if_exists='append', index=False)

    #Merging itemListDF and saleDF to get only those items which are in our sales
    item_sale = pd.merge(itemListDF, saleDF, left_on=['UPC','ItemID'], right_on=['UPC','ItemID'], how ='inner').drop_duplicates(keep='first')

    #Inserting data into ItemListDim
    ItemListDim = item_sale[['UPC','ItemID','LongDes','ShortDes','ExtraDes']].drop_duplicates(keep='first')
    ItemListDim.to_sql('ItemListDim', engine, if_exists='append', index=False)

    #Inserting data into item hierarchy
    ItemHierarchyDim = item_sale[['DepartmentCode','FamilyCode','FamilyDes','CategoryCode','CategoryDes','ClassCode','ClassDes']].drop_duplicates(keep='last').astype(str).drop_duplicates(keep='first')
    ItemHierarchyDim.to_sql('ItemHierarchyDim', engine, if_exists='append', index=False)

    #Inserting data into ItemJunkDim
    ItemJunkDim = item_sale[['StoreBrand','Status']].drop_duplicates(keep='first').drop_duplicates(keep='first')
    ItemJunkDim.to_sql('ItemJunkDim', engine, if_exists='append', index=False)

    #Inserting into scraped StoreServicesDim
    scrapedDF.to_sql('StoreServiceDim', engine, if_exists='append', index=False)

    #Inserting into item attributes dimension
    itemattrinitem = pd.merge(item_sale, itemAttrDF, left_on=['UPC'], right_on=['UPC'], how ='inner').drop_duplicates(keep='first')
    ItemattributesDim = itemattrinitem[['UPC','ItemAttributeValue','ItemAttributeDes','AttributeStartDate','AttributeEndDate']]
    ItemattributesDim.to_sql('ItemattributesDim', engine, if_exists='append', index=False)

    #Inserting into ItemBridge table
    itemdim = pd.read_sql_table('itemlistdim', engine, columns=['ILDK', 'UPC'])
    itemattrdim = pd.read_sql_table('itemattributesdim', engine, columns=['IADK', 'UPC'])
    itembridgedf = pd.merge(itemdim, itemattrdim, left_on=['UPC'], right_on=['UPC'], how ='inner').drop_duplicates(keep='first')
    Itembridge = itembridgedf[['IADK','ILDK']]
    Itembridge.rename(index = str, columns={'IADK':'ItemAttribute_IADK','ILDK':'ItemList_ILDK'})
    ItemattributesDim.to_sql('ItemattributesDim', engine, if_exists='append', index=False)

    # Date Dimension
    temp = pd.DatetimeIndex(saleDF['TransDatetime(GMT)'])
    saleDF['Year_int'] = temp.year
    saleDF['Month_int'] = temp.month
    saleDF['Month_abbr'] = saleDF['Month_int'].apply(lambda x: calendar.month_abbr[x])
    saleDF['Day_int'] = temp.day
    saleDF['DayOfWeek_int'] = temp.dayofweek
    saleDF['DayOfWeek_char'] = saleDF['DayOfWeek_int'].apply(lambda x: calendar.day_name[x])
    saleDF['DayOfYear_int'] = temp.dayofyear
    saleDF['Date'] = saleDF['TransDatetime(GMT)']
    datedim = saleDF[['TransDatetime(GMT)','Date','Year_int','Month_int','Month_abbr','Day_int','DayOfWeek_int','DayOfWeek_char','DayOfYear_int']].drop_duplicates(keep='first')
    datedim[['Date','Year_int','Month_int','Month_abbr','Day_int','DayOfWeek_int','DayOfWeek_char',
            'DayOfYear_int']].to_sql('DateDim', engine, if_exists='append',index=False)

    #Time Dimension
    saleDF['Time_hhmmss_char'] = temp.time.astype(str)
    saleDF['Hour_24_int'] = temp.hour
    saleDF['Time'] =saleDF['Time_hhmmss_char']
    saleDF['Minute_int'] = temp.minute
    saleDF['Second_int'] = temp.second
    temp_12hour = saleDF['TransDatetime(GMT)'] + timedelta(hours=12)
    saleDF['Hour_12_int'] = pd.DatetimeIndex(temp_12hour).hour
    saleDF['AM_PM_char'] = saleDF['Hour_12_int']
    i=0
    ampm = []
    for i in list(range(0,24)):
        if(i< 12):
            ampm.append('AM')
            i=i+1
        elif(i>=12):
            ampm.append('PM')
            i=i+1
    mapping = dict(zip(list(range(0,24)),ampm))
    saleDF.replace({'AM_PM_char': mapping})
    #print(saleDF)

    timedim = saleDF[['TransDatetime(GMT)','Time','Hour_24_int','Minute_int','Second_int','Hour_12_int','AM_PM_char']].drop_duplicates(keep='first')

    timedim.rename(index = str,columns ={'Time':'Time_hhmmss_char','Hour_24_int':'Hour_24_int','Minute_int':'Minute_int','Second_int':'Second_int','Hour_12_int':'Hour_12_int','AM_PM_char':'AM_PM_char'})
    timedim[['Time','Hour_24_int','Minute_int','Second_int','Hour_12_int','AM_PM_char']].to_sql('TimeDim', engine, if_exists='append', index=False)

    #Sales Junk Dimension records are inserted
    salejunkdimtable = salejunkDF[['Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType']].drop_duplicates(keep='first')
    salejunkdimtable.to_sql('salesjunkdim', engine, if_exists='append', index=False)


    #Inserting FACT records in FACT table for sales transaction data

    salesfact = saleDF[['UPC','ItemID','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType']]

    salesjunktable = pd.read_sql_table('salesjunkdim', engine)
    salesjunkfact = pd.merge(salejunkDF[['StoreNum','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType']], salesjunktable, left_on=['Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType'], right_on=['Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType'], how ='inner').drop_duplicates(keep='first')

    salesfact =    pd.merge(salesfact[['UPC','ItemID','TransDatetime(GMT)','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType','StoreNum']], salesjunkfact[['SJDK','StoreNum','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType']], left_on=['StoreNum','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType'], right_on=['StoreNum','Register','DeptNum','CashierNum','PriceType','ServiceType','TenderType'], how ='inner').drop_duplicates(keep='first')

    itemfact = pd.read_sql_table('itemlistdim', engine, columns=['ILDK', 'UPC','ItemID'])
    itemsalesfact = pd.merge(itemfact, salesfact, left_on=['UPC','ItemID'], right_on=['UPC','ItemID'], how ='inner').drop_duplicates(keep='first')

    datefact = pd.read_sql_table('datedim', engine, columns=['DDK','Date'])
    itemsalesdatefact = pd.merge(itemsalesfact, datefact, left_on=['TransDatetime(GMT)'], right_on=['Date'], how ='inner').drop_duplicates(keep='first')
    itemsalesdatefact['time'] = pd.DatetimeIndex(itemsalesdatefact['Date']).time

    timefact = pd.read_sql_table('timedim', engine, columns=['TDK','Time'])
    itemsalesdatetimefact = pd.merge(itemsalesdatefact, timefact, left_on=['time'], right_on=['Time'], how ='inner').drop_duplicates(keep='first')

    itemsaledtfact = itemsalesdatetimefact

    itemjunktable = pd.read_sql_table('itemjunkdim', engine, columns=['IJDK','StoreBrand','Status'])
    itemjunk = pd.merge(itemjunktable, item_sale[['StoreBrand','Status','UPC','ItemID','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], left_on=['StoreBrand','Status'], right_on=['StoreBrand','Status'], how ='inner').drop_duplicates(keep='first')
    itemjunkfact = pd.merge(itemsaledtfact, itemjunk, left_on=['UPC','ItemID'], right_on=['UPC','ItemID'], how ='inner').drop_duplicates(keep='first')

    allitemsalesdtfact=itemjunkfact
    itemtemp_sale=item_sale
    allitemsalesdtfact['sales_SJDK'] = allitemsalesdtfact['SJDK']


    itemtemp_sale['ClassCode']=itemtemp_sale['ClassCode'].astype(str)
    itemtemp_sale['CategoryCode']=itemtemp_sale['CategoryCode'].astype(str)
    itemtemp_sale['DepartmentCode']=itemtemp_sale['DepartmentCode'].astype(str)
    itemtemp_sale['FamilyCode']=itemtemp_sale['FamilyCode'].astype(str)
    allitemsalesdtfact['ClassCode']=allitemsalesdtfact['ClassCode'].astype(str)
    allitemsalesdtfact['CategoryCode']=allitemsalesdtfact['CategoryCode'].astype(str)
    allitemsalesdtfact['DepartmentCode']=allitemsalesdtfact['DepartmentCode'].astype(str)
    allitemsalesdtfact['FamilyCode']=allitemsalesdtfact['FamilyCode'].astype(str)

    itemhiertable = pd.read_sql_table('itemhierarchydim', engine, columns=['IHDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode'])

    itemhiertemp = pd.merge(itemhiertable, itemtemp_sale, left_on=['ClassCode','CategoryCode','DepartmentCode','FamilyCode'], right_on=['ClassCode','CategoryCode','DepartmentCode','FamilyCode'], how ='inner').drop_duplicates(keep='first')
    itemhierfact = pd.merge(allitemsalesdtfact[['sales_SJDK','ILDK','UPC','ItemID','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], itemhiertemp[['IHDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], left_on=['ClassCode','CategoryCode','DepartmentCode','FamilyCode'], right_on=['ClassCode','CategoryCode','DepartmentCode','FamilyCode'], how ='inner').drop_duplicates(keep='first')

    halffact = itemhierfact

    storetable = pd.read_sql_table('storelocationdim', engine, columns=['SLDK','Region','StateCode','City','ZipCode','AddressLine1'])
    storetablefact = pd.merge(storeDF[['StoreName','ClusterName','StoreNum','Region','StateCode','City','ZipCode','AddressLine1']],storetable[['SLDK','Region','StateCode','City','ZipCode','AddressLine1']],left_on=['Region','StateCode','City','ZipCode','AddressLine1'] ,right_on=['Region','StateCode','City','ZipCode','AddressLine1'],how='inner').drop_duplicates(keep='first')
    storefact = pd.merge(halffact[['sales_SJDK','IHDK','ILDK','UPC','ItemID','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], storetablefact[['StoreName','ClusterName','StoreNum','SLDK','Region','StateCode','City','ZipCode','AddressLine1']], left_on=['StoreNum'], right_on=['StoreNum'], how ='inner').drop_duplicates(keep='first')
    storejunktable = pd.read_sql_table('storejunkdim', engine, columns=['SJDK','StoreName','ClusterName','StoreNum','ActiveFlag','SqFoot'])

    storejunkfact = pd.merge(storefact[['sales_SJDK','SLDK','IHDK','ILDK','UPC','ItemID','ClusterName','StoreName','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], storejunktable[['StoreName','ClusterName','StoreNum','SJDK','ActiveFlag','SqFoot']], left_on=['StoreName','ClusterName','StoreNum'], right_on=['StoreName','ClusterName','StoreNum'], how ='inner').drop_duplicates(keep='first')

    storeservicetable = pd.read_sql_table('storeservicedim', engine)
    prefinalfact = pd.merge(storejunkfact, storeservicetable, left_on=['StoreNum'], right_on=['StoreNum'], how ='inner').drop_duplicates(keep='first')

    prefinalfact= prefinalfact[['SSDK','SJDK','sales_SJDK','SLDK','IHDK','ILDK','UPC','ItemID','ClusterName','StoreName','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']]
    customertable = pd.read_sql_table('customerdim', engine)
    customerfact = pd.merge(saleDF[['LoyaltyCardNumber']], customertable, left_on=['LoyaltyCardNumber'], right_on=['LoyaltyCardNum'], how ='inner').drop_duplicates(keep='first')

    prefinalfact = pd.merge(saleDF[['TransNum','LoyaltyCardNumber']], prefinalfact[['SSDK','sales_SJDK','SJDK','SLDK','IHDK','ILDK','UPC','ItemID','ClusterName','StoreName','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], left_on=['TransNum'], right_on=['TransNum'], how ='inner').drop_duplicates(keep='first')

    finalfact =  pd.merge(prefinalfact[['SSDK','LoyaltyCardNumber','sales_SJDK','SJDK','SLDK','IHDK','ILDK','UPC','ItemID','ClusterName','StoreName','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']], customerfact[['CDK','LoyaltyCardNum']], left_on=['LoyaltyCardNumber'], right_on=['LoyaltyCardNum'], how ='inner').drop_duplicates(keep='first')

    fact = finalfact[['SSDK','CDK','sales_SJDK','SJDK','SLDK','IHDK','ILDK','UPC','ItemID','ClusterName','StoreName','TransDatetime(GMT)','StoreNum','WeightAmt','SalesAmt','BusDate','TransNum','ItemQuantity','CostAmt','DDK','Date','TDK','IJDK','ClassCode','CategoryCode','DepartmentCode','FamilyCode']]

    fact.rename(index = str, columns={'CustomerDim_CDK':'CDK','SalesJunkDim_SJDK':'sales_SJDK','StoreJunkDim_SJDK':'SJDK','StoreServiceDim_SSDK':'SSDK','StoreLocationDim_SLDK':'SLDK','ItemHierarchyDim_IHDK':'IHDK','ItemJunkDim_IJDK':'IJDK','ItemListDim_ILDK':'ILDK','TimeDim_TDK':'TDK','DateDim_DDK':'DDK'})

    fact[['CustomerDim_CDK']]=fact[['CDK']]
    fact[['SalesJunkDim_SJDK']]=fact[['sales_SJDK']]
    fact[['StoreJunkDim_SJDK']]=fact[['SJDK']]
    fact[['StoreServiceDim_SSDK']]=fact[['SSDK']]
    fact[['StoreLocationDim_SLDK']]=fact[['SLDK']]
    fact[['ItemHierarchyDim_IHDK']]=fact[['IHDK']]
    fact[['ItemJunkDim_IJDK']]=fact[['IJDK']]
    fact[['ItemListDim_ILDK']]=fact[['ILDK']]
    fact[['TimeDim_TDK']]=fact[['TDK']]
    fact[['DateDim_DDK']]=fact[['DDK']]
    fact[['WeightAmt']] = fact[['WeightAmt']].round(1)

    fact = fact[['CustomerDim_CDK','SalesJunkDim_SJDK','StoreJunkDim_SJDK','StoreServiceDim_SSDK','StoreLocationDim_SLDK','ItemHierarchyDim_IHDK','ItemJunkDim_IJDK','ItemListDim_ILDK','TimeDim_TDK','DateDim_DDK','BusDate','TransNum','ItemQuantity','WeightAmt','SalesAmt','CostAmt']]

    fact.to_sql('trans_fact', engine, if_exists='append', index=False)
