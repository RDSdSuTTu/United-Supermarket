# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 13:06:55 2018

@author: rudas
"""

from selenium import webdriver
from time import sleep

browser = webdriver.Chrome("D:/WEBDRIVER/chromedriver_win32/chromedriver.exe")
url = 'https://www.amigosunited.com/rs/StoreLocator'
abc = browser.get(url)
sleep(15)

browser.find_element_by_id('main-body').click()

a = browser.page_source

ab = open("scraping_3.txt", 'w')
ab.write(a)
ab.close()
browser.close()
