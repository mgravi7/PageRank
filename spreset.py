# CAPSTONE project for Coursera "Python for Everybody"
# 
# spreset.py
#
# All pages are reset to new rank of 1.0 and old rank of

# imports
import sqlite3

dbConn = sqlite3.connect('spider.sqlite')
cur = dbConn.cursor()

cur.execute('''
UPDATE Pages
SET new_rank=1.0, old_rank=0.0''')

dbConn.close()
print "All pages set to a new rank of 1.0"