# CAPSTONE project for Coursera "Python for Everybody"
# 
# spdump.py
#
# Dumps the top pages based on incoming links

# imports
import sqlite3

dbConn = sqlite3.connect('spider.sqlite')
cur = dbConn.cursor()

cur.execute('''
SELECT COUNT(from_id) as num_inbound_links, old_rank, new_rank, id, url
FROM Pages
JOIN Links
ON Pages.id = Links.to_id
WHERE html IS NOT NULL
GROUP BY id
ORDER BY num_inbound_links DESC''')

count = 0
for row in cur:
	if (count < 50): print row
	count += 1
	
print 'Total rows:', count
dbConn.close()