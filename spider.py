# CAPSTONE project for Coursera "Python for Everybody"
# 
# spider.py
#
# Crawls web sites and updates the database with links found on the site

# imports
import sqlite3
import urllib
import ssl
import sys

from urlparse import urljoin
from urlparse import urlparse
from bs4 import BeautifulSoup

# CONSTANTS
ERROR_NON_TEXT_HTML = -1
ERROR_UNKNOWN_HTTP_EXCEPTION = -2
ERROR_HTML_PARSE = -3

# SETUP DATABASE
# creates the necessary tables
#	dbFileName - Database file name. 
def SetupDatabase(dbFileName):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()

	cur.execute('''
	CREATE TABLE IF NOT EXISTS Pages (
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		url TEXT UNIQUE,
		html TEXT,
		error INTEGER,
		new_rank REAL,
		old_rank REAL)''')
		
	cur.execute('''
	CREATE TABLE IF NOT EXISTS Links (
		from_id INTEGER,
		to_id INTEGER)''')
		
	cur.execute('''
	CREATE TABLE IF NOT EXISTS Websites (
		url TEXT UNIQUE)''')
		
	dbConn.commit()
	dbConn.close()
	
	
# GET START URL INTO DB
# ensures a starting URL (for crawling) is available in the database
#	dbFileName - Database file name. 
def GetStartURLIntoDB(dbFileName):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()
		
	# is there an existing crawl?
	cur.execute('''
	SELECT id, url 
	FROM Pages 
	WHERE html is NULL AND error is NULL 
	ORDER BY RANDOM() LIMIT 1''')

	row = cur.fetchone()
	
	if row is None:	# nothing to crawl
		# get the web URL
		startURL = raw_input('Enter Web URL to crawl or hit Enter for default Web URL: ')
		if (len(startURL) < 1): startURL = 'http://python-data.dr-chuck.net/'
		if (startURL.endswith('/')): startURL = startURL[:-1]
		
		webURL = ''
		if (startURL.endswith('.htm') or
			startURL.endswith('.html')):
			pos = startURL.rfind('/')
			if (pos > 0): webURL = startURL[:pos]
			if (len(webURL) < 1):
				print 'Web URL is not of proper length. Quitting.'
				sys.exit()
		else:
			webURL = startURL
			
		# update database
		cur.execute('''
		INSERT OR IGNORE INTO Websites 
		(url) 
		VALUES (?)''',
		(webURL,))
		
		cur.execute('''
		INSERT OR IGNORE INTO Pages 
		(url, html, error, new_rank, old_rank) 
		VALUES (?, NULL, NULL, 1.0, 0.0)''',
		(startURL,))
		dbConn.commit()
		
	dbConn.close()	
	return None
	
# GET ALL WEB SITES
# gets all the web sites from the database
#	dbFileName - Database file name.
def GetAllWebSites(dbFileName):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()
		
	cur.execute('''
	SELECT url 
	FROM Websites ''')
	webSites = list()
	for row in cur:
		webSites.append(str(row[0]))
	
	dbConn.close()
	return webSites

# IS HREF OF INTEREST
#	href - href node
#	webSites - list of Website names. Links outside these websites will be ignored.
def IsHrefOfInterest(href, webSites):
	# skip image links
	if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')): return False
	
	if (href.endswith('/')): href = href[:-1]
	if (len(href) < 1): return False
	
	# does the link belong to any of our websites
	for webSite in webSites:
		if (href.startswith(webSite)): return True
	
	# external link
	return False
	
# CREATE FULLY FORMED HREF
#	href - href node
#	url - url of the page where the href is located
def CreateFullyQualifiedHref(href, url):
	# resolve relative differences like href="/contact"
	up = urlparse(href)
	if (len(up.scheme) < 1):
		href = urljoin(url, href)
		
	# need to strip within page reference (#xyz for example)
	hashPos = href.find('#')
	if (hashPos > 1): href = href[:hashPos]
	
	return href
	
# UPDATE LINKS
#	html - HTML to be parsed for links
#	cur - cursor for the database connection
#	webSites - list of Website names. Links outside these websites will be ignored.
def UpdateLinksForHTML(html, cur, urlId, url, webSites):
	# parse HTML
	try:
		soup = BeautifulSoup(html, "html.parser")
	except:
		print 'HTML Parsing error in calling BeautifulSoup'
		return ERROR_HTML_PARSE
		
	# examine the anchor tags
	tags = soup('a')
	skippedLinks = 0
	foundLinks = 0
	for tag in tags:
		href = tag.get('href', None)
		if (href is None): continue
		
		# is this href of interest?
		href = CreateFullyQualifiedHref(href, url)
		if (IsHrefOfInterest(href, webSites) == False):
			skippedLinks += 1
			continue
			
		# need to update database
		foundLinks += 1
		cur.execute('''
		INSERT OR IGNORE 
		INTO Pages 
		(url, html, error, new_rank, old_rank) 
		VALUES (?, NULL, NULL, 1.0, 0.0)''',
		(href,))
		
		# get the id that was inserted
		cur.execute('''
		SELECT id 
		FROM Pages 
		WHERE url=? 
		LIMIT 1''',
		(href,))
		
		row = cur.fetchone()
		if (row is None):
			print '*** Could not retrieve id for inserted url:', href
			continue
		
		# update the links table
		toId = row[0]
		cur.execute('''
		INSERT OR IGNORE 
		INTO Links 
		(from_id, to_id) 
		VALUES (?, ?)''',
		(urlId, toId))	
			
	print '   Inserted links:', foundLinks, ', Skipped links:', skippedLinks	
	return None
	
# PERFORM CRAWL
# crawls one URL and updates the database
#	webSites - list of Website names. Links outside these websites will be ignored.
def PerformCrawl(webSites):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()
		
	# is there any page to crawl?
	cur.execute('''
	SELECT id, url 
	FROM Pages 
	WHERE html is NULL AND error is NULL 
	ORDER BY RANDOM() LIMIT 1''')

	row = cur.fetchone()
	if row is None:
		print 'No unretrieved HTMP pages found'
		return False
		
	# delete the links for this page (safe to do)
	urlId = row[0]
	url = row[1]
	errorCode = None
	cur.execute('''
	DELETE FROM Links 
	WHERE from_id=?''',
	(urlId,))
	
	# retrieve page
	try:
		document = urllib.urlopen(url)
		html = document.read()
		
		if (document.getcode() != 200):
			errorCode = document.getcode()
			print 'Error in getting:', url, 'HTML error code:', errorCode
		elif (document.info().gettype() != 'text/html'):
			errorCode = ERROR_NON_TEXT_HTML
			print 'Ignoring:', url, 'since the content type is:', document.info().gettype()
		else:
			print 'Get URL:', url, 'size:', len(html)
	except KeyboardInterrupt:
		print ' '
		print 'Crawl interrupted by the user'
		dbConn.close()	# all the work is abandoned
		return False
	except:
		errorCode = ERROR_UNKNOWN_HTTP_EXCEPTION	# we won't be crawling this page again
		print 'Unable to retrieve:', url
		
	# parse if there is no error
	if (errorCode is None): errorCode = UpdateLinksForHTML(html, cur, urlId, url, webSites)
	
	# update Pages table
	if (errorCode is None):
		cur.execute('''
		UPDATE Pages 
		SET html=? 
		WHERE id=? ''',
		(buffer(html), urlId))
	else:
		cur.execute('''
		UPDATE Pages 
		SET error=? 
		WHERE id=? ''',
		(errorCode, urlId))
		
	# phew! all done:-) YAY!!!
	dbConn.commit()
	dbConn.close()
	return True
	

# MAIN
# setup database (if needed)
dbFileName = 'spider.sqlite'
SetupDatabase(dbFileName)
	
# make sure web URL for crawling is available
GetStartURLIntoDB(dbFileName)

# load all the Websites
webSites = GetAllWebSites(dbFileName)

# let us do the crawling one page at a time
numPagesToCrawl = raw_input('Enter number of pages to crawl: ')
if (len(numPagesToCrawl) < 1): sys.exit()
numPagesToCrawl = int(numPagesToCrawl)

while (numPagesToCrawl > 0):
	if (PerformCrawl(webSites) == False): break
	numPagesToCrawl -= 1
