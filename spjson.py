# CAPSTONE project for Coursera "Python for Everybody"
# 
# spjson.py
#
# creates json file for visualization with D3 and Force

# imports
import sqlite3

# CONSTANTS
NODES_NUM_INBOUNDLINKS_IDX = 0
NODES_OLD_RANK_IDX = 1
NODES_NEW_RANK_IDX = 2
NODES_ID_IDX = 3
NODES_URL_IDX = 4

LINKS_FROM_ID_IDX = 0
LINKS_TO_ID_IDX = 1

# CREATE NODES FROM DATABASE
#	dbFileName - name of the database file
#	numNodes - number of nodes to create (this is the max)
def CreateNodesFromDatabase(dbFileName, numNodes):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()

	cur.execute('''
	SELECT COUNT(from_id) as num_inbound_links, old_rank, new_rank, id, url
	FROM Pages
	JOIN Links
	ON Pages.id = Links.to_id
	WHERE html IS NOT NULL AND
		  ERROR IS NULL
	GROUP BY id
	ORDER BY num_inbound_links DESC''')
	
	nodes = list()
	for row in cur:
		nodes.append(row)
		if (len(nodes) >= numNodes): break
		
	dbConn.close()
	print 'Number of nodes created:', len(nodes)
	return nodes
	
# CREATE LINKS FROM DATABASE
#	dbFileName - name of the database file
def CreateLinksFromDatabase(dbFileName):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()

	cur.execute('''
	SELECT DISTINCT from_id, to_id
	FROM Links''')
	
	links = list()
	for row in cur:
		links.append(row)
		
	dbConn.close()
	print 'Number of links created:', len(links)
	return links

# WRITE NODES AND LINKS TO JSON FILE
#	nodes - list() in the order of num_inbound_links, old_rank, new_rank, id, url
#	links - list() in the order of from_id, to_id
#	fh - file handle to write json to
def WriteNodesAndLinksToJsonFile(nodes, links, fh):
	# find the max and min rank
	maxRank = None
	minRank = None
	for row in nodes:
		rank = row[NODES_NEW_RANK_IDX]
		if (maxRank is None or rank > maxRank): maxRank = rank
		if (minRank is None or rank < minRank): minRank = rank
		
	if (minRank is None or maxRank is None or maxRank == minRank):
		print 'Have you run sprank.py to compute page rank?'
		sys.exit()
		
	# nodes first
	fh.write('spiderJson = {"nodes":[\n')
	count = 0
	map = dict()
	ranks = dict()
	for row in nodes:
		if (count > 0): fh.write(',\n')
		
		id = row[NODES_ID_IDX]
		numInboundLinks = row[NODES_NUM_INBOUNDLINKS_IDX]
		rank = row[NODES_NEW_RANK_IDX]
		url = row[NODES_URL_IDX]
		
		rank = 19 * ((rank - minRank)/(maxRank - minRank))
		fh.write('{' + '"weight":' + str(numInboundLinks) + ', "rank":' + str(rank) + ',')
		fh.write(' "id":' + str(id) + ', "url":"' + url + '"}')
		
		map[id] = count
		ranks[id] = rank
		count += 1
	fh.write('],\n')
	
	# links next
	fh.write('"links":[\n')
	count = 0
	for row in links:
		# is this link of interest?
		fromID = row[LINKS_FROM_ID_IDX]
		toID = row[LINKS_TO_ID_IDX]
		if (fromID not in map or toID not in map): continue
		
		if (count > 0): fh.write(',\n')
		fh.write('{"source":' + str(map[fromID]) + ', "target":' + str(map[toID]) + ', "value":3}')
		count += 1
	fh.write(']\n};')
	return
	
# MAIN
# read the nodes
numNodes = int(raw_input("Enter number of nodes: "))
dbFileName = 'spider.sqlite'
nodes = CreateNodesFromDatabase(dbFileName, numNodes)
links = CreateLinksFromDatabase(dbFileName)

# write nodes to JSON file
jsonFileHandle = open('spider.js', 'w')
WriteNodesAndLinksToJsonFile(nodes, links, jsonFileHandle)
jsonFileHandle.close()

print "Open force.html in a browser to view the visualization"
