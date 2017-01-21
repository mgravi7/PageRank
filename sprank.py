# CAPSTONE project for Coursera "Python for Everybody"
# 
# sprank.py
#
# Updates the ranking based on incoming links

# imports
import sqlite3

# FIND LINKAGES
#	dbFileName - database file name
#	fromIDs - list of valid fromIDs
#	toIDs - list of valid toIDs that have linkage from fromIDs
#	links - list capturing the from_id to to_id relationship
def FindLinkages(dbFileName, fromIDs, toIDs, links):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()

	# all from_ids are valid for the list
	cur.execute('''
	SELECT DISTINCT from_id
	FROM Links''')
	for row in cur:
		fromIDs.append(row[0])
		
	# find the valid ones with relationships
	cur.execute('''
	SELECT DISTINCT from_id, to_id
	FROM Links''')
	for row in cur:
		fromID = row[0]
		toID = row[1]
		
		# exclusions
		if (fromID == toID): continue			# refers to self
		if (fromID not in fromIDs): continue	# unlikely!
		if (toID not in fromIDs): continue		# contains no reference to others
		
		links.append(row)
		if (toID not in toIDs): toIDs.append(toID)
	
	dbConn.close()	
	print 'From IDs:', len(fromIDs), ', To IDs:', len(toIDs), ', Links:', len(links)
	return
	
# FIND RANKS
#	dbFileName - database file name
#	fromIDs - list of valid fromIDs
#	lastRanks - latest ranks for the URLs in fromIDs. Dictionary<fromID, rank>
def FindRanks(dbFileName, fromIDs, lastRanks):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()

	for fromID in fromIDs:
		cur.execute('''
		SELECT new_rank
		FROM Pages
		WHERE id=?''',
		(fromID,))
		
		row = cur.fetchone()
		lastRanks[fromID] = row[0]
	return

# COMPUTE NEXT RANKS
# computes next ranks based on last ranks
#	lastRanks - last rank Dictionary<urlID, oldRank>
#	toIDs - list of valid toIDs that have linkage from fromIDs
#	links - fromID, toID relationship
#	nextRanks - next rank Dictionary<urlID, newRank> - will be updated
def ComputeNextRanks(lastRanks, toIDs, links, nextRanks):
	# compute lastTotal and initialize the nextRanks
	lastTotal = 0.0
	for (id, lastRank) in lastRanks.items():
		lastTotal += lastRank
		nextRanks[id] = 0.0
		
	# find the number of outbound links and send the page rank down
	for (id, lastRank) in lastRanks.items():
		giveIDs = list()
		for (fromID, toID) in links:
			if (fromID != id): continue
			if (toID not in toIDs): continue
			giveIDs.append(toID)
		if (len(giveIDs) < 1): continue
		
		amount = lastRank / len(giveIDs)
		for giveID in giveIDs:
			#print 'giveID: ', giveID
			nextRanks[giveID] = nextRanks[giveID] + amount
			
	# next total
	nextTotal = 0.0
	for (id, nextRank) in nextRanks.items():
		nextTotal += nextRank
	evap = (lastTotal - nextTotal) / len(nextRanks)
	
	# update based on evap
	for id in nextRanks:
		nextRanks[id] = nextRanks[id] + evap
	
	# done!
	return
	
# UPDATE RANKS
# (1) Update the old_rank with new_rank values in the database
# (2) Update the new_rank column with newRanks dictionary object values
#	dbFileName - database file name
#	newRanks - new rank Dictionary<urlID, newRank>
def UpdateRanks(dbFileName, newRanks):
	dbConn = sqlite3.connect(dbFileName)
	cur = dbConn.cursor()
	
	# step 1
	cur.execute('''
	UPDATE Pages
	SET old_rank = new_rank''')

	# step 2
	for (urlId, newRank) in newRanks.items():
		cur.execute('''
		UPDATE Pages
		SET new_rank=?
		WHERE id=?''',
		(newRank, urlId))
		
	dbConn.commit()
	dbConn.close()
	return
	
# MAIN
#
# find the linkages
dbFileName = 'spider.sqlite'
fromIDs = list()
toIDs = list()
links = list() # links fromIds to toIDs
FindLinkages(dbFileName, fromIDs, toIDs, links)

# get the ranks
lastRanks = dict()
FindRanks(dbFileName, fromIDs, lastRanks)
if (len(lastRanks) < 1):
	print 'There is nothing to rank. Have you run spider.py to crawl a website?'
	sys.exit()
else:
	print 'Found', len(lastRanks), 'pages to rank'
	
# how many iterations?
numIterations = 1
userInput = raw_input('Enter number of iterations for page ranking algorithm: ')
if (len(userInput) > 0): numIterations = int(userInput)

for idx in range(numIterations):
	# get the next ranks computed
	nextRanks = dict()
	ComputeNextRanks(lastRanks, toIDs, links, nextRanks)
	
	# what is the change?
	totalDiff = 0.0
	for (id, oldRank) in lastRanks.items():
		newRank = nextRanks[id]
		totalDiff += abs(oldRank - newRank)
	print 'Iteration:', idx+1, ', Average Difference:', totalDiff/len(lastRanks)
	
	# next iteration setup
	lastRanks = nextRanks
	
# update the database
UpdateRanks(dbFileName, nextRanks)
print 'Database updated with new ranks!'		