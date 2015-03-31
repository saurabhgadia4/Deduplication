#always avoid passing object. It introduces dependencies in class with some other class.
#so can't acheive generalization and reusage


import threading
import Queue
import time
from dedupParam import *
from jaccardSim import *
import sets
import time


class genShingle(threading.Thread):
	def __init__(self, dedupObj, gram):
		threading.Thread.__init__(self)
		self.gram = gram
		self.ulock = threading.Lock()
		self.dedupObj = dedupObj
		self.totalRecords = 0
		self.totalShingleCount = 0
		self.totalShingleLen = 0

	def run(self):
		#create threads for hadnling segment task
		threadPool = []
		for i in range(NUM_REC_THREAD):
			t = threading.Thread(target = self.recWorker)
			threadPool.append(t)
			t.daemon = True
			t.start()

		self.dedupObj.segDoneEvent.wait()
		print 'got event from segment'
		self.dedupObj.recTaskQueue.join()
		print ' rec taskq freed'
		jaccSimObj = jaccSim(self.dedupObj)
		jaccSimObj.process()
		self.getStats()
		#create threads for handling record task

	#record = {'segno':segnum, 'recno':recno, 'url':url, 'parse_list':parseList}
	def createShingle(self):
		shingleCount = 0
		totalShingleLen = 0
		localRecIndex = {} #dict = {Recno:set(shingle_indexes)}
		print 'in createShingle'
		while True:
			if(self.dedupObj.recTaskQueue.empty() and localRecIndex):
				self.ulock.acquire()
				try:
					#update global dictionaries
					self.updateData(localRecIndex)
					#inittialize local to start new round
					localRecIndex = {} #dict = {Recno:set(shingle_indexes)}
				finally:
					self.ulock.release()

			record = self.dedupObj.recTaskQueue.get()
			for i in range(len(record['parse_list'])-self.dedupObj.kgram+1):
				shingle = ''
				itr = i
				for j in range(self.gram):
					shingle += record['parse_list'][itr]
					itr+=1
				
				#updating localRecIndex dictionary
				rec_id = record['recno']
				seg_id = record['segnum']
				rid = str(seg_id) + '-' + str(rec_id)
				try:
					localRecIndex[rid][SET_FIELD].add(shingle)
				except KeyError:
					newSet = sets.Set()
					newSet.add(shingle)
					localRecIndex[rid] = [record['url'], newSet]

			if(self.ulock.acquire(False)):
				try:
					#update global dictionaries
					self.updateData(localRecIndex)
					#inittialize local to start new round
					localRecIndex = {} #dict = {Recno:set(shingle_indexes)}
				finally:
					self.ulock.release()
			self.dedupObj.recTaskQueue.task_done()


	def updateData(self, localRecIndex):
		#update shingle_index
		for rid in localRecIndex:
			record = localRecIndex[rid]
			shingle_set = record[SET_FIELD]
			recset = set()
			for shingle in shingle_set:
				#adding shingle:index in g_shingle_index
				try:
					shid = self.dedupObj.g_shingle_index[shingle]
				except KeyError:
					self.dedupObj.g_shingle_index[shingle] = self.dedupObj.shid
					shid = self.dedupObj.shid

					self.dedupObj.shid +=1
					self.dedupObj.totalShingleCount+=1
					self.dedupObj.totalShingleLen+=len(shingle)
				recset.add(shid)

				#updating g_shindex_rec
				try:
					self.dedupObj.g_shindex_rec[shid].add(rid)
				except KeyError:
					self.dedupObj.g_shindex_rec[shid] = set()
					self.dedupObj.g_shindex_rec[shid].add(rid)

			#updating g_rec_shindex
			self.dedupObj.g_rec_shindex[rid] = [record[URL_FIELD],recset]
			self.dedupObj.totalRecCount+=1

	def getStats(self):
		print 'Total Number of URLS:',self.dedupObj.totalRecCount
		print 'Gram Value:',self.gram
		print 'Total Shingles Count:',self.dedupObj.totalShingleCount
		print 'Total totalShingleLen:',self.dedupObj.totalShingleLen
		print 'Average Shingle Length:', (self.dedupObj.totalShingleLen/self.dedupObj.totalShingleCount)
		print 'Similarity Threshold Value:', self.dedupObj.threshold * 100, '%'
		print 'Total Duplicate Links:', self.dedupObj.totalDupLink

	def recWorker(self):
		self.createShingle()

'''		
	def createShingle(self):
		while True:
			record = self.dedupObj.recTaskQueue.get()
			shingleCount = 0
			totalShingleLen = 0
			for i in range(len(record['parse_list'])-self.dedupObj.kgram+1):
				shingle = ''
				itr = i
				for j in range(self.gram):
					shingle += record['parse_list'][itr]
					itr+=1
				shingleCount = shingleCount + 1
				totalShingleLen = totalShingleLen + len(shingle)
			self.ulock.acquire()
			try:
				self.totalRecords+=1
				self.totalShingleCount+=shingleCount
				self.totalShingleLen+=totalShingleLen
				print 'totalRecords:',self.totalRecords
			finally:
				self.ulock.release()
			self.dedupObj.recTaskQueue.task_done() '''

'''#updating localUrlInfo  No need for local record
					rec_id = record['recno']
					seg_id = record['segnum']
					rid = str(seg_id) + '-' + str(rec_id)
					localUrlInfo[rid] = record['url']

					#updating localShingleIndex
					try:
						shid = localShingleIndex[shingle]
					except KeyError:
						localShingleIndex[shingle] = sid
						shid = sid
						sid+=1
					'''