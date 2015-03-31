import os
import sys
import threading
import time
import re
from dedupParam import *
from nltk.corpus import stopwords
from genShingle import *

cachedStopWords = stopwords.words("english")

def removeStopWords(rawText):
	rawText = re.sub(REGEXP_PUNC,'',rawText)
	parseList = [word for word in rawText.split() if word not in cachedStopWords]
	return parseList


class parseTask(threading.Thread):
	def __init__(self, dedupObj):
		threading.Thread.__init__(self)
		self.dedupObj = dedupObj
		self.recTaskHandler = None


	def run(self):
		#create threads for hadnling segment task
		for i in range(NUM_SEG_THREAD):
			t = threading.Thread(target = self.segWorker)
			t.daemon = True
			t.start()
		#create threads for handling record task
		self.recTaskHandler = genShingle(self.dedupObj, self.dedupObj.kgram)
		self.recTaskHandler.start()
		self.dedupObj.segTaskQueue.join()
		print 'segment task queue joined'
		self.dedupObj.segDoneEvent.set()
		#self.dedupObj.recTaskQueue.join() #MUST#if not given then as soon as recTaskQ becomes empty then it will join n later no pushing  of records
		print 'waiting for thread to finish'
		self.recTaskHandler.join()
		'''while self.dedupObj.segTaskQueue.qsize():
			segnum, segpath = self.dedupObj.segTaskQueue.get()
			self.parseDump(segnum, segpath)'''

	def parseDump(self, segnum, segpath):
		if os.path.exists(segpath):
			for fpath in os.listdir(segpath):
				fp = open(os.path.join(segpath,fpath), 'r')
				fdata = fp.read()
				fdata = fdata.split('Recno:: ')
				#have to omit 1st item from list as first item in \n
				for item in range(1,len(fdata)):
					subitems = fdata[item].splitlines()
					recno = int(subitems[0])
					url = subitems[1]
					rawText = subitems[4]
					if(rawText):
						parseList = removeStopWords(rawText)
						record = {'segnum':int(segnum), 'recno':recno, 'url':url, 'parse_list':parseList}	
						self.dedupObj.recTaskQueue.put(record)
					
	def segWorker(self):
		while True:
			segnum, segpath = self.dedupObj.segTaskQueue.get()
			self.parseDump(segnum, segpath)
			self.dedupObj.segTaskQueue.task_done()






