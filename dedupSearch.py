#/home/saurabh/usc_sem2/cs572/assgn1/nutch/nutch/runtime/local/db1.x/segments
#nutch_home /home/saurabh/usc_sem2/cs572/assgn1/nutch/nutch/

import os
import sys
from optparse import OptionParser
import subprocess
from dedupParam import *
import shutil
import Queue
import threading
from parseTask import *
import time

class DedupSearch():
	def __init__(self, segmentLoc, outdirLoc, kgram = DEFAULT_KGRAM, threshold = DEFAULT_THRESH):
		self.segmentLoc = segmentLoc
		self.outdirLoc = outdirLoc
		self.kgram = kgram
		self.threshold = threshold
		self.totalShingleCount = 0
		self.totalShingleLen = 0
		self.totalRecCount = 0
		self.totalDupLink = 0
		self.shid = 0
		self.g_shingle_index = {}
		self.g_rec_shindex = {}
		self.g_shindex_rec = {}
		self.g_cmp_results = {}
		self.segTaskQueue = Queue.Queue()
		self.recTaskQueue = Queue.Queue()
		self.segTaskHandler = None
		self.lock = threading.Lock()
		self.segDoneEvent = threading.Event()
		self.segDoneEvent.clear()
		self.segDirs = []

	def compute(self):
		try:
			self.generateDump()
			self.enqueSegJobs()
			start_time = time.time()
			self.segTaskHandler = parseTask(self)
			self.segTaskHandler.start()
			self.segTaskHandler.join()
			print 'Time of Execution:',(time.time() - start_time)
			print 'seg task handler joined'
			if(os.path.exists(self.outdirLoc)):
				for dirs in self.segDirs:
					if not dirs.startswith('dedup'):
						path1 = os.path.join(self.outdirLoc, dirs)
						shutil.rmtree(path1)
		except Exception,e:
			print e

	def enqueSegJobs(self):
		i=1
		for item in os.listdir(self.outdirLoc):
			if item.startswith(DUMP_DIR_PREFIX):
				self.segTaskQueue.put([item.strip(DUMP_DIR_PREFIX),item])

	def generateDump(self):
		path = None
		count = 1
		'''temp_out_dir = os.path.join(self.outdirLoc, 'output')
		if(os.path.exists(temp_out_dir)):
			print 'deleting output directory', temp_out_dir
			shutil.rmtree(temp_out_dir)
		self.outdirLoc = temp_out_dir'''
		for item in os.listdir(self.segmentLoc):
			path = os.path.join(self.segmentLoc, item)
			newOutDir = os.path.join(self.outdirLoc, DUMP_DIR_PREFIX+str(count))
			if(os.path.exists(newOutDir)):
				print 'deleting dir', newOutDir
				shutil.rmtree(newOutDir)
			#print 'outdir',self.outdirLoc
			self.segDirs.append(newOutDir)
			command = NUTCH_BIN + GEN_PARSE_DUMP + path +' ' + newOutDir + PARSE_ARGS
			#print 'command', command
			ret = subprocess.call(command, shell=True)
			if(ret):
				print GEN_PARSE_ERROR, path
				sys.exit(-1)
			count = count+1

	def displayResult(self):
		pass



if __name__ == "__main__":
	parser = OptionParser()
	parser.add_option("-s", "--spath", action="store", type="string", dest="segmentLoc", help="Specify segment path of crawled pages")
	parser.add_option("-n", "--npath", action="store", type="string", dest="nutchHome", help="specify nutch home directory path")
	parser.add_option("-o", "--outdir", action="store", type="string", dest="outLoc", help="specify output directory path to create dump")
	parser.add_option("-k", "--kgram", action="store", type="int", dest="kgram", help="Specify k-gram at word granularity")
	parser.add_option("-t", "--threshold", action="store", type="float", dest="threshold", help="Specify threshold for similarity")
	(options, args) = parser.parse_args()
	if((not options.segmentLoc) or (not options.nutchHome) or (not options.outLoc)):
		print "Please specify arguments correctly to search for near duplicate document"
		print SEARCH_COMMAND
		sys.exit(-1)
	if(len(args)):
		print "Wrong command line arguments passed"
		print SEARCH_COMMAND
		sys.exit(-1)
	if(not options.threshold):
		thresh = DEFAULT_THRESH
	else:
		thresh = (options.threshold)
	
	if thresh > 1:
		thresh = thresh/100
	
	if(not options.kgram):
		kgram = DEFAULT_KGRAM
	else:
		kgram = int(options.kgram)
	
	NUTCH_HOME  = os.path.abspath(options.nutchHome)
	NUTCH_BIN = os.path.join(NUTCH_HOME, REL_BIN_PATH)
	NUTCH_BIN = os.path.normpath(NUTCH_BIN)
	SEGMENT_LOC = os.path.abspath(options.segmentLoc)
	OUTDIR_LOC = os.path.abspath(options.outLoc)
	if(not os.path.exists(NUTCH_BIN)):
		print INV_OUTDIR_PATH
		sys.exit(-1)

	if(not os.path.exists(OUTDIR_LOC)):
		print INV_NUTCH_PATH
		sys.exit(-1)

	if(not os.path.exists(SEGMENT_LOC)):
		print INV_SEG_PATH
		sys.exit(-1)
	SearchObj = DedupSearch(SEGMENT_LOC, OUTDIR_LOC, kgram, thresh)
	SearchObj.compute()
	print 'exiting program'
	#sys.exit(0)

