import os
from dedupParam import *
import time
import copy

class jaccSim():
	def __init__(self, dedupObj):
		self.dedupObj = dedupObj
		self.filep = None
		self.no_cmp = 0
		self.no_rec_act_cmp = 0
		self.matchCount = 0

#self.dedupObj.g_rec_shindex[rid] = [record[URL_FIELD],recset]
#self.g_rec_shindex = {}
	def process(self):
		print 'Total compare dataset records:',len(self.dedupObj.g_rec_shindex)
		print 'threshold', self.dedupObj.threshold
		time.sleep(5)
		try:
			outFile = os.path.join(self.dedupObj.outdirLoc, 'dedup.txt')
			print 'outfile:',outFile
			self.filep = open(outFile, 'w')
			while self.dedupObj.g_rec_shindex:
				self.no_rec_act_cmp+=1
				record = self.dedupObj.g_rec_shindex.popitem()
				print 'pending length of global record:',len(self.dedupObj.g_rec_shindex)
				#self.filep.write(line)
				self.compare(record[1], record)
			
		finally:
			self.filep.close()
		print 'number of records after filtering:',self.no_rec_act_cmp
		print 'number of comparisons:',self.no_cmp
			
	def compare(self, cmpdata, record):
		#print 'threshold:',self.dedupObj.threshold
		header = 0
		local_g_rec_shindex = copy.deepcopy(self.dedupObj.g_rec_shindex)
		for rec,  data in local_g_rec_shindex.iteritems():
			data_out = ''
			ints_len = float(len(cmpdata[SET_FIELD].intersection(data[SET_FIELD])))
			union_len = float(len(cmpdata[SET_FIELD].union(data[SET_FIELD])))
			self.no_cmp+=1
			if((ints_len/union_len)>=self.dedupObj.threshold):
				#print 'cmpset', cmpdata[SET_FIELD]
				#print 'matchset', data[SET_FIELD]
				#print 'int-len', ints_len
				#print 'unn-len', union_len
				#print 'intlen/union\n', (ints_len/union_len)
				#print '\n'
				#print 'base record',cmpdata[URL_FIELD]
				#print 'comparing record',data[URL_FIELD]
				sep = record[0].split('-')
				dedupBuffer=''
				if not header:
					dedupBuffer = '\n\nmatch::'+ str(self.matchCount+1)+ '\nLocation:: segment#: '+sep[0] + ' recno#: '+sep[1] + '\n' + record[1][URL_FIELD] + '\n\n'
					dedupBuffer+='duplicate links::\n'
					header = 1
					self.matchCount+=1
				sep1 = rec.split('-')
				data_out = 'segment#: '+sep1[0] + ' recno# '+sep1[1] + '; ' + data[URL_FIELD] +'\n'
				dedupBuffer+=data_out
				self.filep.write(dedupBuffer)
				print 'deduplinks count', self.dedupObj.totalDupLink
				self.dedupObj.totalDupLink+=1

				#delete all duplicate links from g_rec_shingles
				del self.dedupObj.g_rec_shindex[rec]






			