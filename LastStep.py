from mrjob.job import MRJob
import mrjob

class ValueProtocol(object):
	def write(self,key,values):
		return ';'.join(str(v) for v in values)

class getresult(MRJob):

	'''
	get final result from the former two steps

	step1 got (MovieID1,MovieID2),(similarity,n)
	step2 got (MovieID1,MovieID2),(MovieName1,MovieName2)

	what we want (MovieName1,MovieName2),(similarity,n)
	'''
	OUTPUT_PROTOCOL=ValueProtocol
	INPUT_PROTOCOL=mrjob.protocol.JSONProtocol

	def steps(self):
		return [self.mr(mapper=self.group_by_idpairs,reducer=self.generate_final_result)]

	def group_by_idpairs(self,key,values):

		if len(values)==2:
			name1,name2=values
			yield key,list((name1,name2))
		else:
			s1,s2,s3,n=values
			yield key,list((s1,s2,s3,n))

	def generate_final_result(self,key,values):

		# name1,name2,s1,s2,s3,n=values
		f=[]
		for item in values:
			f=f+item
			# name1,name2=item[0]
			# s1,s2,s3,n=item[1]
		# 	if len(item)==2:
		# 		name1,name2=item[:]
		# 	elif len(item)==4:
		# 		s1,s2,s3,n=item[:]
			# yield (name1,name2),(s1,s2,s3,n)
		if len(f)>2:
			yield None,f


if __name__ == '__main__':
	getresult.run()
