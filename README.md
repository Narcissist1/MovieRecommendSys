# MovieRecommendSys

V1.0
整个过程分三步
Step1:
moviesimilarity.py 计算 电影相似度 input: u.data output: (movieid1,movieid2) (similarity,n) 
similarity 有三个，denfined in metrics.py. n is the number of users who had seen that two movies

Step2:
nameid.py 计算全部电影的组合 input: u.item output: (movieid1,movieid2) (moviename1,moviename2)

Step3:

LastStep.py 合成之前两步的结果，input: two files got from former steps output (moviename1,moviename2) (similarity,n)

Final output file is "output"

data scale: 100,000 ratings (1-5) from 943 users on 1682 movies. 
单机运行大约2，3 分钟
V1.1版本可以考虑加大数据量，使用EMR
