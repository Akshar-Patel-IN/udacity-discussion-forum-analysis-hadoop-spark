from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func

#sparksession
spark = SparkSession \
    .builder \
    .appName("Average Length of Question, Answer and Comment") \
    .getOrCreate()

#sparkcontext
sc = spark.sparkContext

#get lines as Rdd
linesRdd = sc.textFile("forum_node.tsv")

#remove header from lines
linesRdd = linesRdd.zipWithIndex().filter(lambda (row,index): index > 0).keys()

#for infering the schema
def infer(p):
	if(len(p) == 19):
		row = Row(body = int(len(p[4])),node_type = str(p[5][1:-1]))
		return row

#infer the schema
partsRdd = linesRdd.map(lambda l: l.split("\t"))
nodeRdd = partsRdd.map(infer)

#filter None values and create dataframe
nodeRdd = nodeRdd.filter(lambda p: p is not None)
nodeDf = spark.createDataFrame(nodeRdd)

#group by node type
qaDf = nodeDf.groupBy('node_type').agg(func.sum(nodeDf.body).alias("body_sum") \
					,func.count(nodeDf.node_type).alias("total_node"))
#calculate sum for each node type					
sumQue = qaDf.filter(qaDf.node_type == 'question').take(1)[0].body_sum
totalQue = qaDf.filter(qaDf.node_type == 'question').take(1)[0].total_node
sumAns = qaDf.filter(qaDf.node_type == 'answer').take(1)[0].body_sum
totalAns = qaDf.filter(qaDf.node_type == 'answer').take(1)[0].total_node
sumCom = qaDf.filter(qaDf.node_type == 'comment').take(1)[0].body_sum
totalCom = qaDf.filter(qaDf.node_type == 'comment').take(1)[0].total_node

#calculate average
print("**************************************************************")
print ("Average Length of Question, Answer and Comment:\n")
print "Average Question Length:" + str(sumQue / totalQue)
print "\nAverage Answer Length:" + str(sumAns / totalAns)
print "\nAverage Comment Length:" + str(sumCom / totalCom)
print("**************************************************************")

