from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func

#sparksession
spark = SparkSession \
    .builder \
    .appName("Top 10 Tags") \
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
		row = Row(user = p[3][1:-1], hour = p[8][12:14])
		return row

#infer the schema
parts = linesRdd.map(lambda l: l.split("\t"))
nodeRdd = parts.map(infer)

#filter None values and create dataframe
nodeRdd = nodeRdd.filter(lambda p: p is not None)
nodeDf = spark.createDataFrame(nodeRdd)

#group by users and hour and calculate hours count
nodeDf = nodeDf.groupBy('user','hour').agg(func.count(nodeDf.hour) \
		.alias("hours_count")).sort(nodeDf.user)
nodeHoursDf = nodeDf.groupBy('user') \
		.agg(func.max(nodeDf.hours_count).alias("hours_count"))

#perform join to get most active hour
cond = [nodeDf.user == nodeHoursDf.user, nodeDf.hours_count == nodeHoursDf.hours_count]
nodeDf = nodeDf.join(nodeHoursDf, cond, 'inner').select(nodeDf.user,'hour').sort(nodeDf.user)

#display result
nodeDf.show(25,True);



