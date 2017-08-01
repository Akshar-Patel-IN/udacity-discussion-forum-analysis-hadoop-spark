from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as func

#sparksession
spark = SparkSession \
    .builder \
    .appName("Top Tags") \
    .getOrCreate()

#sparkcontext
sc = spark.sparkContext

#get lines as rdd
linesRdd = sc.textFile("forum_node.tsv")

#For executing on Hadoop cluster
#use hdfs path: linesRdd = sc.textFile("hdfs://HadoopMaster:9000/users/hdusers/forum_node.tsv")
#submit program with "spark-submit --master yarn --deploy-mode cluster top-tags.py"

#remove header from lines
linesRdd = linesRdd.zipWithIndex().filter(lambda (row,index): index > 0).keys()

#for infering the schema
def infer(p):
	if(len(p) == 19):
		row = Row(tagnames = p[2][1:-1])
		return row

#infer the schema
partsRdd = linesRdd.map(lambda l: l.split("\t"))
nodeRdd = partsRdd.map(infer)

#filter None values and create dataframe
nodeRdd = nodeRdd.filter(lambda p: p is not None)
nodeDf = spark.createDataFrame(nodeRdd)

#split and explode tags
nodeDf = nodeDf.select(func.explode(func.split(nodeDf.tagnames, " ")).alias("tags"))

#group tags and calculate number of times used
tagsDf = nodeDf.groupBy('tags').agg(func.count(nodeDf.tags).alias("num_times_used")).filter(nodeDf.tags != "");
tagsDf = tagsDf.sort(tagsDf.num_times_used.desc())

#display result
tagsDf.show(25,True);


