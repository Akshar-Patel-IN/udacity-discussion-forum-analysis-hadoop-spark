from pyspark.sql import SparkSession
from pyspark.sql import Row

#sparksession
spark = SparkSession \
    .builder \
    .appName("Top Users") \
    .getOrCreate()

#sparkcontext
sc = spark.sparkContext

#get lines as rdd
linesRdd = sc.textFile("forum_users.tsv")

#remove header from lines
linesRdd = linesRdd.zipWithIndex().filter(lambda (row,index): index > 0).keys()

#infer the schema and create dataframe
partsRdd = linesRdd.map(lambda l: l.split("\t"))
usersRdd = partsRdd.map(lambda p: Row(user_id=int(p[0][1:-1]), reputation=int(p[1][1:-1])))
usersDf = spark.createDataFrame(usersRdd)

#sort by reputation and get top users
topUsers = usersDf.sort(usersDf.reputation.desc());

#display result
topUsers.show(25,True)


