#Import libraries
Import pyspark.sql import Spark Session

#create session
spark = Spark Session.builder.getOrCreate()

#create a array
data = [("a",1),("b",2),("c",3)]

#create dataframe
df = spark.createDataFrame(data,["sample","Value"])
df.show()

#Import a csv - assuming CSV contains the header
csvDF = spark.read.csv("Path/To/CSV/On/PC", header=True, inferSchema=True)
csvDF.show()

#Filter rows
filtered_df = df.where(df.value > 5)
#same as
filtered_df = df.filter(df.value > 5)

#select specific columns
samplesDF = df.select("sample")
samplesDF.show()

#Rename a column
renamedDF= df.withColumnRenamed("sample","Sample")
renamedDF.show()

#sort/orderby
sortedDF = df.sort(df.Sample)
orderedDF = df.orderBy(df.Value)

sortedDF.show()

#Groupby
groupedDF = df.groupBy("Sample").count()
groupedDF.show()


#Join
data2 = [("a","paid"),("b","paid"),("c","unpaid")]
df2 = spark.createDataFrame(data2,["Sample","Bill Status"])
joined = df.join(df2,df.sample == df2.sample, "inner")
joined.show()

#union
data3 = [("d","4"),("e","5"),("f","6")]
df3 = spark.createDataFrame(data3,["sample","value"])
unionedDF = df.union(df3)
unionedDF.show()

#Cache a df to memory
df.cache()

#Handle missing or null

#drop null values
df.dropna()

#fill null values with a specific value
df.fillna(0)

#Replace null values in a specific column
df.na.fill({"sample":0})
