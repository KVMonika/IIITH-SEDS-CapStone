from pyspark import SparkContext
from pyspark.sql import SparkSession
Mongo_Atlas_URI = "mongodb+srv://monika:monika@cluster0.99bxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
News_Feed_DataBase = "news"
News_Collection_URI = Mongo_Atlas_URI + News_Feed_DataBase+ ".news"

spark = (
            SparkSession.builder.appName("News Classifier Training")
            .config("spark.mongodb.input.uri", News_Collection_URI)
            .config("spark.mongodb.output.uri", News_Collection_URI)
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
            .getOrCreate()
        )                                                                                                                              sc = SparkContext.getOrCreate()

# Read data from MongoDB Atlas - DB.Collection: 'news.news'
df = (
        spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("database","news").option("collection", "news").load()
    )
print("output:",df.show())