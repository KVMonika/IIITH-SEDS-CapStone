# For Reference
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, Normalizer, Stemmer, StopWordsCleaner
from nltk.corpus import stopwords
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.sql.functions import concat,concat_ws
from pyspark.sql.functions import col, concat, lit

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

import mlflow

Mongo_Atlas_URI = "mongodb+srv://monika:monika@cluster0.99bxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
News_Feed_DataBase = "news"
News_Collection_URI = Mongo_Atlas_URI + News_Feed_DataBase+ ".news"

sparkSession = (
            SparkSession.builder.appName("News Classifier Training")
            .config("spark.mongodb.input.uri", News_Collection_URI)
            .config("spark.mongodb.output.uri", News_Collection_URI)
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
            .config("spark.jars", "spark-nlp-spark24_2.11-3.3.1.jar")
            .getOrCreate()
        )
sc = SparkContext.getOrCreate()

# Read data from MongoDB Atlas - DB.Collection: 'news.news'
df = (
        sparkSession.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("database","news").option("collection", "news").load()
    )
print("output:",df.show())

# PreProcessing Steps

text_col = 'text'
summary_col = 'summary'
title_col = 'title'
df_filtered = df.filter(F.col(summary_col).isNotNull() | F.col(title_col).isNotNull())
df_filtered = df_filtered.withColumn(text_col, concat_ws(' ', col(title_col), col(summary_col)))

# Document Assembler
documentAssembler = DocumentAssembler().setInputCol(text_col).setOutputCol('document')

# Tokenization
tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('tokenized')

# Normalization
normalizer = Normalizer().setInputCols(['tokenized']).setOutputCol('normalized').setLowercase(True)

# Stemming
stemmer = Stemmer().setInputCols(['normalized']).setOutputCol('stemmed')

# Stop Words Removal
eng_stopwords = stopwords.words('english')
stopwords_cleaner = ( StopWordsCleaner()
                        .setInputCols(['stemmed'])
                        .setOutputCol('no_stop_stemmed')
                        .setStopWords(eng_stopwords)
                    )
# Finisher (annotations to human readable)
finisher = Finisher().setInputCols(['no_stop_stemmed'])

tfizer = CountVectorizer(inputCol='finished_no_stop_stemmed', outputCol='tf_features')

idfizer = IDF(inputCol='tf_features', outputCol='tf_idf_features')

labelIndexer = StringIndexer(inputCol="topic", outputCol="indexed_topic").fit(df_filtered)
# Segregation
(trainingData, testingData) = df_filtered.randomSplit([0.8, 0.2])

rf_clf = RandomForestClassifier(labelCol="indexed_topic", featuresCol="tf_idf_features", numTrees=10)
labelConverter = IndexToString(inputCol="prediction", outputCol="predicted_topic",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline2 = Pipeline(stages=[documentAssembler, tokenizer, normalizer, stemmer, stopwords_cleaner, finisher, tfizer, idfizer, labelIndexer, rf_clf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline2.fit(trainingData)

# Make predictions.
predictions = model.transform(testingData)

# Select example rows to display.
predictions.select("predicted_topic", "topic", text_col).show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexed_topic", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[1]
print(rfModel)  # summary only

# save model
mlflow.spark.save_model(model, "../news-classifier-model")