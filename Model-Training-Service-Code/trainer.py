from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sparknlp 
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, Normalizer, LemmatizerModel, StopWordsCleaner
from nltk.corpus import stopwords

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF

Mongo_Atlas_URI = "mongodb+srv://monika:monika@cluster0.99bxh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
News_Feed_DataBase = "news"
News_Collection_URI = Mongo_Atlas_URI + News_Feed_DataBase+ ".news"
News_Feed_Collection = "news"

spark = (
            SparkSession.builder.appName("News Classifier Training")
            .config("spark.mongodb.input.uri", News_Collection_URI)
            .config("spark.mongodb.output.uri", News_Collection_URI)
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.1")
            .config("spark.jars", "spark-nlp-spark24_2.11-3.3.0.jar")
            .getOrCreate()
        )                                                                                                                              
sc = SparkContext.getOrCreate()

# Read data from MongoDB Atlas - DB.Collection: 'news.news'
df = (
        spark.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("database", News_Feed_DataBase).option("collection", News_Feed_Collection).load()
    )
print("output:",df.show())

# PreProcessing Steps

text_col = 'summary'
summary = df.select(text_col).filter(F.col(text_col).isNotNull())
print(summary.show())

# Document Assembler
documentAssembler = DocumentAssembler().setInputCol(text_col).setOutputCol('document')
print("hi")

# Tokenization
tokenizer = Tokenizer().setInputCols(['document']).setOutputCol('tokenized')

# Normalization
normalizer = Normalizer().setInputCols(['tokenized']).setOutputCol('normalized').setLowercase(True)

# Lemmatization
lemmatizer = LemmatizerModel.pretrained().setInputCols(['normalized']).setOutputCol('lemmatized')

# Stop Words Removal
eng_stopwords = stopwords.words('english')
stopwords_cleaner = ( StopWordsCleaner()
                        .setInputCols(['lemmatized'])
                        .setOutputCol('no_stop_lemmatized')
                        .setStopWords(eng_stopwords)
                    )       

# pipeline
pipeline = Pipeline().setStages([documentAssembler,
                                tokenizer,
                                normalizer,
                                lemmatizer,
                                stopwords_cleaner])

processed_text = pipeline.fit(summary).transform(summary)
print("Processed Text:")
print(processed_text.show())


tfizer = CountVectorizer(inputCol='no_stop_lemmatized', outputCol='tf_features')
term_frequecy_res = tfizer.fit(processed_text).transform(processed_text)

idfizer = IDF(inputCol='tf_features', outputCol='tf_idf_features')
idf_res = idfizer.fit(term_frequecy_res).transform(term_frequecy_res)

print("TF/IDF: ", idf_res)