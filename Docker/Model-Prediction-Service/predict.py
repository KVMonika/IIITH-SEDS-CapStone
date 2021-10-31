import re
import pickle
from pyspark.sql import SparkSession
import mlflow

def predict_topic(title, summary):
    text = clean(title + " " + summary);

    spark = (SparkSession.builder.appName("News Classifier Prediction"))
    test = spark.createDataFrame([(text)], ["text"])

    # ---- load model ----
    model = mlflow.spark.load_model("models/news-classifier-model")
    #model = pickle.load(open("models/news_classifier.pkl", "rb"))

    # Make predictions.
    predictions = model.transform(test)

    # todo
    # Select example rows to display.
    predictions.select("predicted_topic").show(5)
    return True

def clean(text):
    # Remove HTML tags
    if not text:
        return ''
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)