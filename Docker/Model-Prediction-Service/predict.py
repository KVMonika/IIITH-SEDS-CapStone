import re
import pickle
from pyspark.sql import SparkSession
import mlflow
from mlflow import spark
from pyspark.sql.types import StructType,StructField, StringType
import pandas as pd
from pyspark.ml.pipeline import PipelineModel

def predict_topic(title, summary):
    text = clean(title + " " + summary);
    sparkSession = (SparkSession.builder.appName("News Classifier Prediction").getOrCreate())

    df = pd.DataFrame(columns = ['text'])
    row = {'text': text}
    df = df.append(row, ignore_index=True)

    test = sparkSession.createDataFrame(df)
    #print(test.show())
    # ---- load model ----
    #model = mlflow.spark.load_model("models/news-classifier-model")
    #model = pickle.load(open("models/news_classifier.pkl", "rb"))
    model = PipelineModel.load("models/news-classifier-model_2")
    #print("model loaded")
    # Make predictions.
    predictions = model.transform(test)

    # todo
    # Select example rows to display.
    predictions.select("predicted_topic").show(5)

    return predictions.first()["predicted_topic"]

def clean(text):
    # Remove HTML tags
    if not text:
        return ''
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)