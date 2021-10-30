import mlflow
from mlflow import spark

def predict(title, summary):
    test = spark.createDataFrame([
    (title + " " + summary)], ["text"])

    # ---- load model ----
    model = mlflow.spark.load_model("../news-classifier-model")

    # Make predictions.
    predictions = model.transform(test)

    # todo
    # Select example rows to display.
    predictions.select("predicted_topic").show(5)
    return True