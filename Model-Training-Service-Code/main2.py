from flask import Flask
from retrain import retrain_model

app = Flask(__name__)

@app.route("/retrain", methods=['GET'])
def retrain():
    response = retrain_model()
    return {"detail": "Training successful"}

if __name__ == "__main__":
    app.run(host='10.0.2.15',debug=True)
