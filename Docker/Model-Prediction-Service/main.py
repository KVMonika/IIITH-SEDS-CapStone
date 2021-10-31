from flask import Flask,render_template, request
from predict import predict_topic

app = Flask(__name__)

@app.route("/predict/", methods=['POST'])
def predict():
    payload = request.get_json()
    response = predict_topic(str(payload['title']), str(payload['description']))
    return {"detail": "Training successful"}

@app.route('/')
def home():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8111,debug=True)
