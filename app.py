import pickle
from flask import Flask, request
from utils.models import Clean

app = Flask(__name__)

@app.route("/api/v1/resources/books/all", methods=["GET", "POST"])
def get_all():
    if request.method == "GET":

        return {"Instructions": """Use the following endpoint '/api/v1/resources/books/all' 
        with the param 'text' followed by a text to predict"""}

    else:

        with open("data/finished_model.model", "rb") as file:
            model = pickle.load(file)

        text = request.args.get("text")
        text = Clean().clean_emojis(text)
        text = Clean().remove_links(text)
        text = Clean().signs_tweets(text)

        prediction = model.predict(text)

        if prediction == 0:
            result = "Positive sentiment"
        else: 
            result = "Negative sentiment"

        return {"Prediction": result}


app.run()