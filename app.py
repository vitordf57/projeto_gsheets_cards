from flask import Flask, render_template, jsonify
import pandas as pd

app = Flask(__name__)

CSV_URL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=0"

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/dados")
def dados():
    df = pd.read_csv(CSV_URL)
    df = df.fillna("")
    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__":
    app.run(debug=True)
