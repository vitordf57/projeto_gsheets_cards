from flask import Flask, render_template, jsonify
import pandas as pd
import sqlite3
from flask import Flask, render_template, jsonify, request

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

def init_db():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS status_cards (
            codigo TEXT PRIMARY KEY,
            status TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comentarios (
            sku TEXT PRIMARY KEY,
            comentario TEXT
        )
    """)

    conn.commit()
    conn.close()

init_db()

@app.route("/salvar-status", methods=["POST"])
def salvar_status():
    data = request.json
    codigo = data.get("codigo")
    status = data.get("status")

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO status_cards (codigo, status)
        VALUES (?, ?)
        ON CONFLICT(codigo) DO UPDATE SET status=excluded.status
    """, (codigo, status))

@app.route("/salvar-comentario", methods=["POST"])
def salvar_comentario():
    data = request.json
    sku = data.get("sku")
    comentario = data.get("comentario")

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO comentarios (sku, comentario)
        VALUES (?, ?)
        ON CONFLICT(sku)
        DO UPDATE SET comentario=excluded.comentario
    """, (sku, comentario))

    conn.commit()
    conn.close()

    return jsonify({"success": True})

@app.route("/status")
def get_status():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("SELECT codigo, status FROM status_cards")
    rows = cursor.fetchall()

    conn.close()

    status_dict = {codigo: status for codigo, status in rows}

    return jsonify(status_dict)

@app.route("/comentarios")
def listar_comentarios():
    conn = sqlite3.connect("status.db")
    c = conn.cursor()
    c.execute("SELECT sku, comentario FROM comentarios")
    rows = c.fetchall()
    conn.close()

    comentarios = {}
    for sku, comentario in rows:
        comentarios[sku] = comentario

    return comentarios

@app.route("/debug-comentarios")
def debug_comentarios():
    conn = sqlite3.connect("status.db")
    c = conn.cursor()
    c.execute("SELECT * FROM comentarios")
    dados = c.fetchall()
    conn.close()
    return {"dados": dados}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
