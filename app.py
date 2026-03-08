from flask import Flask, render_template, jsonify, request
import pandas as pd
import sqlite3
import time
from collections import Counter

app = Flask(__name__)

CSV_URL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=0"
CSV_URL_FULL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=184771586"

def carregar_dados_base():
    global cache_dados, cache_dados_ts

    agora = time.time()

    if cache_dados is None or (agora - cache_dados_ts) > CACHE_TTL:
        df = pd.read_csv(CSV_URL)
        df = df.fillna("")
        cache_dados = df.to_dict(orient="records")
        cache_dados_ts = agora

    return cache_dados

CACHE_TTL = 300  # 5 minutos

cache_dados = None
cache_dados_ts = 0

cache_full = None
cache_full_ts = 0

cache_dados_ts = 0
CACHE_TTL = 300


def carregar_csv_com_cache(url, tipo="dados"):
    global cache_dados, cache_dados_ts, cache_full, cache_full_ts

    agora = time.time()

    if tipo == "dados":
        if cache_dados is None or (agora - cache_dados_ts) > CACHE_TTL:
            df = pd.read_csv(url)
            df = df.fillna("")
            cache_dados = df.to_dict(orient="records")
            cache_dados_ts = agora
        return cache_dados

    if tipo == "full":
        if cache_full is None or (agora - cache_full_ts) > CACHE_TTL:
            df = pd.read_csv(url)
            df = df.fillna("")
            cache_full = df.to_dict(orient="records")
            cache_full_ts = agora
        return cache_full

    return []


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


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/home")
def home():
    return render_template("home.html")


@app.route("/metricas-full")
def metricas_full():
    return render_template("metricas_full.html")


@app.route("/dados")
def dados():
    base = carregar_dados_base()

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, status FROM status_cards")
    rows = cursor.fetchall()
    conn.close()

    status_dict = {str(codigo): status for codigo, status in rows}

    dados_filtrados = []

    for row in base:
        codigo = str(row.get("Código do Anúncio", "")).strip()
        status = status_dict.get(codigo)

        if status == "enviando" or status == "nao_enviar":
            continue

        dados_filtrados.append(row)

    return jsonify(dados_filtrados)

@app.route("/dados-dashboard")
def dados_dashboard():
    return jsonify(carregar_dados_base())


@app.route("/dados-full")
def dados_full():
    return jsonify(carregar_csv_com_cache(CSV_URL_FULL, "full"))


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

    return jsonify(comentarios)


@app.route("/debug-comentarios")
def debug_comentarios():
    conn = sqlite3.connect("status.db")
    c = conn.cursor()
    c.execute("SELECT * FROM comentarios")
    dados = c.fetchall()
    conn.close()
    return jsonify({"dados": dados})


def carregar_vendas():
    dados = carregar_csv_com_cache(CSV_URL, "dados")
    vendas = []

    for row in dados:
        vendas.append({
            "produto": row.get("Código do Anúncio", "")
        })

    return vendas


@app.route("/dashboard")
def dashboard():
    vendas = carregar_vendas()
    contador = Counter([v["produto"] for v in vendas if v["produto"]])

    mais_vendidos = contador.most_common(5)

    return render_template(
        "metricas_full.html",
        mais_vendidos=mais_vendidos
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)