from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import sqlite3
import time
from io import BytesIO
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

def normalizar_texto(valor):
    return str(valor or "").strip().upper()

def numero_float(valor):
    if valor is None or valor == "":
        return 0
    try:
        return float(str(valor).replace(".", "").replace(",", "."))
    except:
        return 0

def init_db():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS status_cards (
        codigo TEXT PRIMARY KEY,
        status TEXT,
        quantidade INTEGER DEFAULT 0
    )
""")

    cursor.execute("PRAGMA table_info(status_cards)")
    colunas_status = [col[1] for col in cursor.fetchall()]

    if "quantidade" not in colunas_status:
        cursor.execute("ALTER TABLE status_cards ADD COLUMN quantidade INTEGER DEFAULT 0")

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

@app.route("/exportar-excel")
def exportar_excel():
    dados = carregar_dados_base()
    df = pd.DataFrame(dados)

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT codigo, status, quantidade FROM status_cards")
        rows = cursor.fetchall()
        mapa_quantidade = {str(codigo): quantidade or 0 for codigo, status, quantidade in rows}
    except:
        mapa_quantidade = {}

    conn.close()

    df["Quantidade para Enviar"] = df["Código do Anúncio"].astype(str).map(mapa_quantidade).fillna(0)

    tela = request.args.get("tela", "")

    if tela:
        conn = sqlite3.connect("status.db")
        cursor = conn.cursor()
        cursor.execute("SELECT codigo, status FROM status_cards")
        rows = cursor.fetchall()
        conn.close()

        status_dict = {str(codigo): status for codigo, status in rows}

        def destino(codigo):
            status = status_dict.get(str(codigo), "")
            if status == "enviando":
                return "enviando"
            if status in ["nao_enviar", "naoEnviar"]:
                return "naoEnviar"
            return "principal"

        df = df[df["Código do Anúncio"].apply(destino) == tela]

    colunas_exportar = [
        "Nickname",
        "Código do Anúncio",
        "SKU",
        "Título",
        "LOTE",
        "Quantidade para Enviar"
    ]

    for col in colunas_exportar:
        if col not in df.columns:
            df[col] = ""

    df_export = df[colunas_exportar].copy()

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Relatorio")

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name="relatorio_envio.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

@app.route("/dados-full")
def dados_full():
    return jsonify(carregar_csv_com_cache(CSV_URL_FULL, "full"))


@app.route("/salvar-status", methods=["POST"])
def salvar_status():
    data = request.json
    codigo = data.get("codigo")
    status = data.get("status")
    quantidade = data.get("quantidade", 0)

    try:
        quantidade = int(quantidade)
    except:
        quantidade = 0

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO status_cards (codigo, status, quantidade)
        VALUES (?, ?, ?)
        ON CONFLICT(codigo) DO UPDATE SET
            status=excluded.status,
            quantidade=excluded.quantidade
    """, (codigo, status, quantidade))

    conn.commit()
    conn.close()

    return jsonify({"success": True})

@app.route("/status")
def get_status():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("SELECT codigo, status, quantidade FROM status_cards")
    rows = cursor.fetchall()

    conn.close()

    status_dict = {}
    for codigo, status, quantidade in rows:
        status_dict[str(codigo)] = {
            "status": status,
            "quantidade": quantidade or 0
        }

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