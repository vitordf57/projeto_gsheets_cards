from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import sqlite3
import time
from io import BytesIO
from collections import Counter
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter
import os
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER_CONFERENCIA = os.path.join("static", "uploads", "conferencia")
os.makedirs(UPLOAD_FOLDER_CONFERENCIA, exist_ok=True)

app.config["UPLOAD_FOLDER_CONFERENCIA"] = UPLOAD_FOLDER_CONFERENCIA

CSV_URL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=0"
CSV_URL_FULL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=184771586"

CACHE_TTL = 300  # 5 minutos

cache_dados = None
cache_dados_ts = 0

cache_full = None
cache_full_ts = 0


def carregar_dados_base():
    global cache_dados, cache_dados_ts

    agora = time.time()

    if cache_dados is None or (agora - cache_dados_ts) > CACHE_TTL:
        df = pd.read_csv(CSV_URL)
        df = df.fillna("")
        cache_dados = df.to_dict(orient="records")
        cache_dados_ts = agora

    return cache_dados


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


@app.route("/conferencia")
def conferencia():
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            lc.numero_lote,
            lc.tipo_lote,
            lc.status,
            lc.data_criacao,
            COUNT(li.id) AS total_itens,
            SUM(CASE WHEN ci.status_item = 'OK' THEN 1 ELSE 0 END) AS itens_ok,
            SUM(CASE WHEN ci.status_item = 'DIVERGENTE' THEN 1 ELSE 0 END) AS itens_divergentes,
            SUM(CASE WHEN ci.id IS NOT NULL THEN 1 ELSE 0 END) AS itens_conferidos
        FROM lotes_conferencia lc
        LEFT JOIN lotes_itens li ON lc.numero_lote = li.numero_lote
        LEFT JOIN conferencia_itens ci
            ON lc.numero_lote = ci.numero_lote AND li.codigo = ci.codigo
        GROUP BY lc.numero_lote, lc.tipo_lote, lc.status, lc.data_criacao
        ORDER BY lc.data_criacao DESC
    """)

    lotes = cursor.fetchall()
    conn.close()

    return render_template("conferencia.html", lotes=lotes, lote=None, itens=[])


@app.route("/conferencia/<numero_lote>")
def conferencia_lote(numero_lote):
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            lc.numero_lote,
            lc.tipo_lote,
            lc.status,
            lc.data_criacao,
            COUNT(li.id) AS total_itens,
            SUM(CASE WHEN ci.status_item = 'OK' THEN 1 ELSE 0 END) AS itens_ok,
            SUM(CASE WHEN ci.status_item = 'DIVERGENTE' THEN 1 ELSE 0 END) AS itens_divergentes,
            SUM(CASE WHEN ci.id IS NOT NULL THEN 1 ELSE 0 END) AS itens_conferidos
        FROM lotes_conferencia lc
        LEFT JOIN lotes_itens li ON lc.numero_lote = li.numero_lote
        LEFT JOIN conferencia_itens ci
            ON lc.numero_lote = ci.numero_lote AND li.codigo = ci.codigo
        WHERE lc.numero_lote = ?
        GROUP BY lc.numero_lote, lc.tipo_lote, lc.status, lc.data_criacao
    """, (numero_lote,))
    lote = cursor.fetchone()

    cursor.execute("""
        SELECT
            li.numero_lote,
            li.codigo,
            li.sku,
            li.titulo,
            li.quantidade_esperada,
            li.endereco,
            li.lote_filete,
            ci.quantidade_conferida,
            ci.foto_path,
            ci.status_item,
            ci.observacao,
            ci.conferido_em
        FROM lotes_itens li
        LEFT JOIN conferencia_itens ci
            ON li.numero_lote = ci.numero_lote AND li.codigo = ci.codigo
        WHERE li.numero_lote = ?
        ORDER BY li.endereco, li.sku
    """, (numero_lote,))
    itens = cursor.fetchall()

    cursor.execute("""
        SELECT
            lc.numero_lote,
            lc.tipo_lote,
            lc.status,
            lc.data_criacao,
            COUNT(li.id) AS total_itens,
            SUM(CASE WHEN ci.status_item = 'OK' THEN 1 ELSE 0 END) AS itens_ok,
            SUM(CASE WHEN ci.status_item = 'DIVERGENTE' THEN 1 ELSE 0 END) AS itens_divergentes,
            SUM(CASE WHEN ci.id IS NOT NULL THEN 1 ELSE 0 END) AS itens_conferidos
        FROM lotes_conferencia lc
        LEFT JOIN lotes_itens li ON lc.numero_lote = li.numero_lote
        LEFT JOIN conferencia_itens ci
            ON lc.numero_lote = ci.numero_lote AND li.codigo = ci.codigo
        GROUP BY lc.numero_lote, lc.tipo_lote, lc.status, lc.data_criacao
        ORDER BY lc.data_criacao DESC
    """)
    lotes = cursor.fetchall()

    conn.close()

    return render_template("conferencia.html", lotes=lotes, lote=lote, itens=itens)


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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comentarios (
            sku TEXT PRIMARY KEY,
            comentario TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comentarios_mlb (
            codigo TEXT PRIMARY KEY,
            comentario TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lotes_conferencia (
            numero_lote TEXT PRIMARY KEY,
            tipo_lote TEXT,
            status TEXT DEFAULT 'PENDENTE',
            data_criacao TEXT,
            data_fechamento TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lotes_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_lote TEXT,
            codigo TEXT,
            sku TEXT,
            titulo TEXT,
            quantidade_esperada INTEGER DEFAULT 0,
            endereco TEXT,
            lote_filete TEXT,
            UNIQUE(numero_lote, codigo)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS conferencia_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_lote TEXT,
            codigo TEXT,
            sku TEXT,
            quantidade_conferida INTEGER DEFAULT 0,
            foto_path TEXT,
            status_item TEXT DEFAULT 'PENDENTE',
            observacao TEXT,
            conferido_em TEXT,
            UNIQUE(numero_lote, codigo)
        )
    """)

    cursor.execute("PRAGMA table_info(status_cards)")
    colunas_status = [col[1] for col in cursor.fetchall()]

    if "quantidade" not in colunas_status:
        cursor.execute("ALTER TABLE status_cards ADD COLUMN quantidade INTEGER DEFAULT 0")

    conn.commit()
    conn.close()


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
    return jsonify(carregar_dados_base())


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
        mapa_status = {str(codigo): status or "" for codigo, status, quantidade in rows}
    except:
        mapa_quantidade = {}
        mapa_status = {}

    try:
        cursor.execute("SELECT sku, comentario FROM comentarios")
        rows_comentarios = cursor.fetchall()
        mapa_comentarios = {str(sku): comentario or "" for sku, comentario in rows_comentarios}
    except:
        mapa_comentarios = {}

    try:
        cursor.execute("SELECT codigo, comentario FROM comentarios_mlb")
        rows_comentarios_mlb = cursor.fetchall()
        mapa_comentarios_mlb = {str(codigo): comentario or "" for codigo, comentario in rows_comentarios_mlb}
    except:
        mapa_comentarios_mlb = {}

    conn.close()

    df["Quantidade para Enviar"] = df["Código do Anúncio"].astype(str).map(mapa_quantidade).fillna(0)
    df["Comentário"] = df["Código do Anúncio"].astype(str).map(mapa_comentarios_mlb).fillna("")
    df["LETICIA"] = ""

    tela = request.args.get("tela", "")

    if tela:
        def destino(codigo):
            status = mapa_status.get(str(codigo), "")
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
        "Quantidade para Enviar",
        "Comentário",
        "LETICIA"
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


def registrar_lote_conferencia(numero_lote, tipo_lote, df_lote):
    if df_lote.empty or not numero_lote:
        return

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO lotes_conferencia (numero_lote, tipo_lote, status, data_criacao)
        VALUES (?, ?, 'PENDENTE', ?)
        ON CONFLICT(numero_lote) DO UPDATE SET
            tipo_lote=excluded.tipo_lote
    """, (numero_lote, tipo_lote, agora))

    for _, item in df_lote.iterrows():
        codigo = str(item.get("Código do Anúncio", "") or "")
        sku = str(item.get("SKU", "") or "")
        titulo = str(item.get("Título", "") or "")
        quantidade_esperada = int(float(item.get("Enviar", 0) or 0))
        endereco = str(item.get("ENDEREÇO", "") or "")
        lote_filete = str(item.get("Lote", "") or "")

        cursor.execute("""
            INSERT INTO lotes_itens (
                numero_lote, codigo, sku, titulo,
                quantidade_esperada, endereco, lote_filete
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(numero_lote, codigo) DO UPDATE SET
                sku=excluded.sku,
                titulo=excluded.titulo,
                quantidade_esperada=excluded.quantidade_esperada,
                endereco=excluded.endereco,
                lote_filete=excluded.lote_filete
        """, (
            numero_lote, codigo, sku, titulo,
            quantidade_esperada, endereco, lote_filete
        ))

    conn.commit()
    conn.close()


@app.route("/gerar-filete")
def gerar_filete():
    numero_lote = request.args.get("numero_lote", "").strip()
    tipo_lote = request.args.get("tipo_lote", "Diversos").strip() or "Diversos"

    dados = carregar_dados_base()
    df = pd.DataFrame(dados)

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, status, quantidade FROM status_cards")
    rows = cursor.fetchall()
    conn.close()

    mapa_status = {str(codigo): status or "" for codigo, status, quantidade in rows}
    mapa_quantidade = {str(codigo): quantidade or 0 for codigo, status, quantidade in rows}

    df["STATUS_CARD"] = df["Código do Anúncio"].astype(str).map(mapa_status).fillna("principal")
    df["Enviar"] = df["Código do Anúncio"].astype(str).map(mapa_quantidade).fillna(0)

    df = df[(df["STATUS_CARD"] == "enviando") & (df["Enviar"].astype(float) > 0)].copy()

    colunas_necessarias = [
        "Nickname",
        "Código do Anúncio",
        "SKU",
        "Título",
        "ENDEREÇO"
    ]

    for col in colunas_necessarias:
        if col not in df.columns:
            df[col] = ""

    df["Lote"] = f"Lote {tipo_lote} - #{numero_lote}" if numero_lote else f"Lote {tipo_lote}"

    if "ENDEREÇO" in df.columns:
        df = df.sort_values(by="ENDEREÇO", kind="stable")
        registrar_lote_conferencia(numero_lote, tipo_lote, df)

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        workbook = writer.book
        worksheet = workbook.create_sheet("Filete", 0)

        fill_topo = PatternFill(fill_type="solid", fgColor="D9E2F3")
        fill_label = PatternFill(fill_type="solid", fgColor="F4B183")
        fill_valor = PatternFill(fill_type="solid", fgColor="FFFDEB")

        border = Border(
            left=Side(style="thin", color="000000"),
            right=Side(style="thin", color="000000"),
            top=Side(style="thin", color="000000"),
            bottom=Side(style="thin", color="000000")
        )

        fonte_titulo = Font(bold=True, size=16)
        fonte_label = Font(bold=True, size=11)
        fonte_valor = Font(size=12)
        fonte_codigo = Font(bold=True, size=14)
        fonte_qtd = Font(bold=True, size=16)

        alinhamento_centro = Alignment(horizontal="center", vertical="center", wrap_text=True)
        alinhamento_esquerda = Alignment(horizontal="left", vertical="center", wrap_text=True)

        worksheet.column_dimensions["A"].width = 16
        worksheet.column_dimensions["B"].width = 18
        worksheet.column_dimensions["C"].width = 14
        worksheet.column_dimensions["D"].width = 52
        worksheet.column_dimensions["E"].width = 12
        worksheet.column_dimensions["F"].width = 30
        worksheet.column_dimensions["G"].width = 28

        linha = 1

        for _, item in df.iterrows():
            conta = str(item.get("Nickname", ""))
            codigo = str(item.get("Código do Anúncio", ""))
            sku = str(item.get("SKU", ""))
            titulo = str(item.get("Título", ""))
            enviar = int(float(item.get("Enviar", 0) or 0))
            endereco = str(item.get("ENDEREÇO", ""))
            lote = str(item.get("Lote", ""))

            worksheet.row_dimensions[linha].height = 28
            worksheet.row_dimensions[linha + 1].height = 26
            worksheet.row_dimensions[linha + 2].height = 26
            worksheet.row_dimensions[linha + 3].height = 42
            worksheet.row_dimensions[linha + 4].height = 28
            worksheet.row_dimensions[linha + 5].height = 12

            worksheet.merge_cells(start_row=linha, start_column=1, end_row=linha, end_column=4)
            worksheet.merge_cells(start_row=linha, start_column=5, end_row=linha, end_column=5)
            worksheet.merge_cells(start_row=linha, start_column=6, end_row=linha, end_column=7)

            c = worksheet.cell(row=linha, column=1)
            c.value = f"CONTA: {conta}"
            c.font = fonte_titulo
            c.fill = fill_topo
            c.alignment = alinhamento_esquerda

            c = worksheet.cell(row=linha, column=5)
            c.value = "ENVIAR"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha, column=6)
            c.value = "ENDEREÇO"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            worksheet.merge_cells(start_row=linha + 1, start_column=1, end_row=linha + 1, end_column=2)
            worksheet.merge_cells(start_row=linha + 1, start_column=3, end_row=linha + 1, end_column=4)
            worksheet.merge_cells(start_row=linha + 1, start_column=5, end_row=linha + 1, end_column=5)
            worksheet.merge_cells(start_row=linha + 1, start_column=6, end_row=linha + 1, end_column=7)

            c = worksheet.cell(row=linha + 1, column=1)
            c.value = "CÓDIGO"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=3)
            c.value = "SKU"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=5)
            c.value = enviar
            c.font = fonte_qtd
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=6)
            c.value = endereco
            c.font = fonte_valor
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            worksheet.merge_cells(start_row=linha + 2, start_column=1, end_row=linha + 2, end_column=2)
            worksheet.merge_cells(start_row=linha + 2, start_column=3, end_row=linha + 2, end_column=4)
            worksheet.merge_cells(start_row=linha + 2, start_column=5, end_row=linha + 2, end_column=7)

            c = worksheet.cell(row=linha + 2, column=1)
            c.value = codigo
            c.font = fonte_codigo
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 2, column=3)
            c.value = sku
            c.font = fonte_codigo
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 2, column=5)
            c.value = lote
            c.font = Font(bold=True, size=12)
            c.fill = fill_topo
            c.alignment = alinhamento_centro

            worksheet.merge_cells(start_row=linha + 3, start_column=1, end_row=linha + 3, end_column=7)
            c = worksheet.cell(row=linha + 3, column=1)
            c.value = titulo
            c.font = Font(bold=True, size=13)
            c.fill = fill_valor
            c.alignment = alinhamento_esquerda

            worksheet.merge_cells(start_row=linha + 4, start_column=1, end_row=linha + 4, end_column=7)
            c = worksheet.cell(row=linha + 4, column=1)
            c.value = "FILETE DE SEPARAÇÃO"
            c.font = Font(bold=True, size=11)
            c.fill = fill_topo
            c.alignment = alinhamento_centro

            for r in range(linha, linha + 5):
                for col in range(1, 8):
                    cell = worksheet.cell(row=r, column=col)
                    cell.border = border

            linha += 6

        worksheet.sheet_view.showGridLines = False
        worksheet.freeze_panes = "A1"

    output.seek(0)

    nome_arquivo = "filete.xlsx"
    if numero_lote:
        nome_arquivo = f"filete_{numero_lote}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=nome_arquivo,
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


@app.route("/comentarios")
def get_comentarios():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("SELECT sku, comentario FROM comentarios")
    rows = cursor.fetchall()

    conn.close()

    comentarios_dict = {str(sku): comentario for sku, comentario in rows}
    return jsonify(comentarios_dict)


@app.route("/comentarios-mlb")
def get_comentarios_mlb():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("SELECT codigo, comentario FROM comentarios_mlb")
    rows = cursor.fetchall()

    conn.close()

    comentarios_dict = {str(codigo): comentario for codigo, comentario in rows}
    return jsonify(comentarios_dict)


@app.route("/salvar-comentario", methods=["POST"])
def salvar_comentario():
    data = request.json
    sku = data.get("sku")
    comentario = data.get("comentario", "")

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO comentarios (sku, comentario)
        VALUES (?, ?)
        ON CONFLICT(sku) DO UPDATE SET comentario=excluded.comentario
    """, (sku, comentario))

    conn.commit()
    conn.close()

    return jsonify({"success": True})


@app.route("/salvar-comentario-mlb", methods=["POST"])
def salvar_comentario_mlb():
    data = request.json
    codigo = data.get("codigo")
    comentario = data.get("comentario", "")

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO comentarios_mlb (codigo, comentario)
        VALUES (?, ?)
        ON CONFLICT(codigo) DO UPDATE SET comentario=excluded.comentario
    """, (codigo, comentario))

    conn.commit()
    conn.close()

    return jsonify({"success": True})


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


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)