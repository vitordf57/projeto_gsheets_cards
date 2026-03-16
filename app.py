from flask import Flask, render_template, jsonify, request, send_file
import pandas as pd
import sqlite3
import time
from io import BytesIO
from collections import Counter
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
import os
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER_CONFERENCIA = os.path.join("static", "uploads", "conferencia")
os.makedirs(UPLOAD_FOLDER_CONFERENCIA, exist_ok=True)

app.config["UPLOAD_FOLDER_CONFERENCIA"] = UPLOAD_FOLDER_CONFERENCIA

CSV_URL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=0"
CSV_URL_FULL = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=184771586"

CACHE_TTL = 300

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


@app.route("/salvar-conferencia-item", methods=["POST"])
def salvar_conferencia_item():
    numero_lote = request.form.get("numero_lote", "").strip()
    codigo = request.form.get("codigo", "").strip()
    sku = request.form.get("sku", "").strip()
    observacao = request.form.get("observacao", "").strip()

    try:
        quantidade_conferida = int(request.form.get("quantidade_conferida", 0))
    except:
        quantidade_conferida = 0

    foto = request.files.get("foto")
    foto_path = None

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT foto_path
        FROM conferencia_itens
        WHERE numero_lote = ? AND codigo = ?
    """, (numero_lote, codigo))
    registro_existente = cursor.fetchone()

    foto_path_existente = None
    if registro_existente:
        foto_path_existente = registro_existente["foto_path"]

    if foto and foto.filename:
        nome_original = secure_filename(foto.filename)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        nome_arquivo = f"{numero_lote}_{codigo}_{timestamp}_{nome_original}"
        caminho_arquivo = os.path.join(app.config["UPLOAD_FOLDER_CONFERENCIA"], nome_arquivo)
        foto.save(caminho_arquivo)
        foto_path = os.path.join("static", "uploads", "conferencia", nome_arquivo).replace("\\", "/")
    else:
        foto_path = foto_path_existente

    cursor.execute("""
        SELECT quantidade_esperada
        FROM lotes_itens
        WHERE numero_lote = ? AND codigo = ?
    """, (numero_lote, codigo))
    item_lote = cursor.fetchone()

    quantidade_esperada = 0
    if item_lote:
        try:
            quantidade_esperada = int(item_lote["quantidade_esperada"] or 0)
        except:
            quantidade_esperada = 0

    if quantidade_conferida <= 0:
        status_item = "PENDENTE"
    elif quantidade_conferida != quantidade_esperada:
        status_item = "DIVERGENTE"
    else:
        status_item = "OK"

    conferido_em = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO conferencia_itens (
            numero_lote,
            codigo,
            sku,
            quantidade_conferida,
            foto_path,
            status_item,
            observacao,
            conferido_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(numero_lote, codigo) DO UPDATE SET
            sku=excluded.sku,
            quantidade_conferida=excluded.quantidade_conferida,
            foto_path=excluded.foto_path,
            status_item=excluded.status_item,
            observacao=excluded.observacao,
            conferido_em=excluded.conferido_em
    """, (
        numero_lote,
        codigo,
        sku,
        quantidade_conferida,
        foto_path,
        status_item,
        observacao,
        conferido_em
    ))

    cursor.execute("""
        SELECT COUNT(*)
        FROM lotes_itens
        WHERE numero_lote = ?
    """, (numero_lote,))
    total_itens = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM conferencia_itens
        WHERE numero_lote = ?
    """, (numero_lote,))
    total_conferidos = cursor.fetchone()[0]

    novo_status_lote = "PENDENTE"
    data_fechamento = None

    if total_itens > 0 and total_conferidos >= total_itens:
        cursor.execute("""
            SELECT COUNT(*)
            FROM conferencia_itens
            WHERE numero_lote = ? AND status_item = 'DIVERGENTE'
        """, (numero_lote,))
        qtd_divergentes = cursor.fetchone()[0]

        if qtd_divergentes > 0:
            novo_status_lote = "CONFERIDO COM DIVERGÊNCIA"
        else:
            novo_status_lote = "CONFERIDO"

        data_fechamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        UPDATE lotes_conferencia
        SET status = ?, data_fechamento = ?
        WHERE numero_lote = ?
    """, (novo_status_lote, data_fechamento, numero_lote))

    conn.commit()
    conn.close()

    return conferencia_lote(numero_lote)


def init_db():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS status_cards (
            codigo TEXT PRIMARY KEY,
            status TEXT,
            quantidade INTEGER DEFAULT 0,
            estrategia TEXT DEFAULT ''
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historico_filetes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_lote TEXT,
            tipo_lote TEXT,
            codigo TEXT,
            sku TEXT,
            titulo TEXT,
            nickname TEXT,
            quantidade INTEGER DEFAULT 0,
            endereco TEXT,
            lote_filete TEXT,
            data_geracao TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lotes_envio (
            numero_lote TEXT PRIMARY KEY,
            tipo_lote TEXT,
            total_mlbs INTEGER DEFAULT 0,
            total_pecas INTEGER DEFAULT 0,
            status TEXT DEFAULT 'CRIADO',
            responsavel TEXT DEFAULT '',
            transportadora TEXT DEFAULT '',
            observacao TEXT DEFAULT '',
            prioridade TEXT DEFAULT '',
            data_envio TEXT DEFAULT '',
            status_expedicao TEXT DEFAULT 'AGUARDANDO',
            status_ecommerce TEXT DEFAULT 'AGUARDANDO',
            origem TEXT DEFAULT 'MANUAL',
            data_criacao TEXT
        )
    """)

    cursor.execute("PRAGMA table_info(status_cards)")
    colunas_status = [col[1] for col in cursor.fetchall()]

    if "motivo_envio" not in colunas_status:
        cursor.execute("ALTER TABLE status_cards ADD COLUMN motivo_envio TEXT DEFAULT ''")

    if "quantidade" not in colunas_status:
        cursor.execute("ALTER TABLE status_cards ADD COLUMN quantidade INTEGER DEFAULT 0")

    if "estrategia" not in colunas_status:
        cursor.execute("ALTER TABLE status_cards ADD COLUMN estrategia TEXT DEFAULT ''")

    cursor.execute("PRAGMA table_info(lotes_envio)")
    colunas_lotes_envio = [col[1] for col in cursor.fetchall()]

    if "tipo_lote" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN tipo_lote TEXT")

    if "total_mlbs" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN total_mlbs INTEGER DEFAULT 0")

    if "total_pecas" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN total_pecas INTEGER DEFAULT 0")

    if "status" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN status TEXT DEFAULT 'CRIADO'")

    if "responsavel" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN responsavel TEXT DEFAULT ''")

    if "transportadora" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN transportadora TEXT DEFAULT ''")

    if "observacao" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN observacao TEXT DEFAULT ''")

    if "prioridade" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN prioridade TEXT DEFAULT ''")

    if "data_envio" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN data_envio TEXT DEFAULT ''")

    if "status_expedicao" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN status_expedicao TEXT DEFAULT 'AGUARDANDO'")

    if "status_ecommerce" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN status_ecommerce TEXT DEFAULT 'AGUARDANDO'")

    if "origem" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN origem TEXT DEFAULT 'MANUAL'")

    if "data_criacao" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN data_criacao TEXT")

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
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            numero_lote,
            tipo_lote,
            total_mlbs,
            total_pecas,
            status,
            responsavel,
            transportadora,
            observacao,
            prioridade,
            data_envio,
            status_expedicao,
            status_ecommerce,
            origem,
            data_criacao
        FROM lotes_envio
        ORDER BY data_criacao DESC, numero_lote DESC
    """)

    lotes = cursor.fetchall()
    conn.close()

    total_lotes = len(lotes)
    total_mlbs = 0
    total_pecas = 0

    for lote in lotes:
        try:
            total_mlbs += int(lote["total_mlbs"] or 0)
        except:
            pass

        try:
            total_pecas += int(lote["total_pecas"] or 0)
        except:
            pass

    return render_template(
        "metricas_full.html",
        lotes=lotes,
        total_lotes=total_lotes,
        total_mlbs=total_mlbs,
        total_pecas=total_pecas
    )


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
        cursor.execute("SELECT codigo, status, quantidade, estrategia, motivo_envio FROM status_cards")
        rows = cursor.fetchall()

        mapa_quantidade = {str(codigo): quantidade or 0 for codigo, status, quantidade, estrategia, motivo_envio in rows}
        mapa_status = {str(codigo): status or "" for codigo, status, quantidade, estrategia, motivo_envio in rows}
        mapa_estrategia = {str(codigo): estrategia or "" for codigo, status, quantidade, estrategia, motivo_envio in rows}
        mapa_motivo_envio = {str(codigo): motivo_envio or "" for codigo, status, quantidade, estrategia, motivo_envio in rows}
    except:
        mapa_quantidade = {}
        mapa_status = {}
        mapa_estrategia = {}
        mapa_motivo_envio = {}

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
    df["Estratégia"] = df["Código do Anúncio"].astype(str).map(mapa_estrategia).fillna("")
    df["Motivo do Envio"] = df["Código do Anúncio"].astype(str).map(mapa_motivo_envio).fillna("")
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
            if status == "filetado":
                return "historico"
            return "principal"

        df = df[df["Código do Anúncio"].apply(destino) == tela]

    colunas_exportar = [
        "Nickname",
        "Código do Anúncio",
        "SKU",
        "Título",
        "LOTE",
        "Quantidade para Enviar",
        "Estratégia",
        "Motivo do Envio",
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


def atualizar_lote_envio_existente(numero_lote, tipo_lote, df_lote):
    if not numero_lote:
        raise ValueError("Número do lote não informado.")

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT numero_lote
        FROM lotes_envio
        WHERE numero_lote = ?
    """, (numero_lote,))
    lote_existente = cursor.fetchone()

    if not lote_existente:
        conn.close()
        raise ValueError(f"Lote {numero_lote} não foi criado manualmente.")

    total_mlbs = 0
    total_pecas = 0

    if df_lote is not None and not df_lote.empty:
        total_mlbs = len(df_lote)
        try:
            total_pecas = int(pd.to_numeric(df_lote["Enviar"], errors="coerce").fillna(0).sum())
        except:
            total_pecas = 0

    cursor.execute("""
        UPDATE lotes_envio
        SET
            tipo_lote = ?,
            total_mlbs = ?,
            total_pecas = ?
        WHERE numero_lote = ?
    """, (
        tipo_lote,
        total_mlbs,
        total_pecas,
        numero_lote
    ))

    conn.commit()
    conn.close()


def salvar_historico_e_finalizar_envio(numero_lote, tipo_lote, df_lote):
    if df_lote.empty:
        return

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, item in df_lote.iterrows():
        codigo = str(item.get("Código do Anúncio", "") or "")
        sku = str(item.get("SKU", "") or "")
        titulo = str(item.get("Título", "") or "")
        nickname = str(item.get("Nickname", "") or "")
        endereco = str(item.get("ENDEREÇO", "") or "")
        lote_filete = str(item.get("Lote", "") or "")
        quantidade = int(float(item.get("Enviar", 0) or 0))

        cursor.execute("""
            INSERT INTO historico_filetes (
                numero_lote, tipo_lote, codigo, sku, titulo,
                nickname, quantidade, endereco, lote_filete, data_geracao
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            numero_lote,
            tipo_lote,
            codigo,
            sku,
            titulo,
            nickname,
            quantidade,
            endereco,
            lote_filete,
            agora
        ))

        cursor.execute("""
            UPDATE status_cards
            SET status = ?, quantidade = 0
            WHERE codigo = ?
        """, ("filetado", codigo))

    conn.commit()
    conn.close()


@app.route("/gerar-filete")
def gerar_filete():
    numero_lote = request.args.get("numero_lote", "").strip()
    tipo_lote = request.args.get("tipo_lote", "Diversos").strip() or "Diversos"

    if not numero_lote:
        return "Informe um número de lote válido.", 400

    dados = carregar_dados_base()
    df = pd.DataFrame(dados)

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()
    cursor.execute("SELECT codigo, status, quantidade, estrategia FROM status_cards")
    rows = cursor.fetchall()
    conn.close()

    mapa_status = {str(codigo): status or "" for codigo, status, quantidade, estrategia in rows}
    mapa_quantidade = {str(codigo): quantidade or 0 for codigo, status, quantidade, estrategia in rows}

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

    try:
        registrar_lote_conferencia(numero_lote, tipo_lote, df)
        atualizar_lote_envio_existente(numero_lote, tipo_lote, df)
        salvar_historico_e_finalizar_envio(numero_lote, tipo_lote, df)
    except ValueError as e:
        return str(e), 400

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
            worksheet.row_dimensions[linha + 2].height = 38
            worksheet.row_dimensions[linha + 3].height = 12

            worksheet.merge_cells(start_row=linha, start_column=1, end_row=linha, end_column=4)
            worksheet.merge_cells(start_row=linha, start_column=5, end_row=linha, end_column=7)

            c = worksheet.cell(row=linha, column=1)
            c.value = f"CONTA: {conta}"
            c.font = fonte_titulo
            c.fill = fill_topo
            c.alignment = alinhamento_esquerda

            c = worksheet.cell(row=linha, column=5)
            c.value = lote
            c.font = Font(bold=True, size=12)
            c.fill = fill_topo
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=1)
            c.value = "CÓDIGO"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=2)
            c.value = "SKU"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=3)
            c.value = "ENVIAR"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 1, column=4)
            c.value = "ENDEREÇO"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            worksheet.merge_cells(start_row=linha + 1, start_column=5, end_row=linha + 1, end_column=7)
            c = worksheet.cell(row=linha + 1, column=5)
            c.value = "TÍTULO"
            c.font = fonte_label
            c.fill = fill_label
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 2, column=1)
            c.value = codigo
            c.font = fonte_codigo
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 2, column=2)
            c.value = sku
            c.font = fonte_codigo
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 2, column=3)
            c.value = enviar
            c.font = fonte_qtd
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            c = worksheet.cell(row=linha + 2, column=4)
            c.value = endereco
            c.font = fonte_valor
            c.fill = fill_valor
            c.alignment = alinhamento_centro

            worksheet.merge_cells(start_row=linha + 2, start_column=5, end_row=linha + 2, end_column=7)
            c = worksheet.cell(row=linha + 2, column=5)
            c.value = titulo
            c.font = Font(bold=True, size=11)
            c.fill = fill_valor
            c.alignment = alinhamento_esquerda

            for r in range(linha, linha + 3):
                for col in range(1, 8):
                    cell = worksheet.cell(row=r, column=col)
                    cell.border = border

            linha += 4

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

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        SELECT status, quantidade, estrategia, motivo_envio
        FROM status_cards
        WHERE codigo = ?
    """, (codigo,))
    existente = cursor.fetchone()

    status_atual = "principal"
    quantidade_atual = 0
    estrategia_atual = ""
    motivo_atual = ""

    if existente:
        status_atual = existente[0] or "principal"
        quantidade_atual = existente[1] or 0
        estrategia_atual = existente[2] or ""
        motivo_atual = existente[3] or ""

    status = data.get("status", status_atual)
    quantidade = data.get("quantidade", quantidade_atual)
    estrategia = data.get("estrategia", estrategia_atual)
    motivo_envio = data.get("motivo_envio", motivo_atual)

    try:
        quantidade = int(quantidade)
    except:
        quantidade = 0

    cursor.execute("""
        INSERT INTO status_cards (codigo, status, quantidade, estrategia, motivo_envio)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(codigo) DO UPDATE SET
            status=excluded.status,
            quantidade=excluded.quantidade,
            estrategia=excluded.estrategia,
            motivo_envio=excluded.motivo_envio
    """, (codigo, status, quantidade, estrategia, motivo_envio))

    conn.commit()
    conn.close()

    return jsonify({"success": True})


@app.route("/status")
def get_status():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("SELECT codigo, status, quantidade, estrategia, motivo_envio FROM status_cards")
    rows = cursor.fetchall()

    conn.close()

    status_dict = {}
    for codigo, status, quantidade, estrategia, motivo_envio in rows:
        status_dict[str(codigo)] = {
    "status": status,
    "quantidade": quantidade or 0,
    "estrategia": estrategia or "",
    "motivo_envio": motivo_envio or ""
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


@app.route("/salvar-lote-envio", methods=["POST"])
def salvar_lote_envio_manual():
    numero_lote = request.form.get("numero_lote", "").strip()
    tipo_lote = request.form.get("tipo_lote", "Diversos").strip() or "Diversos"
    total_mlbs = request.form.get("total_mlbs", 0)
    total_pecas = request.form.get("total_pecas", 0)
    status = request.form.get("status", "CRIADO").strip() or "CRIADO"
    responsavel = request.form.get("responsavel", "").strip()
    transportadora = request.form.get("transportadora", "").strip()
    observacao = request.form.get("observacao", "").strip()
    prioridade = request.form.get("prioridade", "").strip()
    data_envio = request.form.get("data_envio", "").strip()
    status_expedicao = request.form.get("status_expedicao", "AGUARDANDO").strip() or "AGUARDANDO"
    status_ecommerce = request.form.get("status_ecommerce", "AGUARDANDO").strip() or "AGUARDANDO"

    try:
        total_mlbs = int(total_mlbs)
    except:
        total_mlbs = 0

    try:
        total_pecas = int(total_pecas)
    except:
        total_pecas = 0

    if not numero_lote:
        return render_template("redirect.html", target_url="/metricas-full")

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("SELECT numero_lote FROM lotes_envio WHERE numero_lote = ?", (numero_lote,))
    existe = cursor.fetchone()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if existe:
        cursor.execute("""
            UPDATE lotes_envio
            SET
                tipo_lote = ?,
                total_mlbs = ?,
                total_pecas = ?,
                status = ?,
                responsavel = ?,
                transportadora = ?,
                observacao = ?,
                prioridade = ?,
                data_envio = ?,
                status_expedicao = ?,
                status_ecommerce = ?
            WHERE numero_lote = ?
        """, (
            tipo_lote, total_mlbs, total_pecas, status, responsavel,
            transportadora, observacao, prioridade, data_envio,
            status_expedicao, status_ecommerce, numero_lote
        ))
    else:
        cursor.execute("""
            INSERT INTO lotes_envio (
                numero_lote, tipo_lote, total_mlbs, total_pecas, status,
                responsavel, transportadora, observacao, prioridade,
                data_envio, status_expedicao, status_ecommerce,
                origem, data_criacao
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            numero_lote, tipo_lote, total_mlbs, total_pecas, status,
            responsavel, transportadora, observacao, prioridade,
            data_envio, status_expedicao, status_ecommerce,
            "MANUAL", agora
        ))

    conn.commit()
    conn.close()

    return render_template("redirect.html", target_url="/metricas-full")


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