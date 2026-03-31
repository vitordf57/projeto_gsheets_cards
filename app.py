from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from flask import Flask, render_template, jsonify, request, send_file, redirect
import pandas as pd
import sqlite3
import time
from io import BytesIO
from collections import Counter
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
import os
import json
from datetime import datetime
from werkzeug.utils import secure_filename
from calendar import month_name

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
        cache_dados = df
        cache_dados_ts = agora

    return cache_dados.copy()


def montar_mapa_base_por_codigo_sku(df_base):
    mapa_codigo = {}
    mapa_sku = {}

    if df_base is None or df_base.empty:
        return mapa_codigo, mapa_sku

    df_base = df_base.fillna("").copy()

    for _, row in df_base.iterrows():
        dados = {str(col): row[col] for col in df_base.columns}

        codigo = str(row.get("Código do Anúncio", "") or "").strip()
        sku = str(row.get("SKU", "") or "").strip()

        if codigo and codigo not in mapa_codigo:
            mapa_codigo[codigo] = dados

        if sku and sku not in mapa_sku:
            mapa_sku[sku] = dados

    return mapa_codigo, mapa_sku


def enriquecer_itens_lote_com_base(itens):
    if not itens:
        return itens

    try:
        df_base = carregar_dados_base()
    except:
        return itens

    mapa_codigo, mapa_sku = montar_mapa_base_por_codigo_sku(df_base)

    itens_enriquecidos = []
    for item in itens:
        item_dict = dict(item)

        codigo = str(item_dict.get("codigo", "") or "").strip()
        sku = str(item_dict.get("sku", "") or "").strip()

        base_item = mapa_codigo.get(codigo) or mapa_sku.get(sku) or {}

        dados_json_atual = {}
        try:
            dados_json_atual = json.loads(item_dict.get("dados_json") or "{}")
            if not isinstance(dados_json_atual, dict):
                dados_json_atual = {}
        except:
            dados_json_atual = {}

        dados_mesclados = {}
        if base_item:
            for k, v in base_item.items():
                dados_mesclados[str(k)] = v
        for k, v in dados_json_atual.items():
            if v not in [None, "", []]:
                dados_mesclados[str(k)] = v

        if codigo:
            dados_mesclados["Código do Anúncio"] = codigo
        if sku:
            dados_mesclados["SKU"] = sku
        if item_dict.get("titulo"):
            dados_mesclados["Título"] = item_dict.get("titulo")
        elif dados_mesclados.get("Título"):
            item_dict["titulo"] = dados_mesclados.get("Título")

        if item_dict.get("nickname"):
            dados_mesclados["Nickname"] = item_dict.get("nickname")
        elif dados_mesclados.get("Nickname"):
            item_dict["nickname"] = dados_mesclados.get("Nickname")

        if item_dict.get("endereco"):
            dados_mesclados["ENDEREÇO"] = item_dict.get("endereco")
        elif dados_mesclados.get("ENDEREÇO"):
            item_dict["endereco"] = dados_mesclados.get("ENDEREÇO")

        if item_dict.get("lote_filete"):
            dados_mesclados["LOTE"] = item_dict.get("lote_filete")
        elif dados_mesclados.get("LOTE"):
            item_dict["lote_filete"] = dados_mesclados.get("LOTE")

        item_dict["dados_json"] = json.dumps(dados_mesclados, ensure_ascii=False)
        itens_enriquecidos.append(item_dict)

    return itens_enriquecidos


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


TIMELINE_ETAPAS = [
    "MLBS EM ANÁLISE",
    "CRIAR PEDIDO SIGNUS",
    "GERAR LOTE MELI",
    "AGENDAR DATA",
    "CONFERIR QUANTIDADES",
    "EM SEPARAÇÃO",
    "EM CONFERÊNCIA",
    "EMBALAGEM",
    "CONFERIR CAIXAS MASTER",
    "AGUARDANDO COLETA",
    "COLETADO"
]


ETAPA_TO_STATUS_FIELD = {
    "MLBS EM ANÁLISE": "status_ecommerce",
    "CRIAR PEDIDO SIGNUS": "status_ecommerce",
    "GERAR LOTE MELI": "status_ecommerce",
    "AGENDAR DATA": "status_ecommerce",
    "CONFERIR QUANTIDADES": "status_ecommerce",
    "EM SEPARAÇÃO": "status_expedicao",
    "EM CONFERÊNCIA": "status_expedicao",
    "EMBALAGEM": "status_expedicao",
    "CONFERIR CAIXAS MASTER": "status_expedicao",
    "AGUARDANDO COLETA": "status_expedicao",
    "COLETADO": "status_expedicao"
}


def agora_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def agora_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_data_hora(valor):
    texto = str(valor or "").strip()
    if not texto:
        return None

    formatos = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y"
    ]

    for formato in formatos:
        try:
            return datetime.strptime(texto, formato)
        except:
            pass

    return None


def formatar_data_hora_br(valor, vazio="-"):
    dt = parse_data_hora(valor)
    if not dt:
        return vazio
    return dt.strftime("%d/%m/%Y %H:%M")


def formatar_data_br(valor, vazio="-"):
    dt = parse_data_hora(valor)
    if not dt:
        return vazio
    return dt.strftime("%d/%m/%Y")


def timeline_vazio():
    return {etapa: "" for etapa in TIMELINE_ETAPAS}


def carregar_timeline_json(valor):
    timeline = timeline_vazio()

    if not valor:
        return timeline

    try:
        dados = json.loads(valor)
        if isinstance(dados, dict):
            for etapa in TIMELINE_ETAPAS:
                timeline[etapa] = str(dados.get(etapa, "") or "").strip()
    except:
        pass

    return timeline


def timeline_para_json(timeline):
    dados = {etapa: str(timeline.get(etapa, "") or "").strip() for etapa in TIMELINE_ETAPAS}
    return json.dumps(dados, ensure_ascii=False)


def indice_etapa(etapa):
    try:
        return TIMELINE_ETAPAS.index(etapa)
    except ValueError:
        return 0


def sincronizar_timeline_ate_etapa(timeline, etapa_atual, timestamp=None):
    if timestamp is None:
        timestamp = agora_str()

    idx = indice_etapa(etapa_atual)
    for i, etapa in enumerate(TIMELINE_ETAPAS):
        if i <= idx and not str(timeline.get(etapa, "") or "").strip():
            timeline[etapa] = timestamp
    return timeline


def obter_status_abertura_por_etapa(etapa_atual):
    if indice_etapa(etapa_atual) <= indice_etapa("CONFERIR QUANTIDADES"):
        return "ABERTO"
    return "FECHADO"


def calcular_lead_time_segundos(timeline):
    momentos = []
    for etapa in TIMELINE_ETAPAS:
        dt = parse_data_hora(timeline.get(etapa, ""))
        if dt:
            momentos.append(dt)

    if not momentos:
        return 0

    inicio = min(momentos)
    fim = max(momentos)
    return max(0, int((fim - inicio).total_seconds()))


def formatar_duracao_humana(segundos):
    segundos = int(segundos or 0)
    if segundos <= 0:
        return "-"

    dias = segundos // 86400
    horas = (segundos % 86400) // 3600
    minutos = (segundos % 3600) // 60

    partes = []
    if dias:
        partes.append(f"{dias}d")
    if horas:
        partes.append(f"{horas}h")
    if minutos or not partes:
        partes.append(f"{minutos}min")
    return " ".join(partes)


def resumir_contas_lote(itens_lote):
    mapa = {}
    for item in itens_lote:
        conta = str(item["nickname"] or "SEM CONTA").strip() or "SEM CONTA"
        if conta not in mapa:
            mapa[conta] = {"mlbs": set(), "pecas": 0}
        mapa[conta]["mlbs"].add(str(item["codigo"] or ""))
        try:
            mapa[conta]["pecas"] += int(item["quantidade"] or 0)
        except:
            pass

    linhas = []
    for conta, dados in sorted(mapa.items(), key=lambda x: x[0]):
        linhas.append({
            "conta": conta,
            "mlbs": len(dados["mlbs"]),
            "pecas": int(dados["pecas"])
        })
    return linhas


def construir_timeline_exibicao(timeline, etapa_atual):
    idx_atual = indice_etapa(etapa_atual)
    etapas = []

    for idx, etapa in enumerate(TIMELINE_ETAPAS):
        timestamp = str(timeline.get(etapa, "") or "").strip()
        if timestamp:
            estado = "concluida"
        elif idx == idx_atual:
            estado = "atual"
        else:
            estado = "pendente"

        if idx < idx_atual and not timestamp:
            estado = "concluida"

        etapas.append({
            "nome": etapa,
            "horario": formatar_data_hora_br(timestamp, "-"),
            "estado": estado
        })

    return etapas


def atualizar_statuss_por_etapa(cursor, numero_lote, etapa_atual):
    status = obter_status_abertura_por_etapa(etapa_atual)
    status_expedicao = "AGUARDANDO"
    status_ecommerce = "AGUARDANDO"

    campo = ETAPA_TO_STATUS_FIELD.get(etapa_atual)
    if campo == "status_expedicao":
        status_expedicao = etapa_atual
    else:
        status_ecommerce = etapa_atual

    cursor.execute(
        """
        UPDATE lotes_envio
        SET status = ?,
            status_expedicao = ?,
            status_ecommerce = ?
        WHERE numero_lote = ?
        """,
        (status, status_expedicao, status_ecommerce, numero_lote)
    )


def garantir_timeline_lote(cursor, numero_lote, etapa_atual=None):
    cursor.execute(
        "SELECT timeline_json, etapa_atual FROM lotes_envio WHERE numero_lote = ?",
        (numero_lote,)
    )
    row = cursor.fetchone()
    if not row:
        return timeline_vazio(), etapa_atual or TIMELINE_ETAPAS[0]

    timeline = carregar_timeline_json(row[0] if not isinstance(row, sqlite3.Row) else row["timeline_json"])
    etapa_salva = (row[1] if not isinstance(row, sqlite3.Row) else row["etapa_atual"]) or TIMELINE_ETAPAS[0]
    etapa_final = etapa_atual or etapa_salva
    return timeline, etapa_final


def atualizar_etapa_lote(numero_lote, etapa_atual, data_coleta_agendada=""):
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    timeline, _ = garantir_timeline_lote(cursor, numero_lote, etapa_atual)
    timestamp = agora_str()
    timeline = sincronizar_timeline_ate_etapa(timeline, etapa_atual, timestamp)

    cursor.execute(
        """
        UPDATE lotes_envio
        SET etapa_atual = ?,
            timeline_json = ?,
            data_coleta_agendada = COALESCE(NULLIF(?, ''), data_coleta_agendada)
        WHERE numero_lote = ?
        """,
        (etapa_atual, timeline_para_json(timeline), str(data_coleta_agendada or "").strip(), numero_lote)
    )
    atualizar_statuss_por_etapa(cursor, numero_lote, etapa_atual)

    conn.commit()
    conn.close()


def sincronizar_picking_itens(numero_lote, df_lote):
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    existentes = {}
    cursor.execute("SELECT codigo, coletado, coletado_em, observacao FROM lotes_picking_itens WHERE numero_lote = ?", (numero_lote,))
    for codigo, coletado, coletado_em, observacao in cursor.fetchall():
        existentes[str(codigo)] = {
            "coletado": int(coletado or 0),
            "coletado_em": coletado_em or "",
            "observacao": observacao or ""
        }

    for _, item in df_lote.iterrows():
        codigo = str(item.get("Código do Anúncio", "") or item.get("codigo", "") or "")
        sku = str(item.get("SKU", "") or item.get("sku", "") or "")
        endereco = str(item.get("ENDEREÇO", "") or item.get("endereco", "") or "")
        titulo = str(item.get("Título", "") or item.get("titulo", "") or "")
        produto = str(item.get("PRODUTO", "") or item.get("produto", "") or titulo or "")
        conta = str(item.get("Nickname", "") or item.get("nickname", "") or "")
        selo = str(item.get("SELO", "") or item.get("selo", "") or "")
        quantidade_base = item.get("Enviar", item.get("quantidade", 0))
        try:
            quantidade = int(float(quantidade_base or 0))
        except:
            quantidade = 0

        coletado = existentes.get(codigo, {}).get("coletado", 0)
        coletado_em = existentes.get(codigo, {}).get("coletado_em", "")
        observacao = existentes.get(codigo, {}).get("observacao", "")

        cursor.execute(
            """
            INSERT INTO lotes_picking_itens (
                numero_lote, codigo, sku, endereco, titulo, produto, conta, selo, quantidade, observacao, coletado, coletado_em
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(numero_lote, codigo) DO UPDATE SET
                sku = excluded.sku,
                endereco = excluded.endereco,
                titulo = excluded.titulo,
                produto = excluded.produto,
                conta = excluded.conta,
                selo = excluded.selo,
                quantidade = excluded.quantidade,
                observacao = ?,
                coletado = ?,
                coletado_em = ?
            """,
            (numero_lote, codigo, sku, endereco, titulo, produto, conta, selo, quantidade, observacao, coletado, coletado_em, observacao, coletado, coletado_em)
        )

    conn.commit()
    conn.close()

@app.route("/api/historico-mensal")
def api_historico_mensal():
    mlb = str(request.args.get("mlb", "")).strip().upper()
    mes = request.args.get("mes", "").strip()
    ano = request.args.get("ano", "").strip()

    if not mlb or not mes or not ano:
        return jsonify({
            "ok": False,
            "erro": "Parâmetros obrigatórios: mlb, mes e ano."
        }), 400

    try:
        mes = int(mes)
        ano = int(ano)
    except:
        return jsonify({
            "ok": False,
            "erro": "Mês e ano inválidos."
        }), 400

    csv_url_acompanhamento = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=1492834688"

    try:
        df = pd.read_csv(csv_url_acompanhamento)
        df = df.fillna("")

        df.columns = [str(c).strip() for c in df.columns]

        coluna_mlb = "# de anúncio"
        coluna_data = "DATA DA VENDA"
        coluna_qtd = "UNIDADE VENDIDA"

        if coluna_mlb not in df.columns or coluna_data not in df.columns or coluna_qtd not in df.columns:
            return jsonify({
                "ok": False,
                "erro": "Colunas esperadas não encontradas na planilha."
            }), 400

        df[coluna_mlb] = df[coluna_mlb].astype(str).str.strip().str.upper()
        df[coluna_data] = pd.to_datetime(df[coluna_data], format="%d/%m/%Y", errors="coerce")
        df[coluna_qtd] = pd.to_numeric(df[coluna_qtd], errors="coerce").fillna(0)

        df_filtrado = df[
            (df[coluna_mlb] == mlb) &
            (df[coluna_data].dt.month == mes) &
            (df[coluna_data].dt.year == ano)
        ]

        total_unidades = int(df_filtrado[coluna_qtd].sum())
        total_pedidos = int(len(df_filtrado))

        return jsonify({
            "ok": True,
            "mlb": mlb,
            "mes": mes,
            "ano": ano,
            "mes_nome": month_name[mes],
            "unidades_vendidas": total_unidades,
            "pedidos": total_pedidos
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "erro": f"Erro ao processar histórico mensal: {str(e)}"
        }), 500

@app.route("/api/historico-mensal-resumo")
def api_historico_mensal_resumo():
    mlb = str(request.args.get("mlb", "")).strip().upper()

    if not mlb:
        return jsonify({"ok": False})

    url = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=1492834688"

    df = pd.read_csv(url)
    df = df.fillna("")

    df["# de anúncio"] = df["# de anúncio"].astype(str).str.upper().str.strip()
    df["DATA DA VENDA"] = pd.to_datetime(df["DATA DA VENDA"], format="%d/%m/%Y", errors="coerce")
    df["UNIDADE VENDIDA"] = pd.to_numeric(df["UNIDADE VENDIDA"], errors="coerce").fillna(0)

    df = df[df["# de anúncio"] == mlb]

    df["MES"] = df["DATA DA VENDA"].dt.month

    resumo = df.groupby("MES")["UNIDADE VENDIDA"].sum().to_dict()

    nomes_meses = {
        1:"JANEIRO",2:"FEVEREIRO",3:"MARÇO",4:"ABRIL",
        5:"MAIO",6:"JUNHO",7:"JULHO",8:"AGOSTO",
        9:"SETEMBRO",10:"OUTUBRO",11:"NOVEMBRO",12:"DEZEMBRO"
    }

    resultado = []

    for i in range(1,13):
        resultado.append({
            "mes": nomes_meses[i],
            "valor": int(resumo.get(i, 0))
        })

    return jsonify({
        "ok": True,
        "dados": resultado
    })

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
        CREATE TABLE IF NOT EXISTS comentarios_mlb_chat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT,
            mensagem TEXT,
            data_hora TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lote_comentarios_chat (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_lote TEXT,
            codigo TEXT,
            mensagem TEXT,
            data_hora TEXT
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lotes_envio_itens_snapshot (
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
            estrategia TEXT DEFAULT '',
            motivo_envio TEXT DEFAULT '',
            comentario_mlb TEXT DEFAULT '',
            dados_json TEXT,
            data_geracao TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lotes_picking_itens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_lote TEXT,
            codigo TEXT,
            sku TEXT,
            endereco TEXT,
            titulo TEXT DEFAULT '',
            produto TEXT DEFAULT '',
            conta TEXT DEFAULT '',
            selo TEXT DEFAULT '',
            quantidade INTEGER DEFAULT 0,
            observacao TEXT DEFAULT '',
            coletado INTEGER DEFAULT 0,
            coletado_em TEXT DEFAULT '',
            quantidade_informada INTEGER DEFAULT 0,
            divergencia INTEGER DEFAULT 0,
            divergencia_em TEXT DEFAULT '',
            UNIQUE(numero_lote, codigo)
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

    cursor.execute("PRAGMA table_info(lotes_picking_itens)")
    colunas_picking = [col[1] for col in cursor.fetchall()]

    if "titulo" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN titulo TEXT DEFAULT ''")

    if "produto" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN produto TEXT DEFAULT ''")

    if "conta" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN conta TEXT DEFAULT ''")

    if "selo" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN selo TEXT DEFAULT ''")

    if "observacao" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN observacao TEXT DEFAULT ''")

    if "quantidade_informada" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN quantidade_informada INTEGER DEFAULT 0")

    if "divergencia" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN divergencia INTEGER DEFAULT 0")


    cursor.execute("PRAGMA table_info(lotes_envio_itens_snapshot)")
    colunas_snapshot = [col[1] for col in cursor.fetchall()]

    if "sku_info" not in colunas_snapshot:
        cursor.execute("ALTER TABLE lotes_envio_itens_snapshot ADD COLUMN sku_info TEXT DEFAULT ''")

    cursor.execute("PRAGMA table_info(historico_filetes)")
    colunas_historico = [col[1] for col in cursor.fetchall()]

    if "sku_info" not in colunas_historico:
        cursor.execute("ALTER TABLE historico_filetes ADD COLUMN sku_info TEXT DEFAULT ''")

    if "comentario_mlb" not in colunas_historico:
        cursor.execute("ALTER TABLE historico_filetes ADD COLUMN comentario_mlb TEXT DEFAULT ''")

    if "estrategia" not in colunas_historico:
        cursor.execute("ALTER TABLE historico_filetes ADD COLUMN estrategia TEXT DEFAULT ''")

    if "divergencia_em" not in colunas_picking:
        cursor.execute("ALTER TABLE lotes_picking_itens ADD COLUMN divergencia_em TEXT DEFAULT ''")

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

    if "timeline_json" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN timeline_json TEXT DEFAULT ''")

    if "etapa_atual" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN etapa_atual TEXT DEFAULT 'MLBS EM ANÁLISE'")

    if "data_coleta_agendada" not in colunas_lotes_envio:
        cursor.execute("ALTER TABLE lotes_envio ADD COLUMN data_coleta_agendada TEXT DEFAULT ''")

    conn.commit()
    conn.close()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/home")
def home():
    return render_template("home.html")

def garantir_tabela_lotes_envio_snapshot():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lotes_envio_itens_snapshot (
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
            estrategia TEXT DEFAULT '',
            motivo_envio TEXT DEFAULT '',
            comentario_mlb TEXT DEFAULT '',
            dados_json TEXT,
            data_geracao TEXT
        )
    """)

    conn.commit()
    conn.close()

@app.route("/metricas-full")
def metricas_full():
    garantir_tabela_lotes_envio_snapshot()

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM lotes_envio
        ORDER BY data_criacao DESC, numero_lote DESC
    """)
    lotes_rows = cursor.fetchall()

    cursor.execute("""
        SELECT *
        FROM lotes_envio_itens_snapshot
        ORDER BY numero_lote DESC, endereco, sku, codigo
    """)
    itens_snapshot = cursor.fetchall()

    if not itens_snapshot:
        cursor.execute("""
            SELECT
                li.numero_lote,
                COALESCE(le.tipo_lote, lc.tipo_lote, '') AS tipo_lote,
                li.codigo,
                li.sku,
                li.titulo,
                '' AS nickname,
                li.quantidade_esperada AS quantidade,
                li.endereco,
                li.lote_filete,
                '' AS estrategia,
                '' AS motivo_envio,
                '' AS comentario_mlb,
                '' AS dados_json,
                COALESCE(le.data_criacao, lc.data_criacao, '') AS data_geracao
            FROM lotes_itens li
            LEFT JOIN lotes_envio le
                ON le.numero_lote = li.numero_lote
            LEFT JOIN lotes_conferencia lc
                ON lc.numero_lote = li.numero_lote
            ORDER BY li.numero_lote DESC, li.endereco, li.sku, li.codigo
        """)
        itens_snapshot = cursor.fetchall()

    itens_snapshot = enriquecer_itens_lote_com_base([dict(item) for item in itens_snapshot])

    lotes_itens_map = {}
    for item in itens_snapshot:
        numero_lote = item["numero_lote"]
        lotes_itens_map.setdefault(numero_lote, []).append(item)

    total_lotes = len(lotes_rows)
    total_mlbs = sum(int(lote["total_mlbs"] or 0) for lote in lotes_rows)
    total_pecas = sum(int(lote["total_pecas"] or 0) for lote in lotes_rows)

    contas_total = {}
    for item in itens_snapshot:
        conta = str(item["nickname"] or "SEM CONTA").strip() or "SEM CONTA"
        if conta not in contas_total:
            contas_total[conta] = {"mlbs": set(), "pecas": 0}
        contas_total[conta]["mlbs"].add(str(item["codigo"] or ""))
        try:
            contas_total[conta]["pecas"] += int(item["quantidade"] or 0)
        except:
            pass

    contas_resumo = []
    for conta, dados in sorted(contas_total.items(), key=lambda x: x[0]):
        contas_resumo.append({
            "conta": conta,
            "mlbs": len(dados["mlbs"]),
            "pecas": int(dados["pecas"])
        })

    lotes = []
    total_abertos = 0
    total_fechados = 0
    soma_lead_time = 0
    lotes_com_lead_time = 0

    for lote in lotes_rows:
        lote_dict = dict(lote)
        numero_lote = lote_dict["numero_lote"]
        itens_lote = lotes_itens_map.get(numero_lote, [])
        timeline = carregar_timeline_json(lote_dict.get("timeline_json", ""))
        etapa_atual = lote_dict.get("etapa_atual") or TIMELINE_ETAPAS[0]

        if not any(str(v or "").strip() for v in timeline.values()):
            criacao = lote_dict.get("data_criacao") or agora_str()
            timeline[etapa_atual] = criacao
            lote_dict["timeline_json"] = timeline_para_json(timeline)

        status_abertura = obter_status_abertura_por_etapa(etapa_atual)
        if status_abertura == "ABERTO":
            total_abertos += 1
        else:
            total_fechados += 1

        lead_time_segundos = calcular_lead_time_segundos(timeline)
        if lead_time_segundos > 0:
            soma_lead_time += lead_time_segundos
            lotes_com_lead_time += 1

        lote_dict["status"] = status_abertura
        lote_dict["etapa_atual"] = etapa_atual
        lote_dict["timeline_exibicao"] = construir_timeline_exibicao(timeline, etapa_atual)
        lote_dict["lead_time_segundos"] = lead_time_segundos
        lote_dict["lead_time_humano"] = formatar_duracao_humana(lead_time_segundos)
        lote_dict["data_criacao_br"] = formatar_data_hora_br(lote_dict.get("data_criacao"))
        lote_dict["data_coleta_agendada_br"] = formatar_data_br(lote_dict.get("data_coleta_agendada"))
        lote_dict["contas_resumo"] = resumir_contas_lote(itens_lote)
        lotes.append(lote_dict)

    lead_time_medio = formatar_duracao_humana(int(soma_lead_time / lotes_com_lead_time)) if lotes_com_lead_time else "-"

    conn.close()

    return render_template(
        "metricas_full.html",
        lotes=lotes,
        lotes_itens_map=lotes_itens_map,
        total_lotes=total_lotes,
        total_mlbs=total_mlbs,
        total_pecas=total_pecas,
        contas_resumo=contas_resumo,
        total_abertos=total_abertos,
        total_fechados=total_fechados,
        lead_time_medio=lead_time_medio,
        timeline_etapas=TIMELINE_ETAPAS,
        formatar_data_hora_br=formatar_data_hora_br
    )

@app.route("/picking")
def picking_lista():
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM lotes_envio
        ORDER BY data_criacao DESC, numero_lote DESC
    """)
    lotes_rows = cursor.fetchall()

    lotes = []
    for lote in lotes_rows:
        lote_dict = dict(lote)
        etapa_atual = lote_dict.get("etapa_atual") or TIMELINE_ETAPAS[0]
        if obter_status_abertura_por_etapa(etapa_atual) != "ABERTO":
            continue

        numero_lote = lote_dict["numero_lote"]
        cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ?", (numero_lote,))
        total_itens = int(cursor.fetchone()[0] or 0)

        if total_itens == 0:
            itens_snapshot = carregar_itens_snapshot_lote(numero_lote)
            if itens_snapshot:
                df_boot = pd.DataFrame(itens_snapshot)
                sincronizar_picking_itens(numero_lote, df_boot)
                cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ?", (numero_lote,))
                total_itens = int(cursor.fetchone()[0] or 0)

        cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ? AND coletado = 1", (numero_lote,))
        itens_coletados = int(cursor.fetchone()[0] or 0)

        lote_dict["etapa_atual"] = etapa_atual
        lote_dict["status"] = obter_status_abertura_por_etapa(etapa_atual)
        lote_dict["total_itens"] = total_itens
        lote_dict["itens_coletados"] = itens_coletados
        lotes.append(lote_dict)

    conn.close()
    return render_template("picking.html", lote=None, lotes=lotes)


@app.route("/picking/<numero_lote>")
def picking_lote(numero_lote):
    numero_lote = str(numero_lote or "").strip()

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM lotes_envio WHERE numero_lote = ?", (numero_lote,))
    lote_row = cursor.fetchone()

    if not lote_row:
        conn.close()
        return redirect("/picking")

    lote = dict(lote_row)
    etapa_atual = lote.get("etapa_atual") or TIMELINE_ETAPAS[0]
    lote["etapa_atual"] = etapa_atual
    lote["status"] = obter_status_abertura_por_etapa(etapa_atual)

    cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ?", (numero_lote,))
    total_itens = int(cursor.fetchone()[0] or 0)

    if total_itens == 0:
        itens_snapshot = carregar_itens_snapshot_lote(numero_lote)
        if itens_snapshot:
            df_boot = pd.DataFrame(itens_snapshot)
            sincronizar_picking_itens(numero_lote, df_boot)

    cursor.execute("""
        SELECT numero_lote, codigo, sku, endereco, titulo, produto, conta, selo, quantidade, observacao, coletado, coletado_em, quantidade_informada, divergencia, divergencia_em
        FROM lotes_picking_itens
        WHERE numero_lote = ?
        ORDER BY coletado ASC, endereco ASC, sku ASC, codigo ASC
    """, (numero_lote,))
    itens = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return render_template("picking.html", lote=lote, itens=itens, lotes=[])


@app.route("/api/picking/coletar", methods=["POST"])
def api_picking_coletar():
    data = request.get_json() or {}
    numero_lote = str(data.get("numero_lote", "")).strip()
    sku_digitado = str(data.get("sku", "") or "").strip()
    observacao = str(data.get("observacao", "") or "").strip()

    try:
        quantidade_informada = int(float(str(data.get("quantidade", 0) or 0).replace(",", ".")))
    except:
        quantidade_informada = 0

    if not numero_lote or not sku_digitado:
        return jsonify({"ok": False, "erro": "Informe o lote e o SKU."}), 400

    if quantidade_informada <= 0:
        return jsonify({"ok": False, "erro": "Informe uma quantidade válida para a coleta."}), 400

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM lotes_picking_itens
        WHERE numero_lote = ?
          AND UPPER(TRIM(COALESCE(sku, ''))) = UPPER(TRIM(?))
        ORDER BY coletado ASC, divergencia ASC, codigo ASC
        """,
        (numero_lote, sku_digitado)
    )
    encontrados = cursor.fetchall()

    item = None
    for row in encontrados:
        if int(row["coletado"] or 0) != 1 and int(row["divergencia"] or 0) != 1:
            item = row
            break

    if not item:
        conn.close()
        return jsonify({"ok": False, "erro": "SKU não encontrado entre os itens pendentes deste lote."}), 404

    quantidade_esperada = int(item["quantidade"] or 0)
    agora = agora_str()

    if quantidade_informada > quantidade_esperada:
        conn.close()
        return jsonify({"ok": False, "erro": f"A quantidade informada ({quantidade_informada}) é maior que a esperada para este SKU ({quantidade_esperada})."}), 400

    divergencia_registrada = False
    mensagem = ""

    if quantidade_informada < quantidade_esperada:
        if not observacao:
            conn.close()
            return jsonify({
                "ok": False,
                "erro": f"Quantidade informada menor que a esperada. Esperado: {quantidade_esperada}. Informe na observação o motivo da divergência para registrar a ocorrência."
            }), 400

        observacao_final = f"DIVERGÊNCIA DE PICKING | Esperado: {quantidade_esperada} | Informado: {quantidade_informada} | Motivo: {observacao}"
        cursor.execute(
            """
            UPDATE lotes_picking_itens
            SET quantidade_informada = ?,
                divergencia = 1,
                divergencia_em = ?,
                observacao = ?
            WHERE numero_lote = ? AND codigo = ?
            """,
            (quantidade_informada, agora, observacao_final, numero_lote, item["codigo"])
        )
        divergencia_registrada = True
        mensagem = "Divergência registrada com sucesso. O item ficou salvo com observação para conferência."
    else:
        cursor.execute(
            """
            UPDATE lotes_picking_itens
            SET coletado = 1,
                coletado_em = ?,
                observacao = ?,
                quantidade_informada = ?,
                divergencia = 0,
                divergencia_em = ''
            WHERE numero_lote = ? AND codigo = ?
            """,
            (agora, observacao, quantidade_informada, numero_lote, item["codigo"])
        )
        mensagem = "SKU coletado com sucesso."

    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ?", (numero_lote,))
    total = int(cursor.fetchone()[0] or 0)

    cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ? AND coletado = 1", (numero_lote,))
    coletados = int(cursor.fetchone()[0] or 0)

    cursor.execute("SELECT COUNT(*) FROM lotes_picking_itens WHERE numero_lote = ? AND divergencia = 1", (numero_lote,))
    divergencias = int(cursor.fetchone()[0] or 0)

    cursor.execute("SELECT etapa_atual FROM lotes_envio WHERE numero_lote = ?", (numero_lote,))
    row_lote = cursor.fetchone()
    etapa_atual = (row_lote["etapa_atual"] if row_lote else TIMELINE_ETAPAS[0]) or TIMELINE_ETAPAS[0]

    conn.close()

    finalizado = total > 0 and (coletados + divergencias) >= total
    if finalizado:
        atualizar_etapa_lote(numero_lote, "EM CONFERÊNCIA")
        etapa_atual = "EM CONFERÊNCIA"

    return jsonify({
        "ok": True,
        "total": total,
        "coletados": coletados,
        "divergencias": divergencias,
        "finalizado": finalizado,
        "etapa_atual": etapa_atual,
        "divergencia": divergencia_registrada,
        "mensagem": mensagem,
        "codigo": str(item["codigo"] or "")
    })


@app.route("/dados")
def dados():
    try:
        df = carregar_dados_base()
        df = df.fillna("")
        return jsonify(df.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"ok": False, "erro": f"Erro ao carregar dados: {str(e)}", "dados": []}), 500


@app.route("/dados-dashboard")
def dados_dashboard():
    df = carregar_dados_base()
    return jsonify(df.to_dict(orient="records"))


@app.route("/exportar-excel")
def exportar_excel():
    df = carregar_dados_base()

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

    if "Código do Anúncio" not in df.columns:
        df["Código do Anúncio"] = ""

    df["Quantidade para Enviar"] = df["Código do Anúncio"].astype(str).map(mapa_quantidade).fillna(0)
    df["Estratégia"] = df["Código do Anúncio"].astype(str).map(mapa_estrategia).fillna("")
    df["Motivo do Envio"] = df["Código do Anúncio"].astype(str).map(mapa_motivo_envio).fillna("")
    df["Motivo do Envio"] = df["Motivo do Envio"].where(df["Motivo do Envio"].astype(str).str.strip() != "", df["Estratégia"])
    df["Comentário"] = df["Código do Anúncio"].astype(str).map(mapa_comentarios_mlb).fillna("")
    df["LETICIA"] = ""

    tela = request.args.get("tela", "").strip()
    conta = request.args.get("conta", "").strip()
    busca = request.args.get("busca", "").strip().lower()
    selo = request.args.get("selo", "").strip()
    logica = request.args.get("logica", "").strip()
    saude = request.args.get("saude", "").strip()
    condicao = request.args.get("condicao", "").strip()
    lote = request.args.get("lote", "").strip()
    magic = request.args.get("magic", "").strip()
    cobertura = request.args.get("cobertura", "").strip()
    full = request.args.get("full", "").strip()
    saude_full = request.args.get("saudeFull", "").strip()
    status_full_filtro = request.args.get("statusFullFiltro", "").strip()
    filtro_especial = request.args.get("filtroEspecial", "").strip()

    def valor_para_numero_excel(valor):
        if valor is None or valor == "":
            return 0
        try:
            return float(str(valor).replace(".", "").replace(",", "."))
        except:
            return 0

    def destino(codigo):
        status = mapa_status.get(str(codigo), "")
        if status == "enviando":
            return "enviando"
        if status == "homologar":
            return "homologar"
        if status in ["nao_enviar", "naoEnviar"]:
            return "naoEnviar"
        if status == "filetado":
            return "historico"
        return "principal"

    if tela:
        df = df[df["Código do Anúncio"].apply(destino) == tela]

    if conta:
        df = df[df["Nickname"].astype(str).str.strip().str.upper() == conta.upper()]

    if busca:
        mask_busca = (
            df["SKU"].astype(str).str.lower().str.contains(busca, na=False) |
            df["Código do Anúncio"].astype(str).str.lower().str.contains(busca, na=False) |
            df["Título"].astype(str).str.lower().str.contains(busca, na=False)
        )
        df = df[mask_busca]

    if selo:
        df = df[df["SELO"].astype(str).str.strip() == selo]

    if logica:
        df = df[df["ANALISE"].astype(str).str.strip() == logica]

    if saude:
        df = df[df["SAUDE DO ESTOQUE 4i"].astype(str).str.strip() == saude]

    if condicao:
        coluna_condicao = "CONDIÇÃO" if "CONDIÇÃO" in df.columns else "CONDIÇAO"
        if coluna_condicao not in df.columns:
            df[coluna_condicao] = ""
        df = df[df[coluna_condicao].astype(str).str.strip() == condicao]

    if lote:
        if "LOTE" not in df.columns:
            df["LOTE"] = ""
        df = df[df["LOTE"].astype(str).str.strip() == lote]

    if magic:
        if "MAGIC" not in df.columns:
            df["MAGIC"] = ""
        df = df[df["MAGIC"].astype(str).str.strip().str.upper() == magic.upper()]

    if full:
        if "Full" not in df.columns:
            df["Full"] = ""
        df = df[df["Full"].astype(str).str.strip().str.upper() == full.upper()]

    if saude_full:
        if "SAUDE_ESTOQUE_FULL" not in df.columns:
            df["SAUDE_ESTOQUE_FULL"] = ""
        df = df[df["SAUDE_ESTOQUE_FULL"].astype(str).str.strip().str.upper() == saude_full.upper()]

    if status_full_filtro == "nao_ofereco_mais_full":
        if "OBSERVAÇÃO MELI" not in df.columns:
            df["OBSERVAÇÃO MELI"] = ""
        df = df[df["OBSERVAÇÃO MELI"].astype(str).str.strip() == "Você deixou de oferecer o Full."]

    if status_full_filtro == "esta_no_full":
        if "Full" not in df.columns:
            df["Full"] = ""
        df = df[df["Full"].astype(str).str.strip().str.upper() == "NO FULL"]

    if status_full_filtro == "nunca_foi_full":
        if "OBSERVAÇÃO MELI" not in df.columns:
            df["OBSERVAÇÃO MELI"] = ""
        if "Full" not in df.columns:
            df["Full"] = ""
        df = df[
            (df["OBSERVAÇÃO MELI"].astype(str).str.strip() != "Você deixou de oferecer o Full.") &
            (df["Full"].astype(str).str.strip().str.upper() != "NO FULL")
        ]

    if cobertura == "baixo30":
        coluna_cobertura = df["Cobertura"].astype(str) if "Cobertura" in df.columns else pd.Series([""] * len(df), index=df.index)
        dias = coluna_cobertura.str.extract(r"(\d+)\s*dias", expand=False)
        dias = pd.to_numeric(dias, errors="coerce")
        df = df[(dias.notna()) & (dias < 30)]

    if filtro_especial:
        unidades = pd.to_numeric(df["30 DIAS"], errors="coerce").fillna(0) if "30 DIAS" in df.columns else pd.Series([0] * len(df), index=df.index)
        valores = df["Total de Vendas 30 DIAS"].apply(valor_para_numero_excel) if "Total de Vendas 30 DIAS" in df.columns else pd.Series([0] * len(df), index=df.index)

        if filtro_especial == "valor500":
            df = df[valores > 500]
        elif filtro_especial == "unidades10":
            df = df[unidades > 10]
        elif filtro_especial == "critico":
            df = df[(unidades < 10) & (valores > 500)]

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


@app.route("/exportar-excel-estrategia")
def exportar_excel_estrategia():
    estrategia = str(request.args.get("estrategia", "") or "").strip()

    if not estrategia:
        return redirect("/home")

    df = carregar_dados_base()

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT codigo, quantidade, estrategia, motivo_envio FROM status_cards")
        rows = cursor.fetchall()
        mapa_status = {
            str(codigo): {
                "quantidade": quantidade or 0,
                "estrategia": estrategia_salva or "",
                "motivo_envio": motivo_envio or ""
            }
            for codigo, quantidade, estrategia_salva, motivo_envio in rows
        }
    except:
        mapa_status = {}

    try:
        cursor.execute("SELECT codigo, comentario FROM comentarios_mlb")
        rows_comentarios_mlb = cursor.fetchall()
        mapa_comentarios_mlb = {str(codigo): comentario or "" for codigo, comentario in rows_comentarios_mlb}
    except:
        mapa_comentarios_mlb = {}

    conn.close()

    if "Código do Anúncio" not in df.columns:
        df["Código do Anúncio"] = ""

    df["Quantidade para Enviar"] = df["Código do Anúncio"].astype(str).map(
        lambda codigo: mapa_status.get(str(codigo), {}).get("quantidade", 0)
    ).fillna(0)

    df["Estratégia"] = df["Código do Anúncio"].astype(str).map(
        lambda codigo: mapa_status.get(str(codigo), {}).get("estrategia", "")
    ).fillna("")

    df["Motivo do Envio"] = df["Código do Anúncio"].astype(str).map(
        lambda codigo: mapa_status.get(str(codigo), {}).get("motivo_envio", "")
    ).fillna("")

    df["Motivo do Envio"] = df["Motivo do Envio"].where(
        df["Motivo do Envio"].astype(str).str.strip() != "",
        df["Estratégia"]
    )

    df["Comentário"] = df["Código do Anúncio"].astype(str).map(mapa_comentarios_mlb).fillna("")
    df["LETICIA"] = ""

    df = df[df["Estratégia"].astype(str).str.strip().str.casefold() == estrategia.casefold()].copy()

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

    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df[colunas_exportar].to_excel(writer, index=False, sheet_name="Relatorio")

    output.seek(0)

    nome_arquivo = f"relatorio_estrategia_{secure_filename(estrategia) or 'estrategia'}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def registrar_lote_conferencia(numero_lote, tipo_lote, df_lote):
    if df_lote.empty:
        return

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        INSERT INTO lotes_conferencia (numero_lote, tipo_lote, status, data_criacao)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(numero_lote) DO UPDATE SET
            tipo_lote=excluded.tipo_lote
    """, (numero_lote, tipo_lote, "PENDENTE", agora))

    for _, item in df_lote.iterrows():
        codigo = str(item.get("Código do Anúncio", "") or "")
        sku = str(item.get("SKU", "") or "")
        titulo = str(item.get("Título", "") or "")
        endereco = str(item.get("ENDEREÇO", "") or "")
        lote_filete = str(item.get("Lote", "") or "")
        quantidade_esperada = int(float(item.get("Enviar", 0) or 0))

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
    if df_lote.empty:
        return

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT numero_lote, timeline_json, etapa_atual, data_coleta_agendada FROM lotes_envio WHERE numero_lote = ?", (numero_lote,))
    existe = cursor.fetchone()

    total_mlbs = int(len(df_lote))
    total_pecas = int(pd.to_numeric(df_lote["Enviar"], errors="coerce").fillna(0).sum())
    agora = agora_str()
    etapa_inicial = "GERAR LOTE MELI"

    if existe:
        timeline = carregar_timeline_json(existe["timeline_json"])
        etapa_atual = existe["etapa_atual"] or etapa_inicial
        timeline = sincronizar_timeline_ate_etapa(timeline, etapa_atual, agora)

        cursor.execute(
            """
            UPDATE lotes_envio
            SET tipo_lote = ?,
                total_mlbs = ?,
                total_pecas = ?,
                origem = 'FILETE',
                etapa_atual = ?,
                timeline_json = ?
            WHERE numero_lote = ?
            """,
            (tipo_lote, total_mlbs, total_pecas, etapa_atual, timeline_para_json(timeline), numero_lote)
        )
        atualizar_statuss_por_etapa(cursor, numero_lote, etapa_atual)
    else:
        timeline = timeline_vazio()
        timeline = sincronizar_timeline_ate_etapa(timeline, etapa_inicial, agora)

        cursor.execute(
            """
            INSERT INTO lotes_envio (
                numero_lote, tipo_lote, total_mlbs, total_pecas,
                status, responsavel, transportadora, observacao,
                prioridade, data_envio, status_expedicao,
                status_ecommerce, origem, data_criacao,
                timeline_json, etapa_atual, data_coleta_agendada
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                numero_lote,
                tipo_lote,
                total_mlbs,
                total_pecas,
                obter_status_abertura_por_etapa(etapa_inicial),
                "",
                "",
                "Gerado via filete",
                "",
                "",
                "AGUARDANDO",
                etapa_inicial,
                "FILETE",
                agora,
                timeline_para_json(timeline),
                etapa_inicial,
                ""
            )
        )

    conn.commit()
    conn.close()


def salvar_historico_e_finalizar_envio(numero_lote, tipo_lote, df_lote):
    if df_lote.empty:
        return

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
        SELECT codigo, estrategia, motivo_envio
        FROM status_cards
    """)
    rows_status = cursor.fetchall()
    mapa_status = {
        str(row["codigo"]): {
            "estrategia": row["estrategia"] or "",
            "motivo_envio": row["motivo_envio"] or ""
        }
        for row in rows_status
    }

    cursor.execute("""
        SELECT codigo, comentario
        FROM comentarios_mlb
    """)
    rows_comentarios = cursor.fetchall()
    mapa_comentarios_mlb = {
        str(row["codigo"]): row["comentario"] or ""
        for row in rows_comentarios
    }

    cursor.execute("""
        SELECT sku, comentario
        FROM comentarios
    """)
    rows_comentarios_sku = cursor.fetchall()
    mapa_comentarios_sku = {
        str(row["sku"]): row["comentario"] or ""
        for row in rows_comentarios_sku
    }

    cursor.execute("""
        SELECT codigo, mensagem, data_hora
        FROM comentarios_mlb_chat
        ORDER BY id ASC
    """)
    rows_chat = cursor.fetchall()
    mapa_chat_codigo = {}
    for row in rows_chat:
        mapa_chat_codigo.setdefault(str(row["codigo"]), []).append({
            "mensagem": row["mensagem"] or "",
            "data_hora": row["data_hora"] or ""
        })

    cursor.execute("""
        DELETE FROM lote_comentarios_chat
        WHERE numero_lote = ?
    """, (numero_lote,))

    cursor.execute("""
        DELETE FROM lotes_envio_itens_snapshot
        WHERE numero_lote = ?
    """, (numero_lote,))

    def valor_json(valor):
        try:
            if pd.isna(valor):
                return ""
        except:
            pass

        if isinstance(valor, (int, float, str, bool)) or valor is None:
            return valor

        return str(valor)

    for _, item in df_lote.iterrows():
        codigo = str(item.get("Código do Anúncio", "") or "")
        sku = str(item.get("SKU", "") or "")
        titulo = str(item.get("Título", "") or "")
        nickname = str(item.get("Nickname", "") or "")
        endereco = str(item.get("ENDEREÇO", "") or "")
        lote_filete = str(item.get("Lote", "") or "")
        quantidade = int(float(item.get("Enviar", 0) or 0))

        estrategia = mapa_status.get(codigo, {}).get("estrategia", "")
        motivo_envio = mapa_status.get(codigo, {}).get("motivo_envio", "") or estrategia
        comentario_mlb = mapa_comentarios_mlb.get(codigo, "")
        sku_info = mapa_comentarios_sku.get(sku, "")

        for msg in mapa_chat_codigo.get(codigo, []):
            cursor.execute("""
                INSERT INTO lote_comentarios_chat (numero_lote, codigo, mensagem, data_hora)
                VALUES (?, ?, ?, ?)
            """, (numero_lote, codigo, msg.get("mensagem", ""), msg.get("data_hora", "")))

        dados_item = {coluna: valor_json(valor) for coluna, valor in item.to_dict().items()}

        cursor.execute("""
            INSERT INTO lotes_envio_itens_snapshot (
                numero_lote, tipo_lote, codigo, sku, titulo,
                nickname, quantidade, endereco, lote_filete,
                estrategia, motivo_envio, comentario_mlb, sku_info,
                dados_json, data_geracao
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            estrategia,
            motivo_envio,
            comentario_mlb,
            sku_info,
            json.dumps(dados_item, ensure_ascii=False),
            agora
        ))

        cursor.execute("""
            UPDATE status_cards
            SET status = ?, quantidade = ?, estrategia = ?, motivo_envio = ?
            WHERE codigo = ?
        """, ("principal", 0, "", "", codigo))

        cursor.execute("DELETE FROM comentarios_mlb WHERE codigo = ?", (codigo,))
        cursor.execute("DELETE FROM comentarios_mlb_chat WHERE codigo = ?", (codigo,))

    conn.commit()
    conn.close()
    sincronizar_picking_itens(numero_lote, df_lote)

@app.route("/criar-lote-enviando", methods=["POST"])
def criar_lote_enviando():
    data = request.get_json() or {}

    tipo_lote_padrao = str(data.get("tipo_lote", "Diversos")).strip() or "Diversos"
    lotes_config = data.get("lotes", []) or []

    df = carregar_dados_base()

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

    if df.empty:
        return jsonify({"ok": False, "erro": "Não há itens na tela Enviando para criar o lote."}), 400

    colunas_necessarias = [
        "Nickname",
        "Código do Anúncio",
        "SKU",
        "Título",
        "ENDEREÇO",
        "SELO"
    ]

    for col in colunas_necessarias:
        if col not in df.columns:
            df[col] = ""

    def classificar_tipo_item(row):
        selo = str(row.get("SELO", "") or "").strip().upper()
        if "CAIXA" in selo:
            return "Caixa"
        if "DIVERS" in selo:
            return "Diversos"
        return tipo_lote_padrao

    df["CONTA_LOTE"] = df["Nickname"].astype(str).str.strip()
    df["CONTA_LOTE"] = df["CONTA_LOTE"].replace("", "SEM CONTA")
    df["TIPO_LOTE_GRUPO"] = df.apply(classificar_tipo_item, axis=1)

    if not isinstance(lotes_config, list) or not lotes_config:
        return jsonify({"ok": False, "erro": "Informe os números dos lotes para cada conta e tipo."}), 400

    mapa_lotes = {}
    numeros_utilizados = set()

    for item in lotes_config:
        conta = str((item or {}).get("conta", "") or "").strip() or "SEM CONTA"
        tipo = str((item or {}).get("tipo", "") or "").strip() or tipo_lote_padrao
        numero_lote = str((item or {}).get("numero_lote", "") or "").strip()

        if not numero_lote:
            return jsonify({"ok": False, "erro": f"Informe o número do lote para {conta} / {tipo}."}), 400

        numero_normalizado = numero_lote.upper()
        if numero_normalizado in numeros_utilizados:
            return jsonify({"ok": False, "erro": f"O número de lote {numero_lote} foi informado mais de uma vez."}), 400

        numeros_utilizados.add(numero_normalizado)
        mapa_lotes[(conta.upper(), tipo.upper())] = numero_lote

    grupos = []
    for (conta, tipo), df_grupo in df.groupby(["CONTA_LOTE", "TIPO_LOTE_GRUPO"], sort=True):
        chave = (str(conta).upper(), str(tipo).upper())
        numero_lote = mapa_lotes.get(chave)

        if not numero_lote:
            return jsonify({"ok": False, "erro": f"Faltou informar o número do lote para {conta} / {tipo}."}), 400

        df_lote = df_grupo.copy()
        df_lote["Lote"] = f"Lote {tipo} - #{numero_lote}"

        if "ENDEREÇO" in df_lote.columns:
            df_lote = df_lote.sort_values(by="ENDEREÇO", kind="stable")

        try:
            registrar_lote_conferencia(numero_lote, tipo, df_lote)
            atualizar_lote_envio_existente(numero_lote, tipo, df_lote)
            salvar_historico_e_finalizar_envio(numero_lote, tipo, df_lote)
        except ValueError as e:
            return jsonify({"ok": False, "erro": str(e)}), 400

        grupos.append({
            "numero_lote": numero_lote,
            "conta": conta,
            "tipo": tipo,
            "itens": int(len(df_lote))
        })

    return jsonify({"ok": True, "lotes_criados": grupos})

def montar_excel_filete_antigo(df, numero_lote, tipo_lote):
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

        center = Alignment(horizontal="center", vertical="center", wrap_text=True)
        left = Alignment(horizontal="left", vertical="center", wrap_text=True)

        colunas = ["Nickname", "Código do Anúncio", "SKU", "Enviar", "ENDEREÇO", "Título"]

        for col in colunas:
            if col not in df.columns:
                df[col] = ""

        df_export = df[colunas].copy()
        df_export.rename(columns={
            "Nickname": "CONTA",
            "Código do Anúncio": "CÓDIGO",
            "Enviar": "ENVIAR",
            "Título": "TÍTULO"
        }, inplace=True)

        conta_principal = ""
        contas = [str(c).strip() for c in df_export["CONTA"].tolist() if str(c).strip()]
        if contas:
            conta_principal = contas[0]

        worksheet["A1"] = f"CONTA: {conta_principal}" if conta_principal else "CONTA:"
        worksheet["A1"].font = Font(bold=True, size=12)
        worksheet["A1"].fill = fill_topo
        worksheet["A1"].alignment = left
        worksheet["A1"].border = border
        worksheet.merge_cells("A1:D1")

        worksheet["E1"] = f"Lote {tipo_lote} - #{numero_lote}"
        worksheet["E1"].font = Font(bold=True, size=12)
        worksheet["E1"].fill = fill_topo
        worksheet["E1"].alignment = center
        worksheet["E1"].border = border

        cabecalhos = ["CÓDIGO", "SKU", "ENVIAR", "ENDEREÇO", "TÍTULO"]
        for idx, coluna in enumerate(cabecalhos, start=1):
            cell = worksheet.cell(row=2, column=idx, value=coluna)
            cell.font = Font(bold=True)
            cell.fill = fill_label
            cell.alignment = center
            cell.border = border

        linha = 3
        for _, row in df_export.iterrows():
            valores = [
                row["CÓDIGO"],
                row["SKU"],
                int(float(row["ENVIAR"] or 0)),
                row["ENDEREÇO"],
                row["TÍTULO"]
            ]
            for col_idx, valor in enumerate(valores, start=1):
                cell = worksheet.cell(row=linha, column=col_idx, value=valor)
                cell.fill = fill_valor
                cell.alignment = center if col_idx in [1, 2, 3] else left
                cell.border = border
                if col_idx in [1, 2, 3]:
                    cell.font = Font(bold=True, size=12)
            linha += 1

        worksheet.row_dimensions[1].height = 24
        worksheet.row_dimensions[2].height = 22

        larguras = {"A": 24, "B": 17, "C": 12, "D": 24, "E": 68}
        for col, largura in larguras.items():
            worksheet.column_dimensions[col].width = largura

    output.seek(0)
    return output


@app.route("/gerar-filete")
def gerar_filete():
    numero_lote = request.args.get("numero_lote", "").strip()
    tipo_lote = request.args.get("tipo_lote", "Diversos").strip() or "Diversos"

    if not numero_lote:
        return "Informe um número de lote válido.", 400

    df = carregar_dados_base()

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
        df = df.sort_values(by=["Nickname", "ENDEREÇO", "Código do Anúncio"], kind="stable")

    try:
        registrar_lote_conferencia(numero_lote, tipo_lote, df)
        atualizar_lote_envio_existente(numero_lote, tipo_lote, df)
        salvar_historico_e_finalizar_envio(numero_lote, tipo_lote, df)
    except ValueError as e:
        return str(e), 400

    output = montar_excel_filete_antigo(df, numero_lote, tipo_lote)

    nome_arquivo = "filete.xlsx"
    if numero_lote:
        nome_arquivo = f"filete_{numero_lote}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=nome_arquivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def carregar_itens_snapshot_lote(numero_lote):
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM lotes_envio_itens_snapshot
        WHERE numero_lote = ?
        ORDER BY endereco, sku, codigo
    """, (numero_lote,))
    itens = [dict(row) for row in cursor.fetchall()]

    if not itens:
        cursor.execute("""
            SELECT
                li.numero_lote,
                COALESCE(le.tipo_lote, lc.tipo_lote, '') AS tipo_lote,
                li.codigo,
                li.sku,
                li.titulo,
                '' AS nickname,
                li.quantidade_esperada AS quantidade,
                li.endereco,
                li.lote_filete,
                '' AS estrategia,
                '' AS motivo_envio,
                '' AS comentario_mlb,
                '' AS dados_json,
                COALESCE(le.data_criacao, lc.data_criacao, '') AS data_geracao
            FROM lotes_itens li
            LEFT JOIN lotes_envio le
                ON le.numero_lote = li.numero_lote
            LEFT JOIN lotes_conferencia lc
                ON lc.numero_lote = li.numero_lote
            WHERE li.numero_lote = ?
            ORDER BY li.endereco, li.sku, li.codigo
        """, (numero_lote,))
        itens = [dict(row) for row in cursor.fetchall()]

    conn.close()

    for item in itens:
        item["dados_json"] = item.get("dados_json") or "{}"

    itens = enriquecer_itens_lote_com_base(itens)
    return itens

def montar_excel_filete_lote(df, numero_lote, tipo_lote):
    return montar_excel_filete_antigo(df, numero_lote, tipo_lote)


@app.route("/lote-envio/<numero_lote>/pdf")
def lote_envio_pdf(numero_lote):
    itens = carregar_itens_snapshot_lote(numero_lote)

    if not itens:
        return "Nenhum item encontrado para este lote.", 404

    dados_pdf = []
    for item in itens:
        try:
            dados_item = json.loads(item["dados_json"] or "{}")
        except:
            dados_item = {}

        dados_pdf.append({
            "mlb": item["codigo"],
            "titulo": item["titulo"] or "",
            "sku": item["sku"] or "",
            "quantidade": item["quantidade"] or 0,
            "vendas7": dados_item.get("7 DIAS", 0) or 0,
            "vendas15": dados_item.get("15 DIAS", 0) or 0,
            "vendas30": dados_item.get("30 DIAS", 0) or 0,
            "comentario": item["comentario_mlb"] or ""
        })

    pdf = gerar_pdf_filete(dados_pdf)

    return send_file(
        pdf,
        as_attachment=True,
        download_name=f"lote_{numero_lote}.pdf",
        mimetype="application/pdf"
    )

@app.route("/lote-envio/<numero_lote>/filete-excel")
def lote_envio_filete_excel(numero_lote):
    itens = carregar_itens_snapshot_lote(numero_lote)

    if not itens:
        return "Nenhum item encontrado para este lote.", 404

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT numero_lote, tipo_lote
        FROM lotes_envio
        WHERE numero_lote = ?
    """, (numero_lote,))
    lote = cursor.fetchone()
    conn.close()

    tipo_lote = lote["tipo_lote"] if lote else "Diversos"

    df = pd.DataFrame([{
        "Nickname": item["nickname"],
        "Código do Anúncio": item["codigo"],
        "SKU": item["sku"],
        "Título": item["titulo"],
        "ENDEREÇO": item["endereco"],
        "Enviar": item["quantidade"],
        "Lote": item["lote_filete"]
    } for item in itens])

    output = montar_excel_filete_lote(df, numero_lote, tipo_lote)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"filete_{numero_lote}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route("/lote-envio/<numero_lote>/exportar-excel")
def lote_envio_exportar_excel(numero_lote):
    itens = carregar_itens_snapshot_lote(numero_lote)

    if not itens:
        return "Nenhum item encontrado para este lote.", 404

    registros = []
    for item in itens:
        dados_item = json.loads(item["dados_json"] or "{}")
        dados_item["Quantidade para Enviar"] = item["quantidade"] or 0
        dados_item["Estratégia"] = item["estrategia"] or ""
        dados_item["Motivo do Envio"] = item["motivo_envio"] or ""
        dados_item["Comentário"] = item["comentario_mlb"] or ""
        dados_item["LETICIA"] = ""
        registros.append(dados_item)

    df = pd.DataFrame(registros)

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
        download_name=f"relatorio_lote_{numero_lote}.xlsx",
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
    if (not str(motivo_envio).strip()) and str(estrategia).strip():
        motivo_envio = estrategia

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
    try:
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
    except Exception:
        return jsonify({})


@app.route("/comentarios")
def get_comentarios():
    try:
        conn = sqlite3.connect("status.db")
        cursor = conn.cursor()

        cursor.execute("SELECT sku, comentario FROM comentarios")
        rows = cursor.fetchall()

        conn.close()

        comentarios = {str(sku): comentario for sku, comentario in rows}
        return jsonify(comentarios)
    except Exception:
        return jsonify({})


@app.route("/comentarios-mlb")
def get_comentarios_mlb():
    try:
        conn = sqlite3.connect("status.db")
        cursor = conn.cursor()

        cursor.execute("SELECT codigo, comentario FROM comentarios_mlb")
        rows = cursor.fetchall()

        conn.close()

        comentarios = {str(codigo): comentario for codigo, comentario in rows}
        return jsonify(comentarios)
    except Exception:
        return jsonify({})


@app.route("/comentarios-mlb-chat")
def get_comentarios_mlb_chat():
    codigo = str(request.args.get("codigo", "") or "").strip()

    try:
        conn = sqlite3.connect("status.db")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, codigo, mensagem, data_hora
            FROM comentarios_mlb_chat
            WHERE codigo = ?
            ORDER BY id ASC
        """, (codigo,))
        rows = cursor.fetchall()
        conn.close()

        return jsonify([{
            "id": row["id"],
            "codigo": row["codigo"],
            "mensagem": row["mensagem"] or "",
            "data_hora": row["data_hora"] or "",
            "data_hora_br": formatar_data_hora_br(row["data_hora"], vazio="")
        } for row in rows])
    except Exception:
        return jsonify([])


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
    data = request.json or {}
    codigo = str(data.get("codigo", "") or "").strip()
    comentario = str(data.get("comentario", "") or "").strip()
    modo = str(data.get("modo", "resumo") or "resumo").strip()

    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    if modo == "chat":
        data_hora = agora_iso()

        if comentario:
            cursor.execute("""
                INSERT INTO comentarios_mlb_chat (codigo, mensagem, data_hora)
                VALUES (?, ?, ?)
            """, (codigo, comentario, data_hora))

            cursor.execute("""
                INSERT INTO comentarios_mlb (codigo, comentario)
                VALUES (?, ?)
                ON CONFLICT(codigo) DO UPDATE SET comentario=excluded.comentario
            """, (codigo, comentario))

        conn.commit()
        conn.close()

        return jsonify({"success": True, "data_hora": data_hora, "data_hora_br": formatar_data_hora_br(data_hora)})

    cursor.execute("""
        INSERT INTO comentarios_mlb (codigo, comentario)
        VALUES (?, ?)
        ON CONFLICT(codigo) DO UPDATE SET comentario=excluded.comentario
    """, (codigo, comentario))

    conn.commit()
    conn.close()

    return jsonify({"success": True})


@app.route("/salvar-lote-envio", methods=["POST"])
def salvar_lote_envio():
    data = request.json if request.is_json else request.form

    numero_lote = str(data.get("numero_lote", "") or "").strip()
    tipo_lote = str(data.get("tipo_lote", "") or "").strip() or "Diversos"
    total_mlbs = int(float(data.get("total_mlbs", 0) or 0))
    total_pecas = int(float(data.get("total_pecas", 0) or 0))
    observacao = str(data.get("observacao", "") or "").strip()
    origem = str(data.get("origem", "MANUAL") or "MANUAL").strip() or "MANUAL"
    etapa_atual = str(data.get("etapa_atual", "MLBS EM ANÁLISE") or "MLBS EM ANÁLISE").strip()
    data_coleta_agendada = str(data.get("data_coleta_agendada", "") or "").strip()

    if not numero_lote:
        return jsonify({"ok": False, "erro": "Número do lote é obrigatório."}), 400

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM lotes_envio WHERE numero_lote = ?", (numero_lote,))
    existe = cursor.fetchone()

    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if existe:
        timeline = carregar_timeline_json(existe["timeline_json"])
        etapa_salva = (existe["etapa_atual"] or TIMELINE_ETAPAS[0]).strip() or TIMELINE_ETAPAS[0]
        if not any(str(v or "").strip() for v in timeline.values()):
            timeline[etapa_salva] = existe["data_criacao"] or agora
        timeline = sincronizar_timeline_ate_etapa(timeline, etapa_atual, agora)

        cursor.execute("""
            UPDATE lotes_envio
            SET tipo_lote = ?,
                total_mlbs = ?,
                total_pecas = ?,
                observacao = ?,
                origem = ?,
                etapa_atual = ?,
                timeline_json = ?,
                data_coleta_agendada = ?
            WHERE numero_lote = ?
        """, (
            tipo_lote,
            total_mlbs,
            total_pecas,
            observacao,
            origem,
            etapa_atual,
            timeline_para_json(timeline),
            data_coleta_agendada,
            numero_lote
        ))
    else:
        timeline = timeline_vazio()
        timeline[etapa_atual] = agora

        cursor.execute("""
            INSERT INTO lotes_envio (
                numero_lote, tipo_lote, total_mlbs, total_pecas,
                status, responsavel, transportadora, observacao,
                prioridade, data_envio, status_expedicao, status_ecommerce,
                origem, data_criacao, timeline_json, etapa_atual, data_coleta_agendada
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            numero_lote,
            tipo_lote,
            total_mlbs,
            total_pecas,
            "CRIADO",
            "",
            "",
            observacao,
            "",
            "",
            "AGUARDANDO",
            "AGUARDANDO",
            origem,
            agora,
            timeline_para_json(timeline),
            etapa_atual,
            data_coleta_agendada
        ))

    atualizar_statuss_por_etapa(cursor, numero_lote, etapa_atual)

    conn.commit()
    conn.close()

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json:
        return jsonify({"ok": True})

    return redirect("/metricas-full")


@app.route("/debug-comentarios")
def debug_comentarios():
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM comentarios")
    dados = cursor.fetchall()
    conn.close()
    return jsonify(dados)


@app.route("/dashboard")
def dashboard():
    return render_template(
        "dashboard.html"
    )


from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT

def gerar_pdf_filete(dados):
    buffer = BytesIO()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=20,
        rightMargin=20,
        topMargin=20,
        bottomMargin=20
    )

    styles = getSampleStyleSheet()

    estilo_titulo = ParagraphStyle(
        name="TituloCustom",
        parent=styles["Title"],
        fontSize=18,
        leading=22,
        alignment=1
    )

    estilo_label = ParagraphStyle(
        name="LabelCustom",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=11
    )

    estilo_valor = ParagraphStyle(
        name="ValorCustom",
        parent=styles["Normal"],
        fontSize=9,
        leading=11
    )

    estilo_comentario = ParagraphStyle(
        name="ComentarioCustom",
        parent=styles["Normal"],
        fontSize=9,
        leading=12
    )

    elementos = []

    data_pdf = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    elementos.append(Paragraph("FILETE DE ENVIO", estilo_titulo))
    elementos.append(Spacer(1, 8))
    elementos.append(Paragraph(f"<b>Gerado em:</b> {data_pdf}", estilo_valor))
    elementos.append(Spacer(1, 12))

    for idx, item in enumerate(dados, start=1):
        mlb = str(item.get("mlb", "") or "")
        sku = str(item.get("sku", "") or "")
        qtd = str(item.get("quantidade", "") or "")
        titulo = str(item.get("titulo", "") or "")
        vendas7 = str(item.get("vendas7", "") or "")
        vendas15 = str(item.get("vendas15", "") or "")
        vendas30 = str(item.get("vendas30", "") or "")
        comentario = str(item.get("comentario", "") or "-")

        linha_item = [
    [Paragraph(f"ITEM {idx} - {titulo}", estilo_label)]
]

        tabela_item = Table(linha_item, colWidths=[555])
        tabela_item.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.7, colors.black),
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#d9d9d9")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ]))

        tabela_info = Table([
            [
                Paragraph(f"<b>MLB:</b> {mlb}", estilo_valor),
                Paragraph(f"<b>SKU:</b> {sku}", estilo_valor),
                Paragraph(f"<b>QTD ENVIADA:</b> {qtd}", estilo_valor),
            ],
            [
                Paragraph(f"<b>7 DIAS:</b> {vendas7}", estilo_valor),
                Paragraph(f"<b>15 DIAS:</b> {vendas15}", estilo_valor),
                Paragraph(f"<b>30 DIAS:</b> {vendas30}", estilo_valor),
            ],
            [
                Paragraph(f"<b>DATA PDF:</b> {data_pdf}", estilo_valor),
                "",
                "",
            ]
        ], colWidths=[185, 185, 185])

        tabela_info.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.7, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))

        tabela_comentario_titulo = Table([
            [Paragraph("COMENTÁRIO", estilo_label)]
        ], colWidths=[555])

        tabela_comentario_titulo.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.7, colors.black),
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eeeeee")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))

        tabela_comentario = Table([
            [Paragraph(comentario, estilo_comentario)]
        ], colWidths=[555])

        tabela_comentario.setStyle(TableStyle([
            ("GRID", (0, 0), (-1, -1), 0.7, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ]))

        elementos.append(tabela_item)
        elementos.append(tabela_info)
        elementos.append(tabela_comentario_titulo)
        elementos.append(tabela_comentario)
        elementos.append(Spacer(1, 12))

    doc.build(elementos)
    buffer.seek(0)
    return buffer

@app.route("/gerar-pdf-filete", methods=["POST"])
def gerar_pdf_filete_route():
    dados = request.json

    pdf = gerar_pdf_filete(dados)

    return send_file(
        pdf,
        as_attachment=True,
        download_name="filete_envio.pdf",
        mimetype="application/pdf"
    )

@app.route("/api/full-distribuicao")
def api_full_distribuicao():
    url = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=46764324"

    try:
        df = pd.read_csv(url)
        df = df.fillna("")
        df.columns = [str(c).strip() for c in df.columns]

        colunas_desejadas = [
            'Unidades que afetam a métrica "Com tempo de estoque"',
            'Entrada pendente',
            'Em transferência',
            'Devolvidas pelo comprador',
            'Não aptas para venda',
            'Temporariamente não aptas para venda\nEnquanto voltam a estar à venda, não ocuparão espaço no Full.',
            'Para colocar à venda',
            'Para evitar descarte'
        ]

        def limpar_numero(valor):
            if pd.isna(valor):
                return 0

            if isinstance(valor, (int, float)):
                return float(valor)

            texto = str(valor).strip()

            if texto == "":
                return 0

            texto = texto.replace(" ", "")

            if "," in texto and "." in texto:
                texto = texto.replace(".", "").replace(",", ".")
            elif "," in texto:
                texto = texto.replace(",", ".")

            try:
                return float(texto)
            except:
                return 0

        dados = []

        for coluna in colunas_desejadas:
            if coluna in df.columns:
                valor_total = df[coluna].apply(limpar_numero).sum()

                if float(valor_total).is_integer():
                    valor_total = int(valor_total)

                dados.append({
                    "titulo": coluna.replace("\n", " "),
                    "coluna": coluna,
                    "valor": valor_total
                })

        return jsonify({
            "ok": True,
            "dados": dados
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "erro": f"Erro ao carregar distribuição do Full: {str(e)}"
        }), 500

@app.route("/excluir-lote/<numero_lote>", methods=["GET", "POST"])
def excluir_lote(numero_lote):
    conn = sqlite3.connect("status.db")
    cursor = conn.cursor()

    cursor.execute("DELETE FROM lotes_envio WHERE numero_lote = ?", (numero_lote,))
    cursor.execute("DELETE FROM lotes_conferencia WHERE numero_lote = ?", (numero_lote,))
    cursor.execute("DELETE FROM lotes_itens WHERE numero_lote = ?", (numero_lote,))
    cursor.execute("DELETE FROM conferencia_itens WHERE numero_lote = ?", (numero_lote,))
    cursor.execute("DELETE FROM lotes_envio_itens_snapshot WHERE numero_lote = ?", (numero_lote,))

    try:
        cursor.execute("DELETE FROM lotes_picking_itens WHERE numero_lote = ?", (numero_lote,))
    except:
        pass

    conn.commit()
    conn.close()

    return redirect("/metricas-full")

@app.route("/api/full-distribuicao-detalhe")
def api_full_distribuicao_detalhe():
    coluna = str(request.args.get("coluna", "")).strip()

    if not coluna:
        return jsonify({"ok": False, "erro": "Coluna não informada."}), 400

    url_full = "https://docs.google.com/spreadsheets/d/1DKdRHI9IEacgOwsEd-bnAN4nU3dA_clULxU1mFa8LmY/export?format=csv&gid=46764324"

    try:
        df = pd.read_csv(url_full).fillna("")
        df.columns = [str(c).strip() for c in df.columns]

        if coluna not in df.columns:
            return jsonify({"ok": False, "erro": f"Coluna '{coluna}' não encontrada."}), 400

        def encontrar_coluna(df, candidatos):
            colunas_reais = list(df.columns)
            colunas_norm = {str(c).strip().lower(): c for c in colunas_reais}

            for candidato in candidatos:
                chave = str(candidato).strip().lower()
                if chave in colunas_norm:
                    return colunas_norm[chave]

            for col_real in colunas_reais:
                col_real_norm = str(col_real).strip().lower()
                for candidato in candidatos:
                    cand_norm = str(candidato).strip().lower()
                    if cand_norm in col_real_norm:
                        return col_real

            return None

        def limpar_numero(valor):
            if pd.isna(valor):
                return 0

            if isinstance(valor, (int, float)):
                return float(valor)

            texto = str(valor).strip()

            if texto == "":
                return 0

            texto = texto.replace(" ", "")

            if "," in texto and "." in texto:
                texto = texto.replace(".", "").replace(",", ".")
            elif "," in texto:
                texto = texto.replace(",", ".")

            try:
                return float(texto)
            except:
                return 0

        def normalizar_mlb(valor):
            texto = str(valor or "").strip()
            numeros = "".join(ch for ch in texto if ch.isdigit())

            if not numeros:
                return texto

            if texto.upper().startswith("MLB"):
                return f"MLB{numeros}"

            return numeros

        coluna_anuncio = encontrar_coluna(df, [
            "# Anúncio /",
            "# Anúncio",
            "#anúncio",
            "# anuncio",
            "# anúncio /",
            "# anúncio",
            "#anuncio",
            "anúncio",
            "anuncio"
        ])

        coluna_sku = encontrar_coluna(df, [
            "SKU",
            "sku",
            "Sku"
        ])

        coluna_conta = encontrar_coluna(df, [
            "CONTA",
            "Conta",
            "conta"
        ])

        if not coluna_anuncio:
            return jsonify({
                "ok": False,
                "erro": "Coluna '# Anúncio /' não encontrada na aba do Full."
            }), 400

        df["valor_coluna"] = df[coluna].apply(limpar_numero)
        df_filtrado = df[df["valor_coluna"] > 0].copy()

        dados = []
        for _, row in df_filtrado.iterrows():
            unidades = row["valor_coluna"]
            if float(unidades).is_integer():
                unidades = int(unidades)

            mlb = normalizar_mlb(row.get(coluna_anuncio, "")) if coluna_anuncio else ""
            sku = str(row.get(coluna_sku, "")).strip() if coluna_sku else ""
            conta = str(row.get(coluna_conta, "")).strip() if coluna_conta else ""

            dados.append({
                "mlb": mlb,
                "sku": sku,
                "conta": conta,
                "status": coluna.replace("\n", " "),
                "unidades": unidades
            })

        dados = sorted(
            dados,
            key=lambda x: float(x["unidades"]) if x["unidades"] != "" else 0,
            reverse=True
        )

        return jsonify({
            "ok": True,
            "dados": dados
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "erro": f"Erro ao carregar detalhes da métrica Full: {str(e)}"
        }), 500
    
import unicodedata
import re

UPLOAD_FOLDER_RELATORIOS = os.path.join("static", "uploads_relatorios")
os.makedirs(UPLOAD_FOLDER_RELATORIOS, exist_ok=True)


def init_uploads_db():
    conn = sqlite3.connect("status.db")
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS uploads_arquivos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_arquivo TEXT,
            conta TEXT,
            sistema TEXT,
            tipo_relatorio TEXT,
            caminho TEXT,
            data_upload TEXT,
            status TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def normalizar_mlb_teste(valor):
    texto = str(valor or "").strip().upper()
    numeros = "".join(ch for ch in texto if ch.isdigit())
    if not numeros:
        return texto
    return f"MLB{numeros}"


def normalizar_texto_coluna(valor):
    texto = str(valor or "").strip().lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    texto = re.sub(r"\s+", " ", texto)
    return texto


def encontrar_coluna(df, candidatos, obrigatoria=False):
    if df is None or df.empty:
        return None

    mapa = {normalizar_texto_coluna(col): col for col in df.columns}
    candidatos_norm = [normalizar_texto_coluna(c) for c in candidatos if str(c).strip()]

    for cand in candidatos_norm:
        if cand in mapa:
            return mapa[cand]

    for col in df.columns:
        col_norm = normalizar_texto_coluna(col)
        for cand in candidatos_norm:
            if cand and cand in col_norm:
                return col
            if cand and col_norm in cand:
                return col

    if obrigatoria:
        raise KeyError(f"Coluna não encontrada. Esperado um destes nomes: {', '.join(candidatos)}")
    return None


def garantir_colunas_unicas(colunas):
    contador = {}
    resultado = []

    for idx, col in enumerate(list(colunas)):
        nome = str(col or "").strip()
        if not nome or nome.lower().startswith("unnamed:"):
            nome = f"coluna_{idx}"

        base = nome
        if base in contador:
            contador[base] += 1
            nome = f"{base}__{contador[base]}"
        else:
            contador[base] = 0

        resultado.append(nome)

    return resultado


def ler_planilha_com_cabecalho_inteligente(caminho, candidatos_coluna_chave=None):
    caminho = str(caminho or "").strip()
    if not caminho or not os.path.exists(caminho):
        return pd.DataFrame()

    ext = os.path.splitext(caminho)[1].lower()

    def _ler_sem_header(header=None):
        if ext in [".csv", ".txt"]:
            return pd.read_csv(caminho, header=header, dtype=str)
        return pd.read_excel(caminho, header=header, dtype=str)

    bruto = _ler_sem_header(header=None).fillna("")
    candidatos_norm = [normalizar_texto_coluna(c) for c in (candidatos_coluna_chave or []) if str(c).strip()]

    melhor_idx = None
    melhor_score = -1
    limite = min(len(bruto), 30)

    for idx in range(limite):
        linha = [str(v or "").strip() for v in bruto.iloc[idx].tolist()]
        valores_norm = [normalizar_texto_coluna(v) for v in linha]
        score = sum(1 for c in candidatos_norm if c and c in valores_norm)
        nao_vazios = sum(1 for v in linha if str(v).strip())

        if score > melhor_score and nao_vazios > 0:
            melhor_score = score
            melhor_idx = idx

    if melhor_idx is None:
        df = _ler_sem_header(header=0).fillna("")
        df.columns = garantir_colunas_unicas(df.columns)
        return df

    header_bruto = [str(v or "").strip() for v in bruto.iloc[melhor_idx].tolist()]
    dados = bruto.iloc[melhor_idx + 1 :].copy().reset_index(drop=True).fillna("")
    dados.columns = garantir_colunas_unicas(header_bruto)

    # remove linhas que repetem o cabeçalho do arquivo TESTE ou vêm vazias
    if len(dados):
        linha_header_norm = [normalizar_texto_coluna(v) for v in header_bruto]
        mascara_validas = []
        for _, row in dados.iterrows():
            valores = [str(v or "").strip() for v in row.tolist()]
            valores_norm = [normalizar_texto_coluna(v) for v in valores]
            eh_vazia = not any(valores)
            eh_repeticao_header = valores_norm == linha_header_norm
            mascara_validas.append(not eh_vazia and not eh_repeticao_header)
        dados = dados[pd.Series(mascara_validas, index=dados.index)].reset_index(drop=True)

    return dados


def to_num(valor):
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    texto = str(valor).strip()
    if not texto:
        return 0.0
    texto = texto.replace("R$", "").replace(" ", "")
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")
    try:
        return float(texto)
    except Exception:
        return 0.0


def inteiro_ou_zero(valor):
    numero = to_num(valor)
    return int(numero) if float(numero).is_integer() else round(numero, 2)


def formatar_moeda_br(valor):
    valor = float(valor or 0)
    txt = f"{valor:,.2f}"
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def obter_valor_por_nome_ou_indice(linha, candidatos=None, indice=None, default="-"):
    candidatos = candidatos or []
    if isinstance(linha, pd.Series):
        for cand in candidatos:
            for col in linha.index:
                if normalizar_texto_coluna(col) == normalizar_texto_coluna(cand):
                    valor = linha[col]
                    return valor if str(valor).strip() != "" else default
        if indice is not None and 0 <= indice < len(linha.index):
            valor = linha.iloc[indice]
            return valor if str(valor).strip() != "" else default
    return default


def buscar_upload(cursor, tipos, conta=""):
    tipos = [t for t in tipos if str(t).strip()]
    if not tipos:
        return None

    placeholders = ",".join(["?"] * len(tipos))
    sql = f"SELECT caminho, nome_arquivo, conta, tipo_relatorio FROM uploads_arquivos WHERE tipo_relatorio IN ({placeholders})"
    params = list(tipos)

    if conta:
        sql += " AND (conta = ? OR conta IN ('BASE', 'AMBAS', 'TODAS'))"
        params.append(conta)

    sql += " ORDER BY id DESC LIMIT 1"
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return dict(row) if row else None


def montar_historico_mensal(df_vendas, mlb_norm, col_mlb, col_data, col_qtd):
    meses_ordem = ["AGOSTO", "SETEMBRO", "OUTUBRO", "NOVEMBRO", "DEZEMBRO", "JANEIRO", "FEVEREIRO", "MARÇO"]
    resultado = {mes: 0 for mes in meses_ordem}
    if df_vendas is None or df_vendas.empty:
        return resultado

    df = df_vendas.copy()
    df["MLB_FIXO"] = df[col_mlb].apply(normalizar_mlb_teste)
    df = df[df["MLB_FIXO"] == mlb_norm].copy()
    if df.empty:
        return resultado

    df["DATA_FIXA"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
    df["QTD_FIXA"] = df[col_qtd].apply(to_num)
    nomes = {1: "JANEIRO", 2: "FEVEREIRO", 3: "MARÇO", 4: "ABRIL", 5: "MAIO", 6: "JUNHO", 7: "JULHO", 8: "AGOSTO", 9: "SETEMBRO", 10: "OUTUBRO", 11: "NOVEMBRO", 12: "DEZEMBRO"}
    grupos = df.groupby(df["DATA_FIXA"].dt.month)["QTD_FIXA"].sum().to_dict()
    for mes_num, soma in grupos.items():
        try:
            nome = nomes.get(int(mes_num))
        except Exception:
            nome = None
        if nome in resultado:
            resultado[nome] = int(soma)
    return resultado


def montar_ultimas_vendas(df_vendas, mlb_norm, col_mlb, col_data, col_qtd, col_valor=None):
    janelas = [7, 15, 30, 45, 60]
    resultado = {dias: {"qtd": 0, "valor": 0.0} for dias in janelas}
    if df_vendas is None or df_vendas.empty:
        return resultado

    df = df_vendas.copy()
    df["MLB_FIXO"] = df[col_mlb].apply(normalizar_mlb_teste)
    df = df[df["MLB_FIXO"] == mlb_norm].copy()
    if df.empty:
        return resultado

    df["DATA_FIXA"] = pd.to_datetime(df[col_data], errors="coerce", dayfirst=True)
    df["QTD_FIXA"] = df[col_qtd].apply(to_num)
    if col_valor:
        df["VALOR_FIXO"] = df[col_valor].apply(to_num)
    else:
        df["VALOR_FIXO"] = 0.0

    hoje = pd.Timestamp(datetime.now().date())
    for dias in janelas:
        inicio = hoje - pd.Timedelta(days=dias)
        janela = df[(df["DATA_FIXA"] >= inicio) & (df["DATA_FIXA"] <= hoje)]
        resultado[dias]["qtd"] = int(janela["QTD_FIXA"].sum())
        resultado[dias]["valor"] = float(janela["VALOR_FIXO"].sum())
    return resultado


def carregar_linha_por_mlb_robusta(arquivo, candidatos_coluna_mlb, mlb_norm):
    if not arquivo:
        return None, None
    df = ler_planilha_com_cabecalho_inteligente(arquivo, candidatos_coluna_mlb)
    if df.empty:
        return None, None
    col = encontrar_coluna(df, candidatos_coluna_mlb, obrigatoria=True)
    df["MLB_FIXO"] = df[col].apply(normalizar_mlb_teste)
    linha = df[df["MLB_FIXO"] == mlb_norm]
    if linha.empty:
        return None, df
    return linha.iloc[0], df


def obter_status_ia_teste(card):
    sugestao = to_num(card.get("sugestao_meli", 0))
    cobertura = to_num(card.get("cobertura", 0))
    curva = str(card.get("curva", "") or "").upper()
    estoque_meli = str(card.get("estoque_meli", "") or "").upper()
    if sugestao > 0 and (("BAIXO" in estoque_meli) or curva in ["A", "B"]):
        return {"titulo": f"IA → ENVIAR {int(sugestao)}", "detalhe": "Baseado nos uploads", "classe": "ia-enviar"}
    if cobertura <= 0 and sugestao <= 0:
        return {"titulo": "IA → NÃO ENVIAR", "detalhe": "Métricas insuficientes", "classe": "ia-nao-enviar"}
    return {"titulo": "IA → ACOMPANHAR", "detalhe": "Validação em teste", "classe": "ia-repor"}


@app.route("/uploads")
def tela_uploads():
    init_uploads_db()
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM uploads_arquivos ORDER BY id DESC")
    uploads = c.fetchall()
    conn.close()
    return render_template("uploads.html", uploads=uploads)


@app.route("/uploads/enviar", methods=["POST"])
def enviar_upload():
    init_uploads_db()

    sistema = str(request.form.get("sistema", "")).strip()
    tipo = str(request.form.get("tipo", "")).strip()

    arquivos_para_salvar = [
        ("BASE", request.files.get("arquivo_base")),
        ("RENAFCAR", request.files.get("arquivo_renafcar")),
        ("4IDISTRIBUIDORA", request.files.get("arquivo_4i"))
    ]

    registros = []
    timestamp_base = datetime.now().strftime("%Y%m%d_%H%M%S")

    for indice, (conta, arquivo) in enumerate(arquivos_para_salvar, start=1):
        if not arquivo or not arquivo.filename:
            continue

        nome_seguro = secure_filename(arquivo.filename)
        nome_final = f"{timestamp_base}_{indice}_{nome_seguro}"
        caminho = os.path.join(UPLOAD_FOLDER_RELATORIOS, nome_final)
        arquivo.save(caminho)
        registros.append((nome_final, conta, sistema, tipo, caminho, agora_str(), "ENVIADO"))

    if not registros:
        return redirect("/uploads")

    conn = sqlite3.connect("status.db")
    c = conn.cursor()
    c.executemany(
        "INSERT INTO uploads_arquivos (nome_arquivo, conta, sistema, tipo_relatorio, caminho, data_upload, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
        registros
    )
    conn.commit()
    conn.close()
    return redirect("/uploads")


@app.route("/teste-cards")
def teste_cards():
    return render_template("teste_cards.html")


@app.route("/api/teste-card")
def api_teste_card():
    init_uploads_db()
    mlb = normalizar_mlb_teste(request.args.get("mlb", ""))
    conta = str(request.args.get("conta", "")).strip()
    if not mlb:
        return jsonify({"ok": False, "erro": "Informe um MLB válido."}), 400

    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    arq_base = buscar_upload(cursor, ["Evolução de Vendas"], conta)
    arq_vendas = buscar_upload(cursor, ["Vendas"], conta)
    arq_estoque = buscar_upload(cursor, ["Relatório Geral de Estoque"], conta)
    arq_planejamento = buscar_upload(cursor, ["Planejamento de Envio"], conta)
    arq_curva = buscar_upload(cursor, ["Curva ABCD"], conta)
    arq_cobertura = buscar_upload(cursor, ["Cobertura de Estoque"], conta)
    arq_unitario = buscar_upload(cursor, ["Saldo Unitário", "Saldo Unitario"], conta)
    arq_kit = buscar_upload(cursor, ["Saldo Kit", "Saldo Kit e Pares", "Saldo Kit/Pares", "Saldo Kit e pares"], conta)
    conn.close()

    card = {
        "mlb": mlb, "sku": "", "titulo": "", "conta": conta or "", "full": "NO FULL", "condicao": "", "lote": "", "exposicao": "", "selo": "", "obs_meli": "", "saldo_apos_envio": 0, "qtd_envio": 0,
        "outros_mlbs_mesmo_sku": 0, "titulos_iguais": 0, "info_meli": "-", "sugestao_meli": "-", "estoque_meli": "-", "ads": "SEM ADS", "curva": "-", "tendencia": "-", "cobertura": "-", "acao": "-",
        "historico_mensal": {m: 0 for m in ["AGOSTO","SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO","JANEIRO","FEVEREIRO","MARÇO"]},
        "ultimas_vendas": {7:{"qtd":0,"valor":0.0},15:{"qtd":0,"valor":0.0},30:{"qtd":0,"valor":0.0},45:{"qtd":0,"valor":0.0},60:{"qtd":0,"valor":0.0}},
        "estoque_total_signus": 0, "a_caminho": 0, "estoque_full": 0, "vai_ficar": 0, "fontes": {}
    }

    try:
        linha_base, df_base = carregar_linha_por_mlb_robusta(arq_base["caminho"] if arq_base else None, ["Código do Anúncio", "Codigo do Anuncio", "# de anúncio", "Anúncio"], mlb)
        if linha_base is not None:
            col_sku = encontrar_coluna(df_base, ["SKU"], obrigatoria=False)
            col_titulo = encontrar_coluna(df_base, ["Título", "Titulo"], obrigatoria=False)
            col_conta = encontrar_coluna(df_base, ["Nickname", "Conta"], obrigatoria=False)
            col_expo = encontrar_coluna(df_base, ["Exposição", "Exposicao"], obrigatoria=False)
            card["sku"] = str(linha_base.get(col_sku, "") or "") if col_sku else ""
            card["titulo"] = str(linha_base.get(col_titulo, "") or "") if col_titulo else ""
            card["conta"] = str(linha_base.get(col_conta, "") or card["conta"]) if col_conta else card["conta"]
            card["exposicao"] = str(linha_base.get(col_expo, "") or "") if col_expo else ""
            card["fontes"]["base"] = arq_base["nome_arquivo"]
            if card["sku"] and col_sku:
                df_tmp = df_base.copy()
                df_tmp["SKU_FIXO"] = df_tmp[col_sku].astype(str).str.strip().str.upper()
                card["outros_mlbs_mesmo_sku"] = max(int((df_tmp["SKU_FIXO"] == card["sku"].strip().upper()).sum()) - 1, 0)
            if card["titulo"] and col_titulo:
                df_tmp = df_base.copy()
                df_tmp["TITULO_FIXO"] = df_tmp[col_titulo].astype(str).str.strip().str.upper()
                card["titulos_iguais"] = max(int((df_tmp["TITULO_FIXO"] == card["titulo"].strip().upper()).sum()) - 1, 0)

        linha, df = carregar_linha_por_mlb_robusta(arq_estoque["caminho"] if arq_estoque else None, ["# de anúncio", "Código do Anúncio", "Anúncio"], mlb)
        if linha is not None:
            card["info_meli"] = str(obter_valor_por_nome_ou_indice(linha, ["Info Meli", "INFO MELI"], 27, "-"))
            col_entrada = encontrar_coluna(df, ["Entrada pendente"], obrigatoria=False)
            col_transfer = encontrar_coluna(df, ["Em transferencia", "Em transferência"], obrigatoria=False)
            col_full = encontrar_coluna(df, ["APTAS PRA VENDA", "Aptas pra venda"], obrigatoria=False)
            card["a_caminho"] = int(to_num(linha.get(col_entrada, 0)) + to_num(linha.get(col_transfer, 0)))
            card["estoque_full"] = int(to_num(linha.get(col_full, 0)))
            card["vai_ficar"] = int(card["a_caminho"] + card["estoque_full"])
            card["fontes"]["info_meli"] = arq_estoque["nome_arquivo"]
            card["fontes"]["estoque_bloco"] = arq_estoque["nome_arquivo"]

        linha, _ = carregar_linha_por_mlb_robusta(arq_planejamento["caminho"] if arq_planejamento else None, ["Anúncios com este produto", "Anuncios com este produto", "Anúncio"], mlb)
        if linha is not None:
            card["sugestao_meli"] = str(obter_valor_por_nome_ou_indice(linha, ["Sugestão Meli", "Sugestao Meli"], 17, "-"))
            card["estoque_meli"] = str(obter_valor_por_nome_ou_indice(linha, ["Estoque Meli"], 18, "-"))
            card["fontes"]["sugestao_meli"] = arq_planejamento["nome_arquivo"]
            card["fontes"]["estoque_meli"] = arq_planejamento["nome_arquivo"]

        linha, _ = carregar_linha_por_mlb_robusta(arq_curva["caminho"] if arq_curva else None, ["Anúncio", "Anuncio", "Código do Anúncio"], mlb)
        if linha is not None:
            card["curva"] = str(obter_valor_por_nome_ou_indice(linha, ["Curva 30d", "Curva 30D"], 14, "-"))
            card["tendencia"] = str(obter_valor_por_nome_ou_indice(linha, ["Tendência 30-0", "Tendencia 30-0", "Tendência 30", "Tendencia 30"], 15, "-"))
            card["fontes"]["curva"] = arq_curva["nome_arquivo"]
            card["fontes"]["tendencia"] = arq_curva["nome_arquivo"]

        linha, _ = carregar_linha_por_mlb_robusta(arq_cobertura["caminho"] if arq_cobertura else None, ["Anúncio", "Anuncio", "Código do Anúncio"], mlb)
        if linha is not None:
            card["cobertura"] = str(obter_valor_por_nome_ou_indice(linha, ["Cobertura"], 9, "-"))
            card["acao"] = str(obter_valor_por_nome_ou_indice(linha, ["Ação Recomendada", "Acao Recomendada"], 10, "-"))
            card["fontes"]["cobertura"] = arq_cobertura["nome_arquivo"]
            card["fontes"]["acao"] = arq_cobertura["nome_arquivo"]

        df_vendas = ler_planilha_com_cabecalho_inteligente(arq_vendas["caminho"] if arq_vendas else None, ["# de anúncio", "Código do Anúncio", "Anúncio"])
        if not df_vendas.empty:
            col_mlb_v = encontrar_coluna(df_vendas, ["# de anúncio", "Código do Anúncio", "Anúncio"], obrigatoria=True)
            col_data_v = encontrar_coluna(df_vendas, ["DATA DA VENDA", "Data da venda", "Data"], obrigatoria=True)
            col_qtd_v = encontrar_coluna(df_vendas, ["UNIDADE VENDIDA", "Unidade vendida", "Quantidade vendida", "Quantidade"], obrigatoria=True)
            col_valor_v = encontrar_coluna(df_vendas, ["VALOR DA VENDA", "Valor da venda", "Total", "Valor total"], obrigatoria=False)
            card["historico_mensal"] = montar_historico_mensal(df_vendas, mlb, col_mlb_v, col_data_v, col_qtd_v)
            card["ultimas_vendas"] = montar_ultimas_vendas(df_vendas, mlb, col_mlb_v, col_data_v, col_qtd_v, col_valor_v)
            card["fontes"]["vendas"] = arq_vendas["nome_arquivo"]

        sku_norm = str(card["sku"] or "").strip().upper()
        total_signus = 0
        for arq, tipo_nome in [(arq_unitario, "unitario"), (arq_kit, "kit")]:
            if not arq or not sku_norm:
                continue
            df_signus = ler_planilha_com_cabecalho_inteligente(arq["caminho"], ["SKU", "Quantidade disponível", "Quantidade disponivel"])
            if df_signus.empty:
                continue
            col_sku = encontrar_coluna(df_signus, ["SKU"], obrigatoria=False)
            col_qtd = encontrar_coluna(df_signus, ["Quantidade disponível", "Quantidade disponivel"], obrigatoria=False)
            if not col_sku and len(df_signus.columns) > 0:
                col_sku = df_signus.columns[0]
            if not col_qtd:
                idx = 6 if tipo_nome == "unitario" else 2
                if len(df_signus.columns) > idx:
                    col_qtd = df_signus.columns[idx]
            if col_sku and col_qtd:
                df_signus["SKU_FIXO"] = df_signus[col_sku].astype(str).str.strip().str.upper()
                total_signus += df_signus.loc[df_signus["SKU_FIXO"] == sku_norm, col_qtd].apply(to_num).sum()
        card["estoque_total_signus"] = int(total_signus)
        card["saldo_apos_envio"] = int(total_signus)
        if arq_unitario:
            card["fontes"]["unitario"] = arq_unitario["nome_arquivo"]
        if arq_kit:
            card["fontes"]["kit"] = arq_kit["nome_arquivo"]

        card["ia"] = obter_status_ia_teste(card)
        return jsonify({"ok": True, "dados": card})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)})


init_db()
init_uploads_db()



# ===== INICIO AJUSTE BASE UNIFICADA TESTE =====

def buscar_upload_like(cursor, termos=None, conta="", preferir_base=False):
    termos = [str(t).strip() for t in (termos or []) if str(t).strip()]
    sql = "SELECT caminho, nome_arquivo, conta, tipo_relatorio, sistema, data_upload, id FROM uploads_arquivos WHERE 1=1"
    params = []
    if termos:
        partes = []
        for termo in termos:
            partes.append("(tipo_relatorio LIKE ? OR nome_arquivo LIKE ? OR sistema LIKE ?)")
            like = f"%{termo}%"
            params.extend([like, like, like])
        sql += " AND (" + " OR ".join(partes) + ")"
    if conta:
        sql += " AND (conta = ? OR conta IN ('BASE', 'AMBAS', 'TODAS'))"
        params.append(conta)
    sql += " ORDER BY CASE WHEN conta='BASE' THEN 0 ELSE 1 END, id DESC"
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return dict(row) if row else None


def escolher_arquivo_base_unificada(cursor, conta=""):
    candidatos = [
        buscar_upload_like(cursor, [], conta, preferir_base=True),
        buscar_upload_like(cursor, ["base", "seconds", "evolucao de vendas", "evolução de vendas", "relatorio base"], conta, preferir_base=True),
        buscar_upload(cursor, ["Evolução de Vendas"], conta),
    ]
    for cand in candidatos:
        if cand and str(cand.get('caminho') or '').strip():
            return cand
    return None


def montar_base_unificada_uploads(conta=""):
    init_uploads_db()
    conn = sqlite3.connect("status.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    arq_base = escolher_arquivo_base_unificada(cursor, conta)
    arq_vendas = buscar_upload_like(cursor, ["vendas"], conta) or buscar_upload(cursor, ["Vendas"], conta)
    arq_estoque = buscar_upload_like(cursor, ["relatorio geral de estoque", "relatório geral de estoque", "estoque full"], conta) or buscar_upload(cursor, ["Relatório Geral de Estoque"], conta)
    arq_planejamento = buscar_upload_like(cursor, ["planejamento de envio", "planejamento"], conta) or buscar_upload(cursor, ["Planejamento de Envio"], conta)
    arq_curva = buscar_upload_like(cursor, ["curva abcd", "curva"], conta) or buscar_upload(cursor, ["Curva ABCD"], conta)
    arq_cobertura = buscar_upload_like(cursor, ["cobertura de estoque", "cobertura"], conta) or buscar_upload(cursor, ["Cobertura de Estoque"], conta)
    arq_unitario = buscar_upload_like(cursor, ["saldo unitario", "saldo unitário", "signus"], conta) or buscar_upload(cursor, ["Saldo Unitário", "Saldo Unitario"], conta)
    arq_kit = buscar_upload_like(cursor, ["saldo kit", "kit e pares", "pares"], conta) or buscar_upload(cursor, ["Saldo Kit", "Saldo Kit e Pares", "Saldo Kit/Pares", "Saldo Kit e pares"], conta)
    conn.close()

    if not arq_base:
        raise RuntimeError("Nenhum arquivo base foi encontrado nos uploads. Envie primeiro o Arquivo Base.")

    df_base = ler_planilha_com_cabecalho_inteligente(arq_base["caminho"], ["Código do Anúncio", "Codigo do Anuncio", "MLB", "SKU", "Título", "Titulo", "Nickname"])
    if df_base is None or df_base.empty:
        raise RuntimeError("O arquivo base foi encontrado, mas não foi possível ler os dados dele.")

    col_mlb_base = encontrar_coluna(df_base, ["Código do Anúncio", "Codigo do Anuncio", "MLB", "# de anúncio", "Anúncio"], obrigatoria=True)
    col_sku_base = encontrar_coluna(df_base, ["SKU"], obrigatoria=False)
    col_titulo_base = encontrar_coluna(df_base, ["Título", "Titulo"], obrigatoria=False)
    col_conta_base = encontrar_coluna(df_base, ["Nickname", "Conta"], obrigatoria=False)
    col_expo_base = encontrar_coluna(df_base, ["Exposição", "Exposicao"], obrigatoria=False)

    base = df_base.copy().fillna("")
    base["MLB_FIXO"] = base[col_mlb_base].apply(normalizar_mlb_teste)
    base = base[base["MLB_FIXO"].astype(str).str.startswith("MLB")].copy()
    if col_conta_base and conta:
        base = base[base[col_conta_base].astype(str).str.strip().str.upper() == conta.strip().upper()].copy()
    base = base.drop_duplicates(subset=["MLB_FIXO"], keep="first").reset_index(drop=True)

    mapas = {}
    fontes = {
        "base": arq_base.get("nome_arquivo") if arq_base else "",
        "vendas": arq_vendas.get("nome_arquivo") if arq_vendas else "",
        "estoque": arq_estoque.get("nome_arquivo") if arq_estoque else "",
        "planejamento": arq_planejamento.get("nome_arquivo") if arq_planejamento else "",
        "curva": arq_curva.get("nome_arquivo") if arq_curva else "",
        "cobertura": arq_cobertura.get("nome_arquivo") if arq_cobertura else "",
        "unitario": arq_unitario.get("nome_arquivo") if arq_unitario else "",
        "kit": arq_kit.get("nome_arquivo") if arq_kit else "",
    }

    def _mapa_por(df, coluna_chave, chave='MLB'):
        if df is None or df.empty or not coluna_chave:
            return {}
        tmp = df.copy().fillna("")
        if chave == 'SKU':
            tmp["CHAVE_FIXA"] = tmp[coluna_chave].astype(str).str.strip().str.upper()
        else:
            tmp["CHAVE_FIXA"] = tmp[coluna_chave].apply(normalizar_mlb_teste)
        tmp = tmp[tmp["CHAVE_FIXA"].astype(str) != ""].drop_duplicates(subset=["CHAVE_FIXA"], keep="first")
        return {str(row["CHAVE_FIXA"]): row for _, row in tmp.iterrows()}

    df_unitario = ler_planilha_com_cabecalho_inteligente(arq_unitario["caminho"], ["SKU", "Produto", "Linha de produto"]) if arq_unitario else pd.DataFrame()
    col_sku_unit = encontrar_coluna(df_unitario, ["SKU"], obrigatoria=False) if not df_unitario.empty else None
    mapa_unitario = _mapa_por(df_unitario, col_sku_unit, 'SKU') if col_sku_unit else {}

    df_kit = ler_planilha_com_cabecalho_inteligente(arq_kit["caminho"], ["SKU", "Produto"]) if arq_kit else pd.DataFrame()
    col_sku_kit = encontrar_coluna(df_kit, ["SKU"], obrigatoria=False) if not df_kit.empty else None
    mapa_kit = _mapa_por(df_kit, col_sku_kit, 'SKU') if col_sku_kit else {}

    df_estoque = ler_planilha_com_cabecalho_inteligente(arq_estoque["caminho"], ["MLB", "Tempo até esgotar estoque", "Unidades que ocupan espacio en Full"]) if arq_estoque else pd.DataFrame()
    col_mlb_estoque = encontrar_coluna(df_estoque, ["MLB", "Código do Anúncio", "Codigo do Anuncio"], obrigatoria=False) if not df_estoque.empty else None
    mapa_estoque = _mapa_por(df_estoque, col_mlb_estoque, 'MLB') if col_mlb_estoque else {}

    df_planejamento = ler_planilha_com_cabecalho_inteligente(arq_planejamento["caminho"], ["MLB", "Unidades sugeridas para enviar", "Nível de estoque", "Observações"]) if arq_planejamento else pd.DataFrame()
    col_mlb_planej = encontrar_coluna(df_planejamento, ["MLB", "Código do Anúncio", "Codigo do Anuncio"], obrigatoria=False) if not df_planejamento.empty else None
    mapa_planejamento = _mapa_por(df_planejamento, col_mlb_planej, 'MLB') if col_mlb_planej else {}

    df_curva = ler_planilha_com_cabecalho_inteligente(arq_curva["caminho"], ["MLB", "Curva 30d", "Tendência 30-0", "Preço unitário de venda do anúncio (BRL)"]) if arq_curva else pd.DataFrame()
    col_mlb_curva = encontrar_coluna(df_curva, ["MLB", "Código do Anúncio", "Codigo do Anuncio"], obrigatoria=False) if not df_curva.empty else None
    mapa_curva = _mapa_por(df_curva, col_mlb_curva, 'MLB') if col_mlb_curva else {}

    df_cobertura = ler_planilha_com_cabecalho_inteligente(arq_cobertura["caminho"], ["MLB", "Cobertura", "Ação Recomendada"]) if arq_cobertura else pd.DataFrame()
    col_mlb_cob = encontrar_coluna(df_cobertura, ["MLB", "Código do Anúncio", "Codigo do Anuncio"], obrigatoria=False) if not df_cobertura.empty else None
    mapa_cobertura = _mapa_por(df_cobertura, col_mlb_cob, 'MLB') if col_mlb_cob else {}

    df_vendas = ler_planilha_com_cabecalho_inteligente(arq_vendas["caminho"], ["MLB", "Data", "Quantidade", "Unidades"]) if arq_vendas else pd.DataFrame()
    col_mlb_vendas = encontrar_coluna(df_vendas, ["MLB", "Código do Anúncio", "Codigo do Anuncio"], obrigatoria=False) if not df_vendas.empty else None
    col_data_vendas = encontrar_coluna(df_vendas, ["Data", "Data da venda", "Date", "Data do pedido"], obrigatoria=False) if not df_vendas.empty else None
    col_qtd_vendas = encontrar_coluna(df_vendas, ["Quantidade", "Unidades", "Quantidade vendida", "Qtd", "Unidades vendidas"], obrigatoria=False) if not df_vendas.empty else None
    col_valor_vendas = encontrar_coluna(df_vendas, ["Valor total", "Total", "Valor", "Preço unitário de venda do anúncio (BRL)", "Preco unitario de venda do anuncio (BRL)"], obrigatoria=False) if not df_vendas.empty else None

    cards = []
    for _, row in base.iterrows():
        mlb = normalizar_mlb_teste(row.get(col_mlb_base, ""))
        sku = str(row.get(col_sku_base, "") or "").strip() if col_sku_base else ""
        conta_row = str(row.get(col_conta_base, "") or "").strip() if col_conta_base else ""
        if not mlb:
            continue
        unit = mapa_unitario.get(sku.strip().upper(), pd.Series(dtype=object)) if sku else pd.Series(dtype=object)
        kit = mapa_kit.get(sku.strip().upper(), pd.Series(dtype=object)) if sku else pd.Series(dtype=object)
        estoque = mapa_estoque.get(mlb, pd.Series(dtype=object))
        planej = mapa_planejamento.get(mlb, pd.Series(dtype=object))
        curva = mapa_curva.get(mlb, pd.Series(dtype=object))
        cobertura = mapa_cobertura.get(mlb, pd.Series(dtype=object))

        hist = montar_historico_mensal(df_vendas, mlb, col_mlb_vendas, col_data_vendas, col_qtd_vendas) if (not df_vendas.empty and col_mlb_vendas and col_data_vendas and col_qtd_vendas) else {m:0 for m in ["AGOSTO","SETEMBRO","OUTUBRO","NOVEMBRO","DEZEMBRO","JANEIRO","FEVEREIRO","MARÇO"]}
        ult = montar_ultimas_vendas(df_vendas, mlb, col_mlb_vendas, col_data_vendas, col_qtd_vendas, col_valor_vendas) if (not df_vendas.empty and col_mlb_vendas and col_data_vendas and col_qtd_vendas) else {7:{"qtd":0,"valor":0},15:{"qtd":0,"valor":0},30:{"qtd":0,"valor":0},45:{"qtd":0,"valor":0},60:{"qtd":0,"valor":0}}

        qtd_signus = to_num(obter_valor_por_nome_ou_indice(unit, ["Quantidade disponível", "Saldo disponível", "Quantidade", "Estoque", "Saldo"], default=0))
        if qtd_signus <= 0:
            qtd_signus += to_num(obter_valor_por_nome_ou_indice(kit, ["Quantidade disponível", "Saldo disponível", "Quantidade", "Estoque", "Saldo"], default=0))
        estoque_full = to_num(obter_valor_por_nome_ou_indice(estoque, ["Unidades que ocupan espacio en Full", "Unidades que ocupam espaço em Full", "Estoque Full", "Full"], default=0))
        a_caminho = to_num(obter_valor_por_nome_ou_indice(estoque, ["Entrada pendente + Em transferência", "Entrada pendente + Em transferencia", "A caminho", "Em transferência"], default=0))
        sugestao = to_num(obter_valor_por_nome_ou_indice(planej, ["Unidades sugeridas para enviar", "Sugestão de envio", "Sugestao de envio"], default=0))
        vai_ficar = estoque_full + sugestao
        info_meli = obter_valor_por_nome_ou_indice(estoque, ["Tempo até esgotar estoque", "Info Meli"], default="-")
        nivel_estoque = obter_valor_por_nome_ou_indice(planej, ["Nível de estoque", "Nivel de estoque", "Estoque Meli"], default="-")
        observacoes = obter_valor_por_nome_ou_indice(planej, ["Observações", "Observacoes", "Obs"], default="")
        curva_30 = obter_valor_por_nome_ou_indice(curva, ["Curva 30d", "Curva"], default="-")
        tendencia = obter_valor_por_nome_ou_indice(curva, ["Tendência 30-0", "Tendencia 30-0", "Tendência", "Tendencia"], default="-")
        cobertura_txt = obter_valor_por_nome_ou_indice(cobertura, ["Cobertura", "Cobertura de Estoque"], default="-")
        acao = obter_valor_por_nome_ou_indice(cobertura, ["Ação Recomendada", "Acao Recomendada"], default="-")
        preco_unit = to_num(obter_valor_por_nome_ou_indice(curva, ["Preço unitário de venda do anúncio (BRL)", "Preco unitario de venda do anuncio (BRL)", "Preço unitário", "Preco unitario"], default=0))

        vendas30 = ult[30]["qtd"]
        valor30 = ult[30]["valor"] if ult[30]["valor"] else (vendas30 * preco_unit)
        ads = "COM ADS" if to_num(valor30) > 0 else "SEM ADS"
        qtd_envio = int(sugestao) if float(sugestao).is_integer() else round(sugestao,2)
        saldo_apos = qtd_signus - sugestao

        card = {
            "Código do Anúncio": mlb,
            "SKU": sku,
            "Título": str(row.get(col_titulo_base, "") or "").strip() if col_titulo_base else "",
            "Nickname": conta_row,
            "Exposição": str(row.get(col_expo_base, "") or "").strip() if col_expo_base else "",
            "SELO": str(obter_valor_por_nome_ou_indice(unit, ["Selo"], default="")).strip(),
            "LINHA": str(obter_valor_por_nome_ou_indice(unit, ["Linha de produto"], default="")).strip(),
            "PRODUTO": str(obter_valor_por_nome_ou_indice(unit, ["Produto"], default="")).strip(),
            "LOCALIZAÇÃO": str(obter_valor_por_nome_ou_indice(unit, ["Localização no estoque", "Localizacao no estoque"], default="")).strip(),
            "ADS": ads,
            "INFO MELI": info_meli,
            "SUGESTÃO MELI": qtd_envio,
            "ESTOQUE MELI": nivel_estoque,
            "OBSERVAÇÕES": observacoes,
            "CURVA 30D": curva_30,
            "TENDÊNCIA 30-0": tendencia,
            "COBERTURA": cobertura_txt,
            "AÇÃO RECOMENDADA": acao,
            "ESTOQUE TOTAL SIGNUS": inteiro_ou_zero(qtd_signus),
            "A CAMINHO DO FULL": inteiro_ou_zero(a_caminho),
            "ESTOQUE FULL": inteiro_ou_zero(estoque_full),
            "ESTOQUE QUE VAI FICAR NO FULL": inteiro_ou_zero(vai_ficar),
            "Saldo após envio": inteiro_ou_zero(saldo_apos),
            "Quantidade para Enviar": qtd_envio,
            "SAUDE DO ESTOQUE 4i": str(obter_valor_por_nome_ou_indice(cobertura, ["Cobertura", "Cobertura de Estoque"], default="")).strip(),
            "7 DIAS": inteiro_ou_zero(ult[7]["qtd"]),
            "15 DIAS": inteiro_ou_zero(ult[15]["qtd"]),
            "30 DIAS": inteiro_ou_zero(ult[30]["qtd"]),
            "45 DIAS": inteiro_ou_zero(ult[45]["qtd"]),
            "60 DIAS": inteiro_ou_zero(ult[60]["qtd"]),
            "Total de Vendas 7 DIAS": formatar_moeda_br(ult[7]["valor"]),
            "Total de Vendas 15 DIAS": formatar_moeda_br(ult[15]["valor"]),
            "Total de Vendas 30 DIAS": formatar_moeda_br(ult[30]["valor"]),
            "Total de Vendas 45 DIAS": formatar_moeda_br(ult[45]["valor"]),
            "Total de Vendas 60 DIAS": formatar_moeda_br(ult[60]["valor"]),
            **hist,
            "fontes": fontes,
        }
        ia = obter_status_ia_teste({"sugestao_meli": sugestao, "cobertura": to_num(cobertura_txt), "curva": curva_30, "estoque_meli": nivel_estoque})
        card["ia_status"] = ia
        cards.append(card)

    return cards, fontes


@app.route('/api/teste-cards-list')
def api_teste_cards_list_auto():
    conta = str(request.args.get('conta', '')).strip()
    try:
        cards, fontes = montar_base_unificada_uploads(conta)
        busca = normalizar_texto_coluna(request.args.get('busca', ''))
        if busca:
            filtrados = []
            for item in cards:
                alvo = ' '.join([
                    str(item.get('Código do Anúncio','')),
                    str(item.get('SKU','')),
                    str(item.get('Título','')),
                    str(item.get('Nickname','')),
                ])
                if busca in normalizar_texto_coluna(alvo):
                    filtrados.append(item)
            cards = filtrados
        cards = sorted(cards, key=lambda x: (str(x.get('Nickname','')), str(x.get('Código do Anúncio',''))))
        return jsonify({"ok": True, "total": len(cards), "dados": cards, "fontes": fontes})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500

# ===== FIM AJUSTE BASE UNIFICADA TESTE =====

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)