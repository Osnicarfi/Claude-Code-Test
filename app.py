"""
Servidor web para consulta de transações ITBI de São Paulo.
Uso: python app.py
Acesse: http://localhost:5000
"""

import sqlite3
import unicodedata
import re
from pathlib import Path

from flask import Flask, render_template, request, jsonify

app = Flask(__name__)
DB_PATH = Path(__file__).parent / "itbi.db"

# Mapeamento de abreviações (igual ao process_data.py)
ABBREVIATIONS = {
    "R": "RUA", "AV": "AVENIDA", "AL": "ALAMEDA", "TV": "TRAVESSA",
    "PC": "PRACA", "PCA": "PRACA", "EST": "ESTRADA", "LG": "LARGO",
    "VL": "VILA", "ROD": "RODOVIA", "BD": "BOULEVARD", "BQ": "BECO",
    "PQ": "PARQUE", "JD": "JARDIM", "VD": "VIADUTO", "PT": "PONTE",
    "LRG": "LARGO",
}

# Reverso: de expandido para abreviado
REVERSE_ABBR = {v: k for k, v in ABBREVIATIONS.items()}


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def normalize_text(text):
    if not text:
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", ascii_text.upper().strip())


def expand_query(text):
    """Expande abreviações na busca do usuário."""
    normalized = normalize_text(text)
    words = normalized.split()
    expanded = []
    for word in words:
        clean = word.replace(".", "")
        if clean in ABBREVIATIONS:
            expanded.append(ABBREVIATIONS[clean])
        else:
            expanded.append(clean)
    return " ".join(expanded)


def abbreviate_query(text):
    """Abrevia termos na busca do usuário para combinar com dados originais."""
    normalized = normalize_text(text)
    words = normalized.split()
    abbreviated = []
    for word in words:
        clean = word.replace(".", "")
        if clean in REVERSE_ABBR:
            abbreviated.append(REVERSE_ABBR[clean])
        else:
            abbreviated.append(clean)
    return " ".join(abbreviated)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search")
def search():
    logradouro = request.args.get("logradouro", "").strip()
    numero = request.args.get("numero", "").strip()

    if not logradouro:
        return jsonify({"error": "Informe o nome do logradouro", "results": []})

    # Gera variações da busca
    normalized = normalize_text(logradouro)
    expanded = expand_query(logradouro)
    abbreviated = abbreviate_query(logradouro)

    # Busca flexível: tenta combinar com nome normalizado OU expandido
    # Usando LIKE com % para busca parcial
    query = """
        SELECT
            data_transacao, logradouro, numero, complemento,
            bairro, tipo_imovel, area, valor_transacao, ano
        FROM transacoes
        WHERE (
            logradouro_normalizado LIKE ?
            OR logradouro_expandido LIKE ?
            OR logradouro_normalizado LIKE ?
            OR logradouro_expandido LIKE ?
        )
    """
    params = [
        f"%{normalized}%",
        f"%{normalized}%",
        f"%{expanded}%",
        f"%{abbreviated}%",
    ]

    if numero:
        query += " AND numero = ?"
        params.append(numero)

    query += " ORDER BY data_transacao DESC LIMIT 500"

    conn = get_db()
    try:
        rows = conn.execute(query, params).fetchall()
        results = []
        for row in rows:
            results.append({
                "data_transacao": row["data_transacao"],
                "logradouro": row["logradouro"],
                "numero": row["numero"],
                "complemento": row["complemento"],
                "bairro": row["bairro"],
                "tipo_imovel": row["tipo_imovel"],
                "area": row["area"],
                "valor_transacao": row["valor_transacao"],
                "ano": row["ano"],
            })
        return jsonify({"results": results, "total": len(results)})
    finally:
        conn.close()


@app.route("/api/suggest")
def suggest():
    """Autocomplete de nomes de logradouro."""
    q = request.args.get("q", "").strip()
    if len(q) < 3:
        return jsonify({"suggestions": []})

    normalized = normalize_text(q)
    expanded = expand_query(q)

    conn = get_db()
    try:
        rows = conn.execute("""
            SELECT DISTINCT logradouro
            FROM transacoes
            WHERE logradouro_normalizado LIKE ? OR logradouro_expandido LIKE ?
            LIMIT 15
        """, (f"%{normalized}%", f"%{expanded}%")).fetchall()
        return jsonify({"suggestions": [row["logradouro"] for row in rows]})
    finally:
        conn.close()


@app.route("/api/stats")
def stats():
    """Estatísticas gerais do banco."""
    conn = get_db()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM transacoes").fetchone()["c"]
        years = conn.execute(
            "SELECT MIN(ano) as min_ano, MAX(ano) as max_ano FROM transacoes"
        ).fetchone()
        return jsonify({
            "total_transacoes": total,
            "ano_inicio": years["min_ano"],
            "ano_fim": years["max_ano"],
        })
    except Exception:
        return jsonify({"total_transacoes": 0, "ano_inicio": None, "ano_fim": None})
    finally:
        conn.close()


if __name__ == "__main__":
    if not DB_PATH.exists():
        print("ERRO: Banco de dados não encontrado!")
        print("Execute primeiro: python process_data.py")
        exit(1)
    print("Servidor iniciado em http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
