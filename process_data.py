"""
Script para baixar e processar as planilhas de ITBI da Prefeitura de SP.
Converte todos os arquivos XLSX em um banco de dados SQLite consolidado.

Uso:
    python process_data.py          # Baixa e processa tudo
    python process_data.py --skip-download  # Só processa (se já baixou)
"""

import os
import sys
import sqlite3
import unicodedata
import re
import argparse
import time
from pathlib import Path

import requests
import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
DB_PATH = Path(__file__).parent / "itbi.db"

# URLs de todas as planilhas por ano
URLS = {
    2025: "https://prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/itbi/GUIAS%20DE%20ITBI%20PAGAS%20%2828012026%29%20XLS.xlsx",
    2024: "https://prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/itbi/GUIAS-DE-ITBI-PAGAS-2024.xlsx",
    2023: "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/XLSX/GUIAS-DE-ITBI-PAGAS-2023.xlsx",
    2022: "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/XLSX/GUIAS_DE_ITBI_PAGAS_12-2022.xlsx",
    2021: "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/itbi/ITBI_Setembro_2022/GUIAS_DE_ITBI_PAGAS_(2021).xlsx",
    2020: "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/itbi/ITBI_Setembro_2022/GUIAS_DE_ITBI_PAGAS_(2020).xlsx",
    2019: "https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/itbi/ITBI_Setembro_2022/GUIAS_DE_ITBI_PAGAS_(2019).xlsx",
}

# Anos 2006-2018 seguem padrão consistente
for year in range(2006, 2019):
    URLS[year] = f"https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/fazenda/arquivos/itbi/guias_de_itbi_pagas_{year}.xlsx"


# Mapeamento de abreviações comuns de logradouros
ABBREVIATIONS = {
    "R": "RUA", "AV": "AVENIDA", "AL": "ALAMEDA", "TV": "TRAVESSA",
    "PC": "PRACA", "PCA": "PRACA", "EST": "ESTRADA", "LG": "LARGO",
    "VL": "VILA", "ROD": "RODOVIA", "BD": "BOULEVARD", "BL": "BLOCO",
    "CJ": "CONJUNTO", "GAL": "GENERAL", "DR": "DOUTOR", "DRA": "DOUTORA",
    "PROF": "PROFESSOR", "PROFA": "PROFESSORA", "ENG": "ENGENHEIRO",
    "MAL": "MARECHAL", "CEL": "CORONEL", "CAP": "CAPITAO",
    "TEN": "TENENTE", "SGT": "SARGENTO", "BRG": "BRIGADEIRO",
    "PRES": "PRESIDENTE", "GOV": "GOVERNADOR", "SEN": "SENADOR",
    "DEP": "DEPUTADO", "MIN": "MINISTRO", "PE": "PADRE",
    "S": "SAO", "STA": "SANTA", "STO": "SANTO",
    "N S": "NOSSA SENHORA", "NS": "NOSSA SENHORA",
    "COM": "COMENDADOR", "DES": "DESEMBARGADOR", "EMB": "EMBAIXADOR",
    "VER": "VEREADOR", "VISC": "VISCONDE", "CDE": "CONDE",
    "BQ": "BECO", "PQ": "PARQUE", "JD": "JARDIM",
    "VD": "VIADUTO", "PT": "PONTE", "LRG": "LARGO",
}


def normalize_text(text):
    """Remove acentos, converte para maiúsculas e limpa espaços."""
    if not text or not isinstance(text, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return re.sub(r"\s+", " ", ascii_text.upper().strip())


def expand_abbreviations(text):
    """Expande abreviações no nome do logradouro para facilitar buscas."""
    normalized = normalize_text(text)
    if not normalized:
        return ""
    words = normalized.split()
    expanded = []
    for word in words:
        clean = word.replace(".", "")
        if clean in ABBREVIATIONS:
            expanded.append(ABBREVIATIONS[clean])
        else:
            expanded.append(clean)
    return " ".join(expanded)


def download_file(url, dest_path, max_retries=3):
    """Baixa um arquivo com retry e barra de progresso simples."""
    for attempt in range(1, max_retries + 1):
        print(f"  Baixando (tentativa {attempt}/{max_retries}): {url}")
        try:
            resp = requests.get(url, stream=True, timeout=180,
                                headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = downloaded * 100 // total
                        mb_down = downloaded / (1024 * 1024)
                        mb_total = total / (1024 * 1024)
                        print(f"\r  Progresso: {pct}% ({mb_down:.1f}MB / {mb_total:.1f}MB)", end="", flush=True)
            print()
            return True
        except Exception as e:
            print(f"\n  ERRO (tentativa {attempt}): {e}")
            if dest_path.exists():
                dest_path.unlink()
            if attempt < max_retries:
                wait = attempt * 5
                print(f"  Aguardando {wait}s antes de tentar novamente...")
                time.sleep(wait)
    return False


def download_all():
    """Baixa todas as planilhas."""
    DATA_DIR.mkdir(exist_ok=True)
    for year in sorted(URLS.keys()):
        dest = DATA_DIR / f"itbi_{year}.xlsx"
        if dest.exists():
            print(f"[{year}] Arquivo já existe, pulando: {dest.name}")
            continue
        print(f"[{year}] Baixando planilha...")
        success = download_file(URLS[year], dest)
        if success:
            print(f"[{year}] OK: {dest.name}")
        else:
            print(f"[{year}] FALHA no download")


def find_column_mapping(columns):
    """Mapeia colunas do DataFrame para nomes padronizados."""
    mapping = {}
    for col in columns:
        normalized = normalize_text(str(col)).lower()

        if "data" in normalized and "transac" in normalized:
            mapping["data_transacao"] = col
        elif "valor" in normalized and "transac" in normalized and "declarad" in normalized:
            mapping["valor_transacao"] = col
        elif "valor" in normalized and "transac" in normalized and "valor_transacao" not in mapping:
            mapping["valor_transacao"] = col
        elif "nome" in normalized and "logradouro" in normalized:
            mapping["logradouro"] = col
        elif "logradouro" in normalized and "logradouro" not in mapping:
            mapping["logradouro"] = col
        elif normalized in ("numero", "numero do imovel", "n", "n do imovel"):
            mapping["numero"] = col
        elif "complemento" in normalized:
            mapping["complemento"] = col
        elif "bairro" in normalized:
            mapping["bairro"] = col
        elif "tipo" in normalized and "imovel" in normalized:
            mapping["tipo_imovel"] = col
        elif "area" in normalized and ("terreno" in normalized or "construi" in normalized):
            if "area" not in mapping:
                mapping["area"] = col

    return mapping


def parse_value(val):
    """Converte valor monetário para float."""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    s = s.replace("R$", "").replace("r$", "").strip()
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_date(val):
    """Converte data para string YYYY-MM-DD."""
    if pd.isna(val):
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    m = re.match(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"
    return s


def process_xlsx(filepath, year, cursor):
    """Processa um arquivo XLSX usando pandas e insere os dados no banco."""
    print(f"  Lendo: {filepath.name} ...", flush=True)
    t0 = time.time()

    try:
        # pandas + openpyxl é MUITO mais rápido que openpyxl célula por célula
        xlsx = pd.ExcelFile(filepath, engine="openpyxl")
    except Exception as e:
        print(f"  ERRO ao abrir {filepath.name}: {e}")
        return 0

    total_inserted = 0

    for sheet_name in xlsx.sheet_names:
        print(f"    Aba: {sheet_name}", end="", flush=True)

        # Ler todas as linhas como texto para encontrar o cabeçalho
        try:
            df_raw = xlsx.parse(sheet_name, header=None, nrows=20)
        except Exception as e:
            print(f" - ERRO ao ler aba: {e}")
            continue

        # Encontrar a linha de cabeçalho
        header_row = None
        target_columns = ["data de transac", "valor de transac", "logradouro"]
        for idx in range(len(df_raw)):
            row_text = " ".join(
                normalize_text(str(v)).lower()
                for v in df_raw.iloc[idx] if pd.notna(v)
            )
            matches = sum(1 for t in target_columns if t in row_text)
            if matches >= 2:
                header_row = idx
                break

        if header_row is None:
            print(" - cabeçalho não encontrado, pulando")
            continue

        # Re-ler a aba com o cabeçalho correto
        try:
            df = xlsx.parse(sheet_name, header=header_row)
        except Exception as e:
            print(f" - ERRO ao re-ler aba: {e}")
            continue

        # Mapear colunas
        col_map = find_column_mapping(df.columns)
        if "logradouro" not in col_map:
            print(" - coluna logradouro não encontrada, pulando")
            continue

        print(f" ({len(df)} linhas)", flush=True)

        # Processar em lote
        batch = []
        for _, row in df.iterrows():
            logradouro_raw = row.get(col_map["logradouro"])
            if pd.isna(logradouro_raw):
                continue

            logradouro = str(logradouro_raw).strip()
            if not logradouro:
                continue

            logradouro_normalizado = normalize_text(logradouro)
            logradouro_expandido = expand_abbreviations(logradouro)

            data_transacao = None
            if "data_transacao" in col_map:
                data_transacao = parse_date(row.get(col_map["data_transacao"]))

            valor = None
            if "valor_transacao" in col_map:
                valor = parse_value(row.get(col_map["valor_transacao"]))

            numero = None
            if "numero" in col_map:
                num_val = row.get(col_map["numero"])
                if pd.notna(num_val):
                    numero = str(num_val).strip()

            complemento = None
            if "complemento" in col_map:
                comp_val = row.get(col_map["complemento"])
                if pd.notna(comp_val):
                    complemento = str(comp_val).strip()

            bairro = None
            if "bairro" in col_map:
                bairro_val = row.get(col_map["bairro"])
                if pd.notna(bairro_val):
                    bairro = str(bairro_val).strip()

            tipo_imovel = None
            if "tipo_imovel" in col_map:
                tipo_val = row.get(col_map["tipo_imovel"])
                if pd.notna(tipo_val):
                    tipo_imovel = str(tipo_val).strip()

            area = None
            if "area" in col_map:
                area = parse_value(row.get(col_map["area"]))

            batch.append((
                year, data_transacao, logradouro, logradouro_normalizado,
                logradouro_expandido, numero, complemento, bairro,
                tipo_imovel, area, valor
            ))

        if batch:
            cursor.executemany("""
                INSERT INTO transacoes (
                    ano, data_transacao, logradouro, logradouro_normalizado,
                    logradouro_expandido, numero, complemento, bairro,
                    tipo_imovel, area, valor_transacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            total_inserted += len(batch)
            print(f"    Inseridos: {len(batch)} registros")

    xlsx.close()
    elapsed = time.time() - t0
    print(f"  Tempo: {elapsed:.1f}s")
    return total_inserted


def create_database():
    """Cria o banco de dados SQLite e processa todos os arquivos."""
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    # WAL mode para escrita mais rápida
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    cursor.execute("""
        CREATE TABLE transacoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ano INTEGER,
            data_transacao TEXT,
            logradouro TEXT,
            logradouro_normalizado TEXT,
            logradouro_expandido TEXT,
            numero TEXT,
            complemento TEXT,
            bairro TEXT,
            tipo_imovel TEXT,
            area REAL,
            valor_transacao REAL
        )
    """)

    total = 0
    for year in sorted(URLS.keys()):
        filepath = DATA_DIR / f"itbi_{year}.xlsx"
        if not filepath.exists():
            print(f"[{year}] Arquivo não encontrado: {filepath.name}, pulando")
            continue
        print(f"[{year}] Processando...")
        count = process_xlsx(filepath, year, cursor)
        total += count
        conn.commit()
        print(f"[{year}] Total parcial: {count} registros\n")

    # Cria índices para busca rápida
    print("Criando índices...")
    cursor.execute("CREATE INDEX idx_logradouro_norm ON transacoes(logradouro_normalizado)")
    cursor.execute("CREATE INDEX idx_logradouro_exp ON transacoes(logradouro_expandido)")
    cursor.execute("CREATE INDEX idx_numero ON transacoes(numero)")
    cursor.execute("CREATE INDEX idx_ano ON transacoes(ano)")
    cursor.execute("CREATE INDEX idx_bairro ON transacoes(bairro)")
    conn.commit()

    print(f"\n{'='*60}")
    print(f"TOTAL DE REGISTROS: {total}")
    print(f"Banco de dados salvo em: {DB_PATH}")
    print(f"{'='*60}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Processa planilhas ITBI de SP")
    parser.add_argument("--skip-download", action="store_true",
                        help="Pula o download (usa arquivos já baixados)")
    args = parser.parse_args()

    if not args.skip_download:
        print("=" * 60)
        print("ETAPA 1: DOWNLOAD DAS PLANILHAS")
        print("=" * 60)
        download_all()

    print("\n" + "=" * 60)
    print("ETAPA 2: PROCESSAMENTO DOS DADOS")
    print("=" * 60)
    create_database()


if __name__ == "__main__":
    main()
