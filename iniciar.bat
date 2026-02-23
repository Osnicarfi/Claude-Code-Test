@echo off
echo ============================================
echo   ITBI SP - Transacoes Imobiliarias
echo ============================================
echo.

cd /d "%~dp0"

if not exist "itbi.db" (
    echo O banco de dados ainda nao foi criado.
    echo Vou baixar e processar as planilhas agora.
    echo Isso pode levar bastante tempo na primeira vez...
    echo.
    python process_data.py
    echo.
)

if exist "itbi.db" (
    echo Banco de dados encontrado! Iniciando o painel...
    echo.
    echo =============================================
    echo   ABRA O NAVEGADOR NO ENDERECO ABAIXO:
    echo   http://localhost:5000
    echo =============================================
    echo.
    echo Para parar o servidor, feche esta janela.
    echo.
    start http://localhost:5000
    python app.py
) else (
    echo ERRO: Nao foi possivel criar o banco de dados.
    echo Verifique se as planilhas foram baixadas na pasta "data".
    pause
)
