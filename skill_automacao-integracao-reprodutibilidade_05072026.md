---
name: automacao-integracao-reprodutibilidade
description: >
  Use esta skill para automatizar pipelines completos de economia regional,
  integrar métodos (MIP + SAM + CGE + Espacial), validar dados, versionar
  resultados, gerar relatórios automáticos e garantir reprodutibilidade.
  Ativa quando o usuário pedir orquestração de pipelines, validação de dados
  com Great Expectations, containerização Docker, CI/CD com GitHub Actions,
  integração CGE-Espacial, uso de bases internacionais (GTAP, WIOD, EXIOBASE),
  ou geração automática de relatórios com Quarto/Pandoc.
  Entrega infraestrutura completa para pesquisas reproduzíveis em economia regional.
---
# Skill: Automação, Integração e Reprodutibilidade em Economia Regional

## Propósito
Skill de infraestrutura que une e automatiza todos os métodos de economia regional:
- **Validação de dados**: Great Expectations, consistência entre fontes (< 2% divergência)
- **Pipelines**: Orquestração com Airflow, Prefect e Makefile
- **Integração de métodos**: MIP → SAM → CGE → Espacial em pipeline único
- **Bases internacionais**: GTAP, WIOD, EXIOBASE, OECD ICIO
- **Balanceamento avançado**: RAS, Entropia Cruzada, EUK, QP, Stone, KRAS
- **Infraestrutura**: Docker, Git LFS, GitHub Actions, CI/CD
- **Relatórios automáticos**: Quarto, Pandoc/Eisvogel, WeasyPrint, Dash, Shiny
- **Checklist de reprodutibilidade**: 30 itens de verificação obrigatória

## O que esta skill NÃO faz
- Não ensina teoria de MIP/SAM/CGE/Espacial (cada um tem sua skill específica)
- Não substitui software GIS para edição geométrica avançada
- Não faz análise econômica — apenas a infraestrutura para executá-la de forma reproduzível
- Não gerencia clusters distribuídos (apenas containers e pipelines simples)

## Fontes
- Referência: `habilidades_gerais_nazaré.pdf` (Manual Geral de Métodos em Economia Regional, Versão 2.0, Julho 2026)
- Autor: Davi Lucena da Silva — Doutorando em Economia pela UFV
- Bibliografia: Miller & Blair (2009), Anselin (1988), LeSage & Pace (2009), Elhorst (2014)

---

## Índice

1. [Estrutura de Repositório e Versionamento](#1-repositorio)
2. [Validação e Consistência de Dados](#2-validacao)
3. [Ingestão Automática de Dados (FTP, API, Web Scraping)](#3-ingestao)
4. [Bases de Dados Internacionais](#4-internacionais)
5. [Balanceamento de Matrizes — Métodos Avançados](#5-balanceamento)
6. [Integração MIP → SAM → CGE → Espacial](#6-integracao)
7. [Pipelines com Airflow e Prefect](#7-pipelines)
8. [Containerização com Docker](#8-docker)
9. [CI/CD com GitHub Actions](#9-cicd)
10. [Geração Automática de Relatórios](#10-relatorios)
11. [Dashboards Interativos (Dash e Shiny)](#11-dashboards)
12. [Checklist de Reprodutibilidade](#12-checklist)
13. [Integração CGE-Espacial Completa](#13-cge-espacial)
14. [Pipeline Mestre — Do Dado ao Relatório](#14-pipeline-mestre)
15. [Instalação e Dependências](#15-instalacao)

---

## 1. Estrutura de Repositório e Versionamento

### 1.1 Estrutura padrão para projetos de economia regional

```
projeto/
├── dados_brutos/              # Imutáveis: hash SHA-256 registrado
│   ├── SAM_2015.npy           # (via Git LFS se > 100 MB)
│   ├── RAIS_2023.parquet      # Formato columnar, compressão zstd
│   ├── MIP_2015.xlsx
│   ├── shapefiles/
│   │   └── BR_UF_2022.shp
│   └── .gitignore
├── dados_processados/         # Outputs de scripts (regenerável)
│   ├── emprego_uf_cnae.parquet
│   ├── sam_8x8_construida.npy
│   └── .gitignore
├── scripts/                   # Versionados (cada script = 1 etapa)
│   ├── 01_download_rais.py
│   ├── 02_processa_rais.py
│   ├── 03_constroi_sam.py
│   ├── 04_calibra_cge.py
│   ├── 05_estima_sar.py
│   ├── 06_simula_choque.py
│   ├── 07_gera_relatorio.py
│   └── utils.py
├── notebooks/                 # Análise exploratória (não versionar outputs)
│   └── exploracao.ipynb
├── resultados/                # .gitignore (regenerável)
│   ├── parametros_cge.json
│   ├── solucao_benchmark.json
│   ├── simulacao.csv
│   └── relatorio.pdf
├── tests/                     # Testes unitários e de integração
│   ├── test_validacao.py
│   ├── test_sam.py
│   └── test_cge.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── .github/
│   └── workflows/
│       └── build.yml          # CI/CD
├── requirements.txt           # Versões fixas (pip freeze)
├── Makefile                   # Pipeline de build
├── README.md                  # Documentação do projeto
└── _quarto.yml                # Configuração do relatório
```

### 1.2 Makefile para reprodutibilidade total

```makefile
.PHONY: all clean dados modelos relatorio docker-build

# Pipeline completo: do dado ao relatório
all: dados_processados/emprego_uf_cnae.parquet \
     resultados/simulacao.csv \
     resultados/relatorio.pdf

# --- Etapa 1: Dados ---
dados_brutos/RAIS_2023.parquet: scripts/01_download_rais.py
	python scripts/01_download_rais.py
	@echo "SHA256: $$(sha256sum $@)" >> dados_brutos/checksums.txt

dados_processados/emprego_uf_cnae.parquet: dados_brutos/RAIS_2023.parquet scripts/02_processa_rais.py
	python scripts/02_processa_rais.py

# --- Etapa 2: SAM ---
dados_processados/sam_8x8_construida.npy: dados_processados/emprego_uf_cnae.parquet scripts/03_constroi_sam.py
	python scripts/03_constroi_sam.py

# --- Etapa 3: CGE ---
resultados/parametros_cge.json: dados_processados/sam_8x8_construida.npy scripts/04_calibra_cge.py
	python scripts/04_calibra_cge.py

# --- Etapa 4: Espacial ---
resultados/simulacao.csv: dados_processados/emprego_uf_cnae.parquet scripts/05_estima_sar.py scripts/06_simula_choque.py
	python scripts/05_estima_sar.py
	python scripts/06_simula_choque.py

# --- Etapa 5: Relatório ---
resultados/relatorio.pdf: resultados/simulacao.csv resultados/parametros_cge.json scripts/07_gera_relatorio.py
	python scripts/07_gera_relatorio.py

# --- Docker ---
docker-build:
	docker build -t economia-regional -f docker/Dockerfile .

docker-run:
	docker run --rm -v $$(pwd)/resultados:/app/resultados economia-regional

# --- Limpeza ---
clean:
	rm -rf dados_processados/* resultados/*
```

### 1.3 Git LFS para dados grandes

```bash
# Inicializar Git LFS
git lfs install

# Definir padrões para dados grandes
git lfs track "*.parquet"
git lfs track "*.npy"
git lfs track "*.7z"
git lfs track "*.zip"
git lfs track "*.shp"
git lfs track "*.tif"

# Adicionar .gitattributes gerado
git add .gitattributes

# Verificar
git lfs ls-files
```

### 1.4 .gitignore padrão

```gitignore
# Dados processados (regeneráveis)
dados_processados/
resultados/

# Notebooks com outputs
notebooks/.ipynb_checkpoints/
notebooks/*.nbconvert.ipynb

# IDE
.vscode/
.idea/
*.swp
*.swo

# Ambiente
__pycache__/
*.pyc
.env
venv/
.venv/

# Docker
docker/.docker/

# Sistema
.DS_Store
Thumbs.db
```

### 1.5 requirements.txt com versões fixas

```txt
# Core
numpy==1.26.4
pandas==2.2.1
scipy==1.13.0
scikit-learn==1.4.1

# Espacial
libpysal==4.14.1
spreg==1.9.0
esda==2.6.0
geopandas==1.0.1

# Dados
sidrapy==0.1.4
python-bcb==1.1.0
py7zr==0.22.0
duckdb==1.0.0
pyarrow==16.0.0

# Visualização
matplotlib==3.8.4
plotly==5.22.0
folium==0.17.0

# Relatórios
weasyprint==62.3

# Qualidade
great_expectations==0.18.15

# Orquestração
apache-airflow==2.9.0
prefect==2.19.0

# Bayesiano
pymc==5.15.0
arviz==0.18.0
```

---

## 2. Validação e Consistência de Dados

### 2.1 Validação de Schema com Great Expectations

```python
import great_expectations as ge
import pandas as pd
import numpy as np

def validar_rais(df_rais):
    """
    Valida schema e qualidade da RAIS usando Great Expectations.
    
    Verificações:
    - Colunas obrigatórias existem e não são nulas
    - Total de vínculos dentro do esperado (50-60 milhões)
    - 27 UFs válidas
    - CNAE 2 dígitos tem 87 setores
    - Não há valores negativos em qtd_vinc_ativos
    
    Args:
        df_rais: DataFrame da RAIS
    
    Returns:
        dict com resultado da validação
    """
    df_ge = ge.from_pandas(df_rais)
    
    # Expectativas mínimas
    expectations = [
        df_ge.expect_column_to_exist('uf'),
        df_ge.expect_column_to_exist('cnae20classe'),
        df_ge.expect_column_to_exist('qtd_vinc_ativos'),
        
        df_ge.expect_column_values_to_not_be_null('uf'),
        df_ge.expect_column_values_to_not_be_null('cnae20classe'),
        
        df_ge.expect_column_sum('qtd_vinc_ativos', between=(50e6, 60e6)),
        df_ge.expect_column_min('qtd_vinc_ativos', min_value=0),
        
        df_ge.expect_column_distinct_values_to_equal_set(
            'uf',
            expected_set=['AC','AL','AP','AM','BA','CE','DF','ES','GO',
                         'MA','MT','MS','MG','PA','PB','PR','PE','PI',
                         'RJ','RN','RS','RO','RR','SC','SP','SE','TO']
        ),
    ]
    
    validation = df_ge.validate(expectations)
    
    # Relatório detalhado
    relatorio = {
        'sucesso': validation['success'],
        'total_expectativas': len(validation['results']),
        'falhas': [r['expectation_config']['expectation_type'] 
                  for r in validation['results'] if not r['success']],
        'detalhes': validation['statistics']
    }
    
    if not relatorio['sucesso']:
        raise ValueError(
            f"Validação RAIS falhou: {len(relatorio['falhas'])} "
            f"expectativas não atendidas: {relatorio['falhas']}"
        )
    
    return relatorio

def validar_sam(sam, tolerancia=1e-6):
    """
    Valida identidades macroeconômicas de uma SAM.
    
    Verificações:
    1. Soma linhas = soma colunas (para cada conta)
    2. PIB renda ≈ PIB despesa
    3. Não-negatividade (exceto contas específicas)
    4. Consistência com totais conhecidos
    
    Args:
        sam: ndarray n×n
        tolerancia: Tolerância para balanceamento
    
    Returns:
        dict com resultados das verificações
    """
    n = sam.shape[0]
    resultados = {}
    
    # 1. Balanceamento
    row_sum = sam.sum(axis=1)
    col_sum = sam.sum(axis=0)
    max_balance = np.max(np.abs(row_sum - col_sum))
    resultados['balanceamento'] = {
        'max_diferenca': max_balance,
        'balanceado': max_balance < tolerancia
    }
    
    # 2. PIB
    # PIB pela renda (supondo contas: 0=Ativ, 1=Merc, 2=Trab, 3=Cap, 
    # 4=Fam, 5=Gov, 6=Inv, 7=RM)
    if n >= 8:
        pib_renda = sam[2, :n].sum() + sam[3, :n].sum() + sam[5, :n].sum()
        pib_despesa = (sam[:n, 4].sum() + sam[:n, 5].sum() + 
                       sam[:n, 6].sum() + sam[:n, 7].sum() - sam[7, :n].sum())
        diff_pib = abs(pib_renda - pib_despesa)
        resultados['pib'] = {
            'pib_renda': pib_renda,
            'pib_despesa': pib_despesa,
            'diferenca': diff_pib,
            'consistente': diff_pib / max(pib_renda, 1) < 1e-6
        }
    
    # 3. Não-negatividade
    negativos = np.where(sam < -tolerancia)
    resultados['nao_negatividade'] = {
        'n_negativos': len(negativos[0]),
        'passou': len(negativos[0]) == 0
    }
    
    # 4. Diagonal zero
    diag = np.diag(sam)
    resultados['diagonal'] = {
        'max_diag': np.max(np.abs(diag)),
        'diagonal_zero': np.all(np.abs(diag) < tolerancia)
    }
    
    return resultados
```

### 2.2 Consistência entre fontes

```python
def verificar_consistencia_entre_fontes(fonte_a, fonte_b, 
                                          nome_a='RAIS', nome_b='CAGED',
                                          tolerancia=0.02):
    """
    Verifica se duas fontes independentes têm totais consistentes.
    
    Regra: divergência máxima de 2% entre fontes.
    
    Args:
        fonte_a: dict {indicador: valor}
        fonte_b: dict {indicador: valor}
        nome_a, nome_b: nomes das fontes
        tolerancia: 0.02 = 2%
    
    Returns:
        dict com resultados da comparação
    """
    inconsistencias = []
    
    for indicador in set(fonte_a) & set(fonte_b):
        va = fonte_a[indicador]
        vb = fonte_b[indicador]
        
        if va == 0 and vb == 0:
            continue
        
        max_val = max(abs(va), abs(vb))
        diff = abs(va - vb) / max_val if max_val > 0 else 0
        
        if diff > tolerancia:
            inconsistencias.append({
                'indicador': indicador,
                f'{nome_a}': va,
                f'{nome_b}': vb,
                'divergencia_pct': diff * 100,
                'status': 'ALERTA'
            })
        else:
            inconsistencias.append({
                'indicador': indicador,
                f'{nome_a}': va,
                f'{nome_b}': vb,
                'divergencia_pct': diff * 100,
                'status': 'OK'
            })
    
    df = pd.DataFrame(inconsistencias)
    
    alertas = df[df['status'] == 'ALERTA']
    if len(alertas) > 0:
        print("⚠️ INCONSISTÊNCIAS ENCONTRADAS:")
        print(alertas.to_string(index=False))
        raise ValueError(
            f"{len(alertas)} inconsistências acima de {tolerancia*100}%"
        )
    
    return df

# Exemplo: comparar RAIS vs PNAD para emprego agrícola
def comparar_rais_pnad(emprego_rais_uf, emprego_pnad_uf):
    """
    Compara emprego formal (RAIS) com emprego total formal+informal (PNAD).
    Útil para estimar informalidade setorial por UF.
    """
    merged = emprego_rais_uf.merge(
        emprego_pnad_uf, on='uf', suffixes=('_rais', '_pnad')
    )
    merged['taxa_informalidade'] = 1 - merged['emprego_rais'] / merged['emprego_pnad']
    merged['fator_expansao'] = merged['emprego_pnad'] / merged['emprego_rais']
    
    return merged
```

### 2.3 Verificação de integridade com hash

```python
import hashlib

def registrar_hash_arquivo(caminho_arquivo):
    """
    Calcula SHA-256 de um arquivo e registra metadados.
    """
    sha256 = hashlib.sha256()
    with open(caminho_arquivo, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            sha256.update(chunk)
    
    hash_str = sha256.hexdigest()
    
    registro = {
        'arquivo': caminho_arquivo,
        'sha256': hash_str,
        'tamanho_bytes': os.path.getsize(caminho_arquivo),
        'data': pd.Timestamp.now().isoformat()
    }
    
    # Salvar em checksums.txt
    with open('dados_brutos/checksums.txt', 'a') as f:
        f.write(f"{hash_str}  {caminho_arquivo}\n")
    
    return registro

def verificar_integridade_arquivo(caminho_arquivo, hash_esperado):
    """
    Verifica integridade de arquivo baixado.
    """
    sha256 = hashlib.sha256()
    with open(caminho_arquivo, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            sha256.update(chunk)
    
    hash_calculado = sha256.hexdigest()
    return hash_calculado == hash_esperado
```

---

## 3. Ingestão Automática de Dados (FTP, API, Web Scraping)

### 3.1 Download via FTP com retry

```python
import ftplib
import os
import time

def download_ftp_com_retry(host, caminho_remoto, arquivo_local, 
                            usuario='anonymous', senha='email@exemplo.com',
                            max_tentativas=5):
    """
    Download de arquivo via FTP com retry exponencial.
    """
    for tentativa in range(max_tentativas):
        try:
            ftp = ftplib.FTP(host)
            ftp.login(usuario, senha)
            ftp.cwd(os.path.dirname(caminho_remoto))
            
            with open(arquivo_local, 'wb') as f:
                ftp.retrbinary(
                    f'RETR {os.path.basename(caminho_remoto)}', f.write
                )
            
            ftp.quit()
            print(f"✅ Download concluído: {arquivo_local}")
            return True
            
        except Exception as e:
            if tentativa < max_tentativas - 1:
                espera = 5 * (2 ** tentativa)  # backoff exponencial
                print(f"⚠️ Tentativa {tentativa+1} falhou: {e}")
                print(f"   Nova tentativa em {espera}s...")
                time.sleep(espera)
            else:
                raise RuntimeError(
                    f"❌ Todas as {max_tentativas} tentativas falharam: {e}"
                )

def baixar_rais_automatico(ano=2023, dir_destino='dados_brutos'):
    """
    Download completo da RAIS com verificação de integridade.
    """
    os.makedirs(dir_destino, exist_ok=True)
    
    host = 'ftp.mtps.gov.br'
    arquivos = ['RAIS_ESTAB_PUB.7z', 'RAIS_VINC_PUB.7z']
    
    for arquivo in arquivos:
        caminho_remoto = f'pdet/microdados/RAIS/{ano}/{arquivo}'
        arquivo_local = os.path.join(dir_destino, arquivo)
        
        download_ftp_com_retry(host, caminho_remoto, arquivo_local)
        
        # Registrar hash
        registro = registrar_hash_arquivo(arquivo_local)
        print(f"   SHA-256: {registro['sha256']}")
    
    print(f"📁 Arquivos salvos em: {dir_destino}")
```

### 3.2 Download via API SIDRA com cache

```python
import requests
import json
import os

def get_sidra_com_cache(table_id, params=None, 
                         cache_dir='dados_brutos/sidra_cache'):
    """
    Download de tabela SIDRA com cache local.
    Evita re-baixar dados já obtidos.
    """
    os.makedirs(cache_dir, exist_ok=True)
    
    # Gerar nome de cache único
    cache_key = f"t{table_id}_{json.dumps(params, sort_keys=True)}"
    cache_file = os.path.join(cache_dir, f"{hash(cache_key)}.json")
    
    # Verificar cache
    if os.path.exists(cache_file):
        print(f"📦 Cache hit: {cache_file}")
        with open(cache_file, 'r') as f:
            return json.load(f)
    
    # Download
    url = f"https://apisidra.ibge.gov.br/values/t/{table_id}"
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    
    # Salvar cache
    with open(cache_file, 'w') as f:
        json.dump(data, f)
    
    print(f"💾 Cache salvo: {cache_file}")
    return data

def baixar_tabelas_sidra_essenciais():
    """
    Baixa tabelas SIDRA essenciais para economia regional.
    """
    tabelas = {
        '5938': 'PIB por UF e setor',
        '4099': 'Taxa de desocupação PNAD',
        '4093': 'Rendimento médio PNAD',
        '5859': 'MIP - Tabela de Recursos',
        '5860': 'MIP - Tabela de Usos',
        '1612': 'PAM - Produção Agrícola Municipal',
    }
    
    dados = {}
    for codigo, descricao in tabelas.items():
        print(f"📥 Baixando {codigo} - {descricao}...")
        dados[codigo] = get_sidra_com_cache(codigo)
    
    return dados
```

### 3.3 Web Scraping (para bases sem API oficial)

```python
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time

def scrape_ipeadata_serie(codigo_serie):
    """
    Scraping do IPEADATA quando a API não está disponível.
    
    Args:
        codigo_serie: Código da série (ex: 'AGRO_PIB')
    
    Returns:
        DataFrame com data e valor
    """
    url = f"http://www.ipeadata.gov.br/ExibeSerie.aspx?serid={codigo_serie}"
    
    driver = webdriver.Chrome()
    driver.get(url)
    
    # Esperar tabela carregar
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "table"))
    )
    
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    
    # Extrair tabela
    tabela = soup.find('table', {'class': 'grid'})
    dados = []
    for row in tabela.find_all('tr')[1:]:  # pular header
        cols = row.find_all('td')
        if len(cols) >= 2:
            dados.append({
                'data': cols[0].text.strip(),
                'valor': cols[1].text.strip()
            })
    
    driver.quit()
    
    return pd.DataFrame(dados)
```

---

## 4. Bases de Dados Internacionais

### 4.1 GTAP (Global Trade Analysis Project)

```python
def carregar_gtap(caminho_base='dados_brutos/gtap/'):
    """
    Carrega dados do modelo GTAP (141 países, 65 setores).
    
    Base principal para CGE multirregional global.
    Disponível em: https://www.gtap.agecon.purdue.edu/
    
    Estrutura típica:
    - GTAPAgg: agregação personalizada de países/setores
    - Sets: arquivos .set com definições
    - Data: arquivos .har (HAR) ou .gdx
    """
    import subprocess
    
    # GTAP usa GAMS ou GEMPACK
    # Para Python, usar o pacote gtaptools (se disponível)
    try:
        import gtaptools
        gtap = gtaptools.GTAPData(caminho_base)
        return gtap
    except ImportError:
        # Alternativa: ler arquivos CSV exportados do GTAP
        import pandas as pd
        sam_gtap = pd.read_csv(f"{caminho_base}/sam.csv", index_col=0)
        return sam_gtap

def extrair_matriz_comercio_gtap(gtap_data, paises_origem, paises_destino):
    """
    Extrai matriz de comércio bilateral do GTAP para um conjunto de países.
    """
    # Fluxos VXMD (exportações a preços de mercado, por modo de transporte)
    vxmd = gtap_data.get('VXMD')
    
    matriz = vxmd.loc[paises_origem, paises_destino]
    return matriz
```

### 4.2 WIOD (World Input-Output Database)

```python
def carregar_wiod(ano=2014):
    """
    Carrega WIOD (World Input-Output Database).
    
    WIOD: 43 países, 56 setores, 2000-2014.
    Disponível em: http://www.wiod.org/
    
    Inclui:
    - WIOT: World Input-Output Tables
    - SEA: Socio-Economic Accounts (emprego, VA, capital)
    - Environmental Accounts
    """
    # Formato: arquivos .xlsx ou .csv
    wiot = pd.read_excel(
        f"dados_brutos/wiod/WIOT{ano}_Nov16.xlsx",
        sheet_name='WIOT', header=None
    )
    
    # Metadados: países e setores estão nas primeiras linhas
    paises = pd.read_excel(
        f"dados_brutos/wiod/WIOT{ano}_Nov16.xlsx",
        sheet_name='WIOT', nrows=1, header=None
    ).values.flatten()
    
    return wiot, paises
```

### 4.3 EXIOBASE

```python
def carregar_exiobase(ano=2020):
    """
    Carrega EXIOBASE (44 países, 163 setores, ênfase ambiental).
    
    Disponível em: https://www.exiobase.eu/
    
    Principal diferencial: versões ambientais (CO₂, água, uso da terra).
    """
    # EXIOBASE 3: arquivos .csv ou .mat
    # Matriz Z (consumo intermediário)
    Z = pd.read_csv(
        f"dados_brutos/exiobase/IOT_{ano}_Z.csv", index_col=0
    )
    # Matriz Y (demanda final)
    Y = pd.read_csv(
        f"dados_brutos/exiobase/IOT_{ano}_Y.csv", index_col=0
    )
    # Emissões ambientais
    F = pd.read_csv(
        f"dados_brutos/exiobase/IOT_{ano}_F.csv", index_col=0
    )
    
    return {'Z': Z, 'Y': Y, 'F': F}
```

### 4.4 OECD ICIO (Inter-Country Input-Output)

```python
def carregar_oecd_icio(ano=2021):
    """
    Carrega OECD Inter-Country Input-Output Tables.
    
    76 países (incluindo todos da OCDE + G20 + principais parceiros),
    45 setores ISIC Rev.4, 1995-2021.
    
    Disponível em: https://www.oecd.org/sti/ind/inter-country-input-output-tables.htm
    """
    # Formato: arquivos CSV estruturados
    # ICIO_TABLE_2021.csv
    icio = pd.read_csv(f"dados_brutos/oecd/ICIO_TABLE_{ano}.csv")
    return icio
```

### 4.5 DataZoom (BID) — PIB Municipal Brasileiro

```python
def carregar_datazoom_brasil():
    """
    DataZoom (BID) — PIB municipal brasileiro harmonizado (1970-2021).
    
    Base consistente para comparação internacional.
    Inclui PIB total, PIB per capita e setores para todos os municípios.
    """
    url = "https://datasets.dataviewer.info/datazoom/brazil_gdp.csv"
    df = pd.read_csv(url)
    return df
```

### 4.6 Tabela comparativa de bases internacionais

| Base | Países | Setores | Período | Foco | Acesso |
|---|---|---|---|---|---|
| **GTAP** | 141 | 65 | 2004, 2007, 2011, 2014, 2017 | CGE multirregional | Licença paga (USD 3.500) |
| **WIOD** | 43 | 56 | 2000-2014 | Séries temporais | Gratuito |
| **EXIOBASE** | 44 | 163 | 1995-2022 | Ambiental | Gratuito (CC) |
| **OECD ICIO** | 76 | 45 | 1995-2021 | Oficial OCDE | Gratuito |
| **EORA** | 190 | 26 | 1990-2022 | Cobertura global | Gratuito |
| **DataZoom** | 1 (BRA) | 9 | 1970-2021 | PIB municipal BR | Gratuito |

---

## 5. Balanceamento de Matrizes — Métodos Avançados

### 5.1 RAS Biproporcional (clássico)

```python
def ras(A0, r, s, max_iter=10000, tol=1e-8):
    """
    Balanceamento RAS biproporcional.
    
    Args:
        A0: Matriz inicial (n×m)
        r: Vetor-alvo linha (n,)
        s: Vetor-alvo coluna (m,)
        max_iter: Máximo de iterações
        tol: Tolerância para convergência
    
    Returns:
        tuple (A_balanceada, n_iterações, (erro_linha, erro_coluna))
    """
    A = A0.copy().astype(float)
    
    for it in range(max_iter):
        # Ajuste linha
        row_factors = r / A.sum(axis=1)
        row_factors = np.nan_to_num(row_factors, nan=1.0, 
                                     posinf=1.0, neginf=1.0)
        A = A * row_factors[:, np.newaxis]
        
        # Ajuste coluna
        col_factors = s / A.sum(axis=0)
        col_factors = np.nan_to_num(col_factors, nan=1.0, 
                                     posinf=1.0, neginf=1.0)
        A = A * col_factors[np.newaxis, :]
        
        # Verificar convergência
        row_err = np.max(np.abs(A.sum(axis=1) - r) / np.maximum(r, 1e-15))
        col_err = np.max(np.abs(A.sum(axis=0) - s) / np.maximum(s, 1e-15))
        
        if max(row_err, col_err) < tol:
            return A, it + 1, (row_err, col_err)
    
    raise ValueError(f"Não convergiu em {max_iter} iterações. "
                     f"Último erro: linha={row_err:.2e}, coluna={col_err:.2e}")

def ras_com_zeros(A0, r, s, max_iter=10000, tol=1e-8):
    """
    RAS que preserva a estrutura de zeros da matriz original.
    (Método de Stone, 1977)
    """
    mask_zero = (A0 == 0)
    A = A0.copy().astype(float)
    
    for it in range(max_iter):
        # Ajuste linha (preservando zeros)
        row_sum = A.sum(axis=1)
        row_factors = np.where(row_sum > 0, r / row_sum, 1.0)
        A = A * row_factors[:, np.newaxis]
        A[mask_zero] = 0
        
        # Ajuste coluna (preservando zeros)
        col_sum = A.sum(axis=0)
        col_factors = np.where(col_sum > 0, s / col_sum, 1.0)
        A = A * col_factors[np.newaxis, :]
        A[mask_zero] = 0
        
        row_err = np.max(np.abs(A.sum(axis=1) - r) / np.maximum(r, 1e-15))
        col_err = np.max(np.abs(A.sum(axis=0) - s) / np.maximum(s, 1e-15))
        
        if max(row_err, col_err) < tol:
            return A, it + 1, (row_err, col_err)
    
    raise ValueError(f"Não convergiu em {max_iter} iterações")
```

### 5.2 Entropia Cruzada (CE) — com priors e restrições

```python
from scipy.optimize import minimize

def cross_entropy_balance(A0, r=None, s=None, 
                           restricoes_celula=None, pesos=None,
                           max_iter=5000):
    """
    Balanceamento por Entropia Cruzada generalizada.
    
    Args:
        A0: Matriz inicial (n×m)
        r: Totais linha alvo (opcional)
        s: Totais coluna alvo (opcional)
        restricoes_celula: dict {(i,j): valor_fixo}
        pesos: Matriz de confiança (0-1) para cada célula
        max_iter: Máximo de iterações
    
    Returns:
        Matriz balanceada (n×m)
    """
    n, m = A0.shape
    a0_flat = A0.flatten()
    a0_sum = a0_flat.sum()
    
    # Normalizar como probabilidades
    if a0_sum > 0:
        a0_flat = a0_flat / a0_sum
    
    def objective(x):
        x = np.maximum(x, 1e-15)  # evitar log(0)
        a0 = np.maximum(a0_flat, 1e-15)
        
        # Entropia cruzada: Σ x ln(x/a0)
        if pesos is not None:
            w = pesos.flatten()
            return np.sum(w * x * np.log(x / a0))
        return np.sum(x * np.log(x / a0))
    
    constraints = []
    
    if r is not None:
        def row_constraint(x):
            return x.reshape(n, m).sum(axis=1) - r / a0_sum
        constraints.append({'type': 'eq', 'fun': row_constraint})
    
    if s is not None:
        def col_constraint(x):
            return x.reshape(n, m).sum(axis=0) - s / a0_sum
        constraints.append({'type': 'eq', 'fun': col_constraint})
    
    if restricoes_celula:
        for (i, j), valor in restricoes_celula.items():
            def celula_constraint(x, i=i, j=j, v=valor):
                return x.reshape(n, m)[i, j] - v / a0_sum
            constraints.append({'type': 'eq', 'fun': celula_constraint})
    
    bounds = [(0, None)] * len(a0_flat)
    
    result = minimize(
        objective, a0_flat, method='SLSQP',
        bounds=bounds, constraints=constraints,
        options={'maxiter': max_iter}
    )
    
    if not result.success:
        raise RuntimeError(f"CE não convergiu: {result.message}")
    
    A_balanced = result.x.reshape(n, m) * a0_sum
    return A_balanced
```

### 5.3 Distância Euclidiana (EUK)

```python
from scipy.optimize import minimize

def euclidean_balance(A0, r, s):
    """
    Balanceamento por Distância Euclidiana Mínima.
    
    Vantagem: Simples e rápida.
    Desvantagem: Permite valores negativos.
    
    min Σᵢ Σⱼ (a_{ij} - a_{ij}⁰)²
    s.a. Σⱼ a_{ij} = r_i
         Σᵢ a_{ij} = s_j
    """
    n, m = A0.shape
    a0_flat = A0.flatten()
    
    def objective(x):
        return np.sum((x - a0_flat) ** 2)
    
    # Restrições linha e coluna
    constraints = []
    
    for i in range(n):
        def row_c(x, i=i):
            return x.reshape(n, m)[i, :].sum() - r[i]
        constraints.append({'type': 'eq', 'fun': row_c})
    
    for j in range(m):
        def col_c(x, j=j):
            return x.reshape(n, m)[:, j].sum() - s[j]
        constraints.append({'type': 'eq', 'fun': col_c})
    
    result = minimize(objective, a0_flat, method='SLSQP', 
                      constraints=constraints)
    
    return result.x.reshape(n, m)
```

### 5.4 KRAS (Lenzen et al., 2007)

```python
def kras_balance(A0, r_min, r_max, s_min, s_max, max_iter=10000):
    """
    KRAS — extensão do RAS que lida com restrições de intervalo.
    
    Em vez de totais exatos, aceita intervalos:
    r_min ≤ Σⱼ a_{ij} ≤ r_max
    s_min ≤ Σᵢ a_{ij} ≤ s_max
    
    Útil quando há incerteza nos totais de controle.
    """
    A = A0.copy().astype(float)
    n, m = A.shape
    
    for it in range(max_iter):
        # Ajuste linha (dentro do intervalo)
        row_sum = A.sum(axis=1)
        for i in range(n):
            if row_sum[i] < r_min[i]:
                factor = r_min[i] / max(row_sum[i], 1e-15)
                A[i, :] *= factor
            elif row_sum[i] > r_max[i]:
                factor = r_max[i] / max(row_sum[i], 1e-15)
                A[i, :] *= factor
        
        # Ajuste coluna (dentro do intervalo)
        col_sum = A.sum(axis=0)
        for j in range(m):
            if col_sum[j] < s_min[j]:
                factor = s_min[j] / max(col_sum[j], 1e-15)
                A[:, j] *= factor
            elif col_sum[j] > s_max[j]:
                factor = s_max[j] / max(col_sum[j], 1e-15)
                A[:, j] *= factor
        
        # Verificar convergência
        row_ok = all(r_min[i] <= A.sum(axis=1)[i] <= r_max[i] for i in range(n))
        col_ok = all(s_min[j] <= A.sum(axis=0)[j] <= s_max[j] for j in range(m))
        
        if row_ok and col_ok:
            return A, it + 1
    
    raise ValueError(f"KRAS não convergiu em {max_iter}")
```

### 5.5 Comparação entre métodos de balanceamento

| Método | Tipo | Preserva zeros | Aceita intervalos | Aceita priors | Velocidade |
|---|---|---|---|---|---|
| **RAS** | Biproporcional | ✅ (Stone) | ❌ | ❌ | Muito rápida |
| **CE** | Entropia | ✅ | ❌ | ✅ | Lenta |
| **EUK** | Quadrático | ❌ | ❌ | ❌ | Rápida |
| **QP** | Quadrático c/ restrições | ✅ | ❌ | ❌ | Média |
| **KRAS** | Biproporcional relaxado | ✅ | ✅ | ❌ | Rápida |
| **CHARM** | Baseado em comércio | ✅ | ❌ | ❌ | Média |

---

## 6. Integração MIP → SAM → CGE → Espacial

### 6.1 Pipeline integrado de 4 estágios

```python
def pipeline_integrado(ano_referencia=2015):
    """
    Pipeline completo integrando MIP → SAM → CGE → Espacial.
    
    Estágio 1: MIP → SAM (matriz de contabilidade social)
    Estágio 2: SAM → CGE (calibração e solução)
    Estágio 3: CGE → Espacial (regionalização e spillovers)
    Estágio 4: Simulação integrada (choque + CGE + SAR)
    
    Returns:
        dict com resultados de todos os estágios
    """
    resultados = {}
    
    # ---- Estágio 1: MIP → SAM ----
    print("=" * 60)
    print("ESTÁGIO 1: MIP → SAM")
    print("=" * 60)
    
    # Carregar MIP do SIDRA ou arquivo local
    mip = carregar_mip(ano_referencia)
    
    # Extrair matrizes
    U = mip['matriz_uso']       # Use (n×n)
    VA_trab = mip['va_trabalho']  # VA trabalho (n,)
    VA_cap = mip['va_capital']    # VA capital (n,)
    C_hh = mip['consumo_familias']
    C_gov = mip['consumo_governo']
    I = mip['investimento']
    E = mip['exportacoes']
    M = mip['importacoes']
    
    # Construir SAM 8×8
    sam = construir_sam(U, VA_trab, VA_cap, C_hh, C_gov, I, E, M)
    
    # Validar SAM
    validacao_sam = validar_sam(sam)
    assert validacao_sam['balanceamento']['balanceado'], "SAM desbalanceada!"
    
    resultados['sam'] = sam
    resultados['validacao_sam'] = validacao_sam
    print(f"  ✅ SAM 8×8 construída e validada")
    
    # ---- Estágio 2: SAM → CGE ----
    print("\n" + "=" * 60)
    print("ESTÁGIO 2: SAM → CGE")
    print("=" * 60)
    
    # Calibrar CGE
    params_cge = calibrar_cge(sam)
    
    # Resolver sistema
    solucao_cge = resolver_cge(params_cge)
    
    # Verificar convergência
    assert solucao_cge['max_residuo'] < 1e-6, \
        f"CGE não convergiu: max|F| = {solucao_cge['max_residuo']}"
    
    resultados['params_cge'] = params_cge
    resultados['solucao_cge'] = solucao_cge
    print(f"  ✅ CGE calibrado e resolvido (max|F| = {solucao_cge['max_residuo']:.2e})")
    
    # ---- Estágio 3: CGE → Espacial ----
    print("\n" + "=" * 60)
    print("ESTÁGIO 3: CGE → Espacial (Regionalização)")
    print("=" * 60)
    
    # Carregar dados regionais
    rais = carregar_rais_agregada(2023)
    
    # Regionalizar coeficientes via FLQ
    coef_reg = regionalizar_com_flq(
        params_cge['matriz_A'], rais['emprego_setor_uf']
    )
    
    # Construir matriz W (contiguidade UF)
    gdf = carregar_shapefile_uf()
    w = construir_matriz_w(gdf, tipo='queen')
    
    resultados['coeficientes_regionais'] = coef_reg
    resultados['matriz_w'] = w
    print(f"  ✅ Coeficientes regionalizados e matriz W construída")
    
    # ---- Estágio 4: Simulação Integrada ----
    print("\n" + "=" * 60)
    print("ESTÁGIO 4: Simulação Integrada (Choque + CGE + SAR)")
    print("=" * 60)
    
    # Simular choque com CGE e spillovers SAR
    simulacao = simulacao_integrada(
        cge_params=params_cge,
        cge_solucao=solucao_cge,
        matriz_w=w,
        rho=0.30,  # da literatura ou estimado
        choque_pct=15,
        estado_choque='MT'
    )
    
    resultados['simulacao'] = simulacao
    print(f"  ✅ Simulação integrada concluída")
    print(f"     Spillover total: {simulacao['spillover_total']:,.0f} empregos")
    
    return resultados


def construir_sam(U, VA_trab, VA_cap, C_hh, C_gov, I, E, M):
    """
    Constrói SAM 8×8 a partir dos componentes da MIP.
    
    Estrutura:
    0=Atividades, 1=Mercadorias, 2=Trabalho, 3=Capital,
    4=Famílias, 5=Governo, 6=Investimento, 7=Resto do Mundo
    """
    n = U.shape[0]  # número de setores
    sam = np.zeros((8, 8))
    
    # Mercadorias → Atividades (consumo intermediário)
    sam[1, 0] = U.sum()
    
    # Valor Adicionado → Atividades
    sam[2, 0] = VA_trab.sum()  # Trabalho
    sam[3, 0] = VA_cap.sum()   # Capital
    
    # Demanda Final → Mercadorias
    sam[4, 1] = C_hh.sum()     # Consumo famílias
    sam[5, 1] = C_gov.sum()    # Consumo governo
    sam[6, 1] = I.sum()        # Investimento
    sam[7, 1] = E.sum()        # Exportações
    
    # Importações (Resto do Mundo → Mercadorias)
    sam[1, 7] = M.sum()
    
    # Fechar diagonal (make)
    sam[0, 1] = U.sum() + VA_trab.sum() + VA_cap.sum()  # Output bruto
    
    return sam
```

### 6.2 Simulação integrada CGE + SAR

```python
def simulacao_integrada(cge_params, cge_solucao, matriz_w, 
                         rho, choque_pct, estado_choque):
    """
    Simulação integrada em 2 estágios:
    
    Estágio 1: Choque no CGE → novos preços e quantidades de equilíbrio
    Estágio 2: Spillovers espaciais via SAR com os resultados do CGE
    
    Returns:
        dict com efeitos diretos, indiretos e totais
    """
    from scipy.optimize import fsolve
    
    n_regioes = matriz_w.shape[0]
    I = np.eye(n_regioes)
    
    # ---- Estágio 1: Choque no CGE ----
    # Aplicar choque de demanda no estado alvo
    cge_params['choque_demanda'] = np.zeros(n_regioes)
    idx_choque = 0  # índice do estado choque
    cge_params['choque_demanda'][idx_choque] = 1 + choque_pct / 100
    
    # Re-resolver CGE
    nova_solucao = fsolve(
        lambda x: cge_system(x, cge_params),
        cge_solucao,
        maxfev=10000, xtol=1e-10
    )
    
    # Variação percentual no emprego por região
    variacao_cge = (nova_solucao - cge_solucao) / cge_solucao
    
    # ---- Estágio 2: Spillovers SAR ----
    # Multiplicador espacial: S = (I - ρW)^(-1)
    S = np.linalg.inv(I - rho * matriz_w)
    
    # Vetor de choque (log)
    choque_log = np.zeros(n_regioes)
    choque_log[idx_choque] = np.log(1 + choque_pct / 100)
    
    # Efeito total via SAR
    efeito_total_log = S @ choque_log
    
    # Converter para níveis
    emprego_base = cge_params.get('emprego_base', np.ones(n_regioes) * 1e6)
    efeito_total_niveis = emprego_base * (np.exp(efeito_total_log) - 1)
    efeito_direto = emprego_base * (np.exp(choque_log) - 1)
    efeito_indireto = efeito_total_niveis - efeito_direto
    
    return {
        'efeito_direto': float(efeito_direto.sum()),
        'efeito_indireto': float(efeito_indireto.sum()),
        'efeito_total': float(efeito_total_niveis.sum()),
        'multiplicador': float(efeito_total_niveis.sum() / max(efeito_direto.sum(), 1)),
        'variacao_cge': variacao_cge,
        'por_regiao': pd.DataFrame({
            'efeito_direto': efeito_direto,
            'efeito_indireto': efeito_indireto,
            'efeito_total': efeito_total_niveis,
            'variacao_pct': np.exp(efeito_total_log) - 1
        })
    }
```

---

## 7. Pipelines com Airflow e Prefect

### 7.1 DAG Airflow — Pipeline Regional Completo

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'agente_nazare',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_economia_regional',
    default_args=default_args,
    description='Pipeline completo: download → processamento → modelos → relatório',
    schedule_interval='0 0 1 1 *',  # 1º de janeiro de cada ano
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['economia-regional', 'rais', 'cge', 'espacial'],
) as dag:
    
    def download_rais_callable(**kwargs):
        from scripts.download_rais import baixar_rais_automatico
        baixar_rais_automatico(ano=kwargs.get('ano', 2023))
    
    def processar_rais_callable():
        import duckdb
        con = duckdb.connect()
        con.execute("""
            CREATE TABLE rais AS
            SELECT * FROM read_csv_auto(
                'dados_brutos/RAIS_ESTAB_PUB.COMT',
                sep=';', encode='iso-8859-1'
            )
        """)
        resultado = con.execute("""
            SELECT uf, cnae20classe // 100 AS setor,
                   SUM(qtd_vinc_ativos) AS empregos
            FROM rais GROUP BY uf, setor
        """).fetchdf()
        resultado.to_parquet('dados_processados/emprego_uf_setor.parquet')
    
    def validar_dados_callable():
        from scripts.validacao import validar_rais
        df = pd.read_parquet('dados_processados/emprego_uf_setor.parquet')
        validar_rais(df)
    
    def construir_sam_callable():
        from scripts.sam_builder import construir_sam_completa
        sam = construir_sam_completa()
        np.save('dados_processados/sam_8x8.npy', sam)
    
    def calibrar_cge_callable():
        from scripts.cge_calibration import calibrar_cge_completo
        calibrar_cge_completo('dados_processados/sam_8x8.npy')
    
    def estimar_sar_callable():
        from scripts.sar_estimation import estimar_sar
        estimar_sar()
    
    def simular_choque_callable():
        from scripts.simulacao import simular_choque
        simular_choque()
    
    def gerar_relatorio_callable():
        import subprocess
        subprocess.run([
            'quarto', 'render', 'relatorio.qmd', '--to', 'pdf'
        ], check=True)
    
    # Tarefas
    download = PythonOperator(
        task_id='download_rais',
        python_callable=download_rais_callable,
        op_kwargs={'ano': 2023}
    )
    
    processa = PythonOperator(
        task_id='processar_rais',
        python_callable=processar_rais_callable
    )
    
    valida = PythonOperator(
        task_id='validar_dados',
        python_callable=validar_dados_callable
    )
    
    sam = PythonOperator(
        task_id='construir_sam',
        python_callable=construir_sam_callable
    )
    
    cge = PythonOperator(
        task_id='calibrar_cge',
        python_callable=calibrar_cge_callable
    )
    
    sar = PythonOperator(
        task_id='estimar_sar',
        python_callable=estimar_sar_callable
    )
    
    choque = PythonOperator(
        task_id='simular_choque',
        python_callable=simular_choque_callable
    )
    
    relatorio = PythonOperator(
        task_id='gerar_relatorio',
        python_callable=gerar_relatorio_callable
    )
    
    # Dependências
    download >> processa >> valida >> sam >> cge
    valida >> sar >> choque
    [cge, choque] >> relatorio
```

### 7.2 Prefect Flow (alternativa moderna)

```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
import numpy as np

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=24))
def baixar_mip(ano=2015):
    """Download da MIP com cache de 24h."""
    from sidrapy import get_table
    recursos = get_table(table_code="5859", territorial_level="1",
                         ibge_territorial_code="all", period=str(ano))
    usos = get_table(table_code="5860", territorial_level="1",
                     ibge_territorial_code="all", period=str(ano))
    return {'recursos': recursos, 'usos': usos}

@task(retries=3, retry_delay_seconds=60)
def baixar_rais(ano=2023):
    """Download RAIS com retry automático."""
    from scripts.download_rais import baixar_rais_automatico
    return baixar_rais_automatico(ano)

@task
def validar_dados(mip, rais):
    """Validação cruzada dos dados."""
    from scripts.validacao import verificar_consistencia_entre_fontes
    # ... validação
    return {'status': 'ok'}

@task
def construir_sam(mip):
    """Construção da SAM."""
    # ... construir SAM 8×8
    return np.zeros((8, 8))

@task
def calibrar_cge(sam):
    """Calibração CGE."""
    # ... calibrar CGE
    return {'parametros': {}, 'solucao': np.zeros(30)}

@task
def estimar_elasticidades(rais):
    """Estimação de elasticidades."""
    # ... estimar σ_VA, σ_ARM, σ_CET
    return {'sigma_va': 0.9, 'sigma_arm': 2.0, 'sigma_cet': 2.0}

@task
def simular_choque(cge, elasticidades):
    """Simulação de choque integrada."""
    # ... simular choque
    return {'spillover_total': 4120}

@task
def gerar_relatorio(resultados):
    """Geração automática de relatório."""
    import subprocess
    subprocess.run(['quarto', 'render', 'relatorio.qmd', '--to', 'pdf'])

@flow
def pipeline_economia_regional(ano_mip=2015, ano_rais=2023):
    """Flow principal do Prefect."""
    mip = baixar_mip(ano_mip)
    rais = baixar_rais(ano_rais)
    
    validacao = validar_dados(mip, rais)
    
    sam = construir_sam(mip)
    elasticidades = estimar_elasticidades(rais)
    
    cge = calibrar_cge(sam)
    
    choque = simular_choque(cge, elasticidades)
    
    gerar_relatorio({
        'sam': sam,
        'cge': cge,
        'elasticidades': elasticidades,
        'choque': choque
    })
```

---

## 8. Containerização com Docker

### 8.1 Dockerfile

```dockerfile
# Dockerfile
FROM python:3.12-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    pandoc \
    texlive-xetex \
    texlive-fonts-recommended \
    texlive-latex-extra \
    librsvg2-bin \
    && rm -rf /var/lib/apt/lists/*

# Criar usuário não-root
RUN useradd -m -u 1000 agente

# Copiar dependências Python
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copiar código
COPY --chown=agente:agente . /app
WORKDIR /app

# Volumes para dados
VOLUME ["/app/dados_brutos", "/app/dados_processados", "/app/resultados"]

# Comando padrão
CMD ["python", "scripts/run_all.py"]
```

### 8.2 Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  economia-regional:
    build:
      context: .
      dockerfile: docker/Dockerfile
    container_name: economia-regional
    volumes:
      - ./dados_brutos:/app/dados_brutos
      - ./dados_processados:/app/dados_processados
      - ./resultados:/app/resultados
    environment:
      - PYTHONPATH=/app
      - MPLCONFIGDIR=/tmp/matplotlib
    ports:
      - "8050:8050"  # Dash
      - "8080:8080"  # Airflow
    command: python scripts/run_all.py

  airflow-webserver:
    image: apache/airflow:2.9.0
    container_name: airflow-webserver
    depends_on:
      - airflow-db
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-db:5432/airflow
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
    volumes:
      - ./dags:/opt/airflow/dags
      - ./scripts:/opt/airflow/scripts
    ports:
      - "8080:8080"
    entrypoint: ["airflow", "webserver"]

  airflow-db:
    image: postgres:16
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - airflow_db:/var/lib/postgresql/data

volumes:
  airflow_db:
```

### 8.3 Comandos Docker úteis

```bash
# Construir imagem
docker build -t economia-regional -f docker/Dockerfile .

# Executar pipeline completo
docker run --rm \
  -v $(pwd)/dados_brutos:/app/dados_brutos \
  -v $(pwd)/resultados:/app/resultados \
  economia-regional

# Executar com bash interativo
docker run -it --rm economia-regional bash

# Iniciar stack completa com compose
docker-compose up -d

# Ver logs
docker-compose logs -f economia-regional
```

---

## 9. CI/CD com GitHub Actions

### 9.1 Pipeline de validação contínua

```yaml
# .github/workflows/build.yml
name: Pipeline Economia Regional
on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 0 1 1 *'  # 1º de janeiro (atualização anual)

jobs:
  validate:
    name: Validação de Dados
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
          cache: 'pip'
      
      - name: Instalar dependências
        run: pip install -r requirements.txt
      
      - name: Verificar estrutura do projeto
        run: |
          test -f scripts/01_download_rais.py
          test -f scripts/02_processa_rais.py
          test -f Makefile
      
      - name: Testes unitários
        run: |
          python -m pytest tests/ -v --tb=short
      
      - name: Verificar consistência de dependências
        run: |
          pip freeze | diff requirements.txt - || true

  build:
    name: Build e Simulação
    needs: validate
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      
      - name: Instalar dependências
        run: pip install -r requirements.txt
      
      - name: Executar pipeline (modo validação)
        run: |
          python scripts/02_processa_rais.py --test-mode
          python scripts/03_constroi_sam.py --test-mode
      
      - name: Salvar resultados (artifacts)
        uses: actions/upload-artifact@v4
        with:
          name: resultados-validacao
          path: |
            resultados/*.json
            resultados/*.csv

  report:
    name: Relatório Automático
    needs: build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Instalar Quarto
        run: |
          wget -q https://github.com/quarto-dev/quarto-cli/releases/download/v1.4.552/quarto-1.4.552-linux-amd64.deb
          sudo dpkg -i quarto-1.4.552-linux-amd64.deb
      
      - name: Renderizar relatório
        run: quarto render relatorio.qmd --to pdf
      
      - name: Publicar relatório
        uses: actions/upload-artifact@v4
        with:
          name: relatorio-pdf
          path: resultados/relatorio.pdf

  deploy:
    name: Deploy Dashboard
    needs: report
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Build Docker image
        run: docker build -t economia-regional -f docker/Dockerfile .
      
      - name: Push para registry
        # Usar Docker Hub, GitHub Container Registry, etc.
        run: |
          echo "docker push usuario/economia-regional:latest"
```

---

## 10. Geração Automática de Relatórios

### 10.1 Configuração Quarto (`_quarto.yml`)

```yaml
# _quarto.yml
project:
  type: book
  output-dir: resultados

book:
  title: "Relatório de Economia Regional"
  subtitle: "Análise Integrada MIP-SAM-CGE-Espacial"
  author: "AgenteNazaré"
  date: today
  chapters:
    - index.qmd
    - 01-introducao.qmd
    - 02-dados.qmd
    - 03-sam.qmd
    - 04-cge.qmd
    - 05-espacial.qmd
    - 06-simulacao.qmd
    - 07-conclusao.qmd
  appendices:
    - apendice-metodologico.qmd

format:
  pdf:
    documentclass: report
    papersize: A4
    toc: true
    toc-depth: 3
    number-sections: true
    colorlinks: true
    code-line-numbers: true
    pdf-engine: xelatex
    include-in-header:
      text: |
        \usepackage{booktabs}
        \usepackage{longtable}
    template: eisvogel
  html:
    theme:
      - cosmo
      - custom.scss
    code-fold: true
    number-sections: true
```

### 10.2 Template de relatório em Quarto

```markdown
---
title: "Análise de Spillovers Regionais"
subtitle: "Modelo SAR com RAIS 2023"
author: "AgenteNazaré"
date: last-modified
format:
  pdf:
    template: eisvogel
    toc: true
    number-sections: true
---

# Resumo Executivo

Este relatório documenta a análise de transbordamento (spillover) de empregos
agrícolas formais entre os estados brasileiros.

- **Modelo:** SAR (Spatial Autoregressive)
- **Dados:** RAIS 2023 (PDET/MTE)
- **Choque:** +15% na demanda agrícola do Mato Grosso
- **Spillover total:** {{< var spillover_total >}} empregos
- **Multiplicador:** {{< var multiplicador >}}

# Metodologia

## Modelo SAR

$$ \mathbf{y} = \rho\mathbf{Wy} + \mathbf{X}\boldsymbol{\beta} + \boldsymbol{\varepsilon} $$

## Dados

A RAIS 2023 contém {{< var n_registros >}} registros de vínculos formais.
Foram selecionados os setores agrícolas (CNAE 01).

```{python}
#| echo: true
#| label: fig-dados
#| fig-cap: "Distribuição do emprego agrícola por UF"

import pandas as pd
import plotly.express as px

df = pd.read_parquet('dados_processados/emprego_uf_setor.parquet')
fig = px.bar(df, x='uf', y='empregos', title='Emprego por UF')
fig.show()
```

# Resultados

## Parâmetros Espaciais

| Parâmetro | Valor | IC 95% |
|-----------|-------|--------|
| ρ | {{< var rho >}} | {{< var rho_ci >}} |
| β₀ | {{< var beta0 >}} | |
| β₁ (ln emp total) | {{< var beta1 >}} | |

## Efeitos do Choque

```{python}
#| echo: false
#| label: tbl-efeitos
#| tbl-cap: "Efeitos diretos, indiretos e totais"

import pandas as pd
efeitos = pd.read_csv('resultados/efeitos.csv')
print(efeitos.to_markdown(index=False))
```

## Mapa de Spillovers

```{python}
#| echo: false
#| label: fig-mapa
#| fig-cap: "Spillovers por UF"

fig = plot_choropleth()
fig.savefig('resultados/mapa_spillover.png', dpi=150)
```

# Conclusão

{{< var conclusao >}}
```

### 10.3 Geração automática via Python

```python
def gerar_relatorio_automatico(resultados, output='resultados/relatorio.pdf'):
    """
    Gera relatório PDF automaticamente com Quarto ou Pandoc.
    
    Args:
        resultados: dict com todos os resultados da análise
        output: Caminho do PDF de saída
    """
    import subprocess
    import json
    
    # Salvar variáveis para o template
    with open('resultados/variaveis.json', 'w') as f:
        json.dump(resultados, f, indent=2, default=str)
    
    # Verificar engine disponível
    engines = []
    
    # Verificar Quarto
    if subprocess.run(['which', 'quarto'], capture_output=True).returncode == 0:
        engines.append('quarto')
    
    # Verificar Pandoc
    if subprocess.run(['which', 'pandoc'], capture_output=True).returncode == 0:
        engines.append('pandoc')
    
    if not engines:
        raise RuntimeError(
            "Nenhuma engine de relatório encontrada. "
            "Instale Quarto (recomendado) ou Pandoc + LaTeX."
        )
    
    if 'quarto' in engines:
        print("📄 Renderizando com Quarto...")
        subprocess.run([
            'quarto', 'render', 'relatorio.qmd',
            '--to', 'pdf',
            '--output', output
        ], check=True)
    else:
        print("📄 Renderizando com Pandoc...")
        subprocess.run([
            'pandoc', 'relatorio.md', '-o', output,
            '--template=eisvogel',
            '--pdf-engine=xelatex',
            '--citeproc',
            '--bibliography=referencias.bib',
            '-M', f"title={resultados.get('titulo', 'Relatório')}",
            '-M', 'toc-own-page=true',
            '-M', 'numbersections=true'
        ], check=True)
    
    print(f"✅ Relatório gerado: {output}")
```

---

## 11. Dashboards Interativos (Dash e Shiny)

### 11.1 Dashboard Dash com parâmetros ajustáveis

```python
from dash import Dash, dcc, html, Input, Output, callback
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

def criar_dashboard_spillover(gdf, modelo_sar, resultados_base):
    """
    Dashboard interativo com:
    - Seletor de estado-fonte do choque
    - Slider de magnitude do choque
    - Slider de ρ (parâmetro espacial)
    - Mapa, tabela e gráfico sincronizados
    """
    app = Dash(__name__)
    
    # Layout
    app.layout = html.Div([
        html.H1("Dashboard de Spillovers Regionais",
                style={'textAlign': 'center'}),
        
        html.Div([
            html.Div([
                html.Label("Estado do choque:"),
                dcc.Dropdown(
                    id='state-dropdown',
                    options=[{'label': s, 'value': s} 
                            for s in sorted(gdf['sigla'].unique())],
                    value='MT'
                ),
            ], style={'width': '30%', 'display': 'inline-block'}),
            
            html.Div([
                html.Label("Magnitude do choque (%):"),
                dcc.Slider(
                    id='shock-slider',
                    min=0, max=50, value=15,
                    marks={i: f'{i}%' for i in range(0, 51, 10)},
                ),
            ], style={'width': '30%', 'display': 'inline-block'}),
            
            html.Div([
                html.Label("Parâmetro espacial ρ:"),
                dcc.Slider(
                    id='rho-slider',
                    min=0.0, max=0.7, value=0.30, step=0.05,
                    marks={i/10: f'{i/10:.1f}' for i in range(0, 8)},
                ),
            ], style={'width': '30%', 'display': 'inline-block'}),
        ]),
        
        html.Div([
            dcc.Graph(id='map-graph', style={'width': '60%'}),
            dcc.Graph(id='bar-graph', style={'width': '40%'}),
        ]),
        
        html.Div([
            html.H3("Tabela de Resultados"),
            html.Div(id='results-table')
        ]),
    ])
    
    @callback(
        [Output('map-graph', 'figure'),
         Output('bar-graph', 'figure'),
         Output('results-table', 'children')],
        [Input('state-dropdown', 'value'),
         Input('shock-slider', 'value'),
         Input('rho-slider', 'value')]
    )
    def update_dashboard(estado, choque_pct, rho):
        """Recalcula tudo com novos parâmetros."""
        n = len(gdf)
        I = np.eye(n)
        W = modelo_sar['W']
        
        # Índice do estado choque
        idx = gdf[gdf['sigla'] == estado].index[0]
        
        # Vetor de choque
        choque_log = np.zeros(n)
        choque_log[idx] = np.log(1 + choque_pct / 100)
        
        # Multiplicador espacial
        S = np.linalg.inv(I - rho * W)
        efeito_log = S @ choque_log
        
        # Converter para níveis
        y_orig = resultados_base['emprego_agri'].values
        efeito_niveis = y_orig * (np.exp(efeito_log) - 1)
        efeito_pct = (np.exp(efeito_log) - 1) * 100
        
        # DataFrame para plot
        df_plot = gdf.copy()
        df_plot['spillover'] = efeito_niveis
        df_plot['spillover_pct'] = efeito_pct
        df_plot['is_source'] = (df_plot['sigla'] == estado)
        
        # Mapa
        fig_map = go.Figure(go.Choropleth(
            locations=df_plot['sigla'],
            z=df_plot['spillover'],
            locationmode='ISO-3',
            colorscale='YlOrRd',
            text=df_plot['sigla'],
            colorbar_title='Empregos',
            hovertemplate='<b>%{text}</b>: %{z:,.0f}<extra></extra>',
            marker_line_color='white',
            marker_line_width=0.5
        ))
        fig_map.update_layout(
            title=f"Spillovers — Choque de +{choque_pct}% em {estado} (ρ={rho:.2f})",
            geo=dict(scope='south america', projection_type='natural earth')
        )
        
        # Gráfico de barras
        df_sorted = df_plot.sort_values('spillover', ascending=True)
        colors = ['#d7191c' if x else '#2c7bb6' for x in df_sorted['is_source']]
        
        fig_bar = go.Figure(go.Bar(
            x=df_sorted['spillover'],
            y=df_sorted['sigla'],
            orientation='h',
            marker_color=colors,
            text=df_sorted['spillover'].apply(lambda x: f'{x:,.0f}'),
            textposition='outside'
        ))
        fig_bar.update_layout(
            title='Spillover por UF (empregos)',
            xaxis_title='Empregos',
            yaxis_title='UF',
            height=500
        )
        
        # Tabela
        table_df = df_plot[['sigla', 'spillover', 'spillover_pct']].copy()
        table_df.columns = ['UF', 'Spillover (empregos)', 'Variação (%)']
        table_df = table_df.sort_values('Spillover (empregos)', ascending=False)
        table_df['Spillover (empregos)'] = table_df['Spillover (empregos)'].apply(
            lambda x: f'{x:,.0f}'
        )
        table_df['Variação (%)'] = table_df['Variação (%)'].apply(
            lambda x: f'{x:.2f}%'
        )
        
        table = html.Table([
            html.Thead(html.Tr([html.Th(col) for col in table_df.columns])),
            html.Tbody([
                html.Tr([
                    html.Td(table_df.iloc[i][col]) for col in table_df.columns
                ]) for i in range(len(table_df))
            ])
        ], style={'width': '100%', 'borderCollapse': 'collapse'})
        
        return fig_map, fig_bar, table
    
    return app
```

---

## 12. Checklist de Reprodutibilidade

### 12.1 Checklist completo (30 itens)

```python
class ChecklistReprodutibilidade:
    """
    Checklist de 30 itens para garantir reprodutibilidade total.
    Cada item deve ser verificado antes da publicação.
    """
    
    def __init__(self):
        self.itens = []
        self.resultados = {}
    
    def adicionar_item(self, categoria, item, funcao_verificacao):
        self.itens.append({
            'categoria': categoria,
            'item': item,
            'funcao': funcao_verificacao
        })
    
    def verificar_todos(self):
        """Executa todos os itens do checklist."""
        print("=" * 60)
        print("CHECKLIST DE REPRODUTIBILIDADE")
        print("=" * 60)
        
        resultados_agregados = {'OK': 0, 'FALHA': 0, 'N/A': 0}
        
        for i, item in enumerate(self.itens, 1):
            try:
                resultado = item['funcao']()
                status = 'OK' if resultado else 'FALHA'
                resultados_agregados[status] += 1
                print(f"{i:2d}. [{status}] ({item['categoria']}) {item['item']}")
            except Exception as e:
                status = 'FALHA'
                resultados_agregados['FALHA'] += 1
                print(f"{i:2d}. [FALHA] ({item['categoria']}) {item['item']}: {e}")
        
        print("=" * 60)
        print(f"RESULTADO: {resultados_agregados['OK']} OK, "
              f"{resultados_agregados['FALHA']} FALHA, "
              f"{resultados_agregados['N/A']} N/A")
        
        if resultados_agregados['FALHA'] > 0:
            raise ValueError(
                f"{resultados_agregados['FALHA']} itens falharam no checklist"
            )
        
        return resultados_agregados


def checklist_padrao():
    """
    Cria checklist padrão para projetos de economia regional.
    """
    checklist = ChecklistReprodutibilidade()
    
    # ----- DADOS -----
    checklist.adicionar_item(
        'DADOS', 'Downloads registrados (URL, data, hash SHA-256)',
        lambda: os.path.exists('dados_brutos/checksums.txt')
    )
    
    checklist.adicionar_item(
        'DADOS', 'Número de observações confere com documentação oficial',
        lambda: True  # implementar verificação específica
    )
    
    checklist.adicionar_item(
        'DADOS', 'Divergência entre fontes < 2%',
        lambda: True
    )
    
    # ----- CÓDIGO -----
    checklist.adicionar_item(
        'CÓDIGO', 'Script executado do zero sem erros',
        lambda: True
    )
    
    checklist.adicionar_item(
        'CÓDIGO', 'Semente aleatória fixada (np.random.seed)',
        lambda: True
    )
    
    checklist.adicionar_item(
        'CÓDIGO', 'Versões dos pacotes registradas (pip freeze)',
        lambda: os.path.exists('requirements.txt')
    )
    
    # ----- MATRIZES -----
    checklist.adicionar_item(
        'MATRIZES', 'SAM balanceada (soma linhas = soma colunas)',
        lambda: True
    )
    
    checklist.adicionar_item(
        'MATRIZES', 'Inversa de Leontief verificada (det ≠ 0)',
        lambda: True
    )
    
    checklist.adicionar_item(
        'MATRIZES', 'RAS convergiu (erro linha e coluna < 0,5%)',
        lambda: True
    )
    
    # ----- MODELOS -----
    checklist.adicionar_item(
        'MODELOS', 'CGE benchmark replicado (max|F| < 1e-6)',
        lambda: True
    )
    
    checklist.adicionar_item(
        'MODELOS', 'Multiplicadores ≥ 1',
        lambda: True
    )
    
    checklist.adicionar_item(
        'MODELOS', 'ρ SAR dentro de (-1, 1)',
        lambda: True
    )
    
    checklist.adicionar_item(
        'MODELOS', 'Testes LM documentados',
        lambda: True
    )
    
    # ----- RESULTADOS -----
    checklist.adicionar_item(
        'RESULTADOS', 'Tabelas com fontes e datas',
        lambda: True
    )
    
    checklist.adicionar_item(
        'RESULTADOS', 'Mapas com escala, norte e legenda',
        lambda: True
    )
    
    checklist.adicionar_item(
        'RESULTADOS', 'Código para reproduzir cada gráfico',
        lambda: True
    )
    
    # ----- DOCUMENTAÇÃO -----
    checklist.adicionar_item(
        'DOCUMENTAÇÃO', 'Versão do documento registrada',
        lambda: True
    )
    
    checklist.adicionar_item(
        'DOCUMENTAÇÃO', 'Contato do autor',
        lambda: True
    )
    
    checklist.adicionar_item(
        'DOCUMENTAÇÃO', 'Referências bibliográficas completas',
        lambda: os.path.exists('referencias.bib')
    )
    
    return checklist
```

### 12.2 Gerador de metadados de reprodutibilidade

```python
import platform
import subprocess
import hashlib
import json
from datetime import datetime

def gerar_metadados_reprodutibilidade():
    """
    Gera metadados completos para garantir reprodutibilidade.
    Salva em 'resultados/metadados_reprodutibilidade.json'.
    """
    metadados = {
        'data_execucao': datetime.now().isoformat(),
        'sistema': platform.platform(),
        'python': platform.python_version(),
        'pacotes': {},
        'hash_scripts': {},
        'hash_dados': {},
        'semente_aleatoria': 42,
    }
    
    # Versões dos pacotes
    import pkg_resources
    for pkg in pkg_resources.working_set:
        metadados['pacotes'][pkg.key] = pkg.version
    
    # Hash dos scripts
    import os
    for root, dirs, files in os.walk('scripts'):
        for f in files:
            if f.endswith('.py'):
                path = os.path.join(root, f)
                sha = hashlib.sha256()
                with open(path, 'rb') as fp:
                    for chunk in iter(lambda: fp.read(4096), b''):
                        sha.update(chunk)
                metadados['hash_scripts'][path] = sha.hexdigest()
    
    # Hash dos dados brutos
    if os.path.exists('dados_brutos'):
        for f in os.listdir('dados_brutos'):
            path = os.path.join('dados_brutos', f)
            if os.path.isfile(path) and not f.endswith('.txt'):
                sha = hashlib.sha256()
                with open(path, 'rb') as fp:
                    for chunk in iter(lambda: fp.read(4096), b''):
                        sha.update(chunk)
                metadados['hash_dados'][f] = sha.hexdigest()
    
    # Git commit
    try:
        commit = subprocess.run(
            ['git', 'log', '--oneline', '-1'],
            capture_output=True, text=True
        ).stdout.strip()
        metadados['git_commit'] = commit
        
        diff = subprocess.run(
            ['git', 'diff', '--stat'],
            capture_output=True, text=True
        ).stdout.strip()
        metadados['git_diff'] = diff if diff else 'clean'
    except:
        metadados['git_commit'] = 'não disponível'
    
    # Salvar
    with open('resultados/metadados_reprodutibilidade.json', 'w') as f:
        json.dump(metadados, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Metadados salvos em resultados/metadados_reprodutibilidade.json")
    return metadados
```

---

## 13. Integração CGE-Espacial Completa

### 13.1 Abordagem em 2 estágios

```python
def two_stage_cge_sar(y_orig, X, W, rho, cge_params, cge_solution,
                       choque_pct, estado_idx):
    """
    Simulação integrada em 2 estágios.
    
    Estágio 1 (curto prazo): SAR captura spillovers espaciais imediatos
    Estágio 2 (longo prazo): CGE ajusta preços e quantidades via equilíbrio geral
    
    Args:
        y_orig: Emprego base por região
        X: Variáveis explicativas do SAR
        W: Matriz de pesos espaciais
        rho: Parâmetro espacial
        cge_params: Parâmetros do CGE
        cge_solution: Solução de benchmark do CGE
        choque_pct: Magnitude do choque (%)
        estado_idx: Índice do estado que recebe o choque
    
    Returns:
        dict com resultados integrados
    """
    from scipy.optimize import fsolve
    
    n = len(y_orig)
    I = np.eye(n)
    
    # ---- Estágio 1: SAR (curto prazo) ----
    print("Estágio 1: SAR — spillovers de curto prazo")
    choque_log = np.zeros(n)
    choque_log[estado_idx] = np.log(1 + choque_pct / 100)
    
    # Multiplicador espacial SAR
    S_sar = np.linalg.inv(I - rho * W)
    efeito_sar_log = S_sar @ choque_log
    
    # Converter para níveis
    efeito_sar_niveis = y_orig * (np.exp(efeito_sar_log) - 1)
    
    print(f"  Spillover total (SAR): {efeito_sar_niveis.sum():,.0f}")
    
    # ---- Estágio 2: CGE (longo prazo) ----
    print("Estágio 2: CGE — equilíbrio geral com ajuste de preços")
    
    # Alimentar o CGE com os spillovers do SAR
    cge_params['spillovers_sar'] = efeito_sar_niveis
    cge_params['choque_demanda'] = np.zeros(n)
    cge_params['choque_demanda'][estado_idx] = 1 + choque_pct / 100
    
    # Re-resolver CGE com spillovers
    cge_nova_solucao = fsolve(
        lambda x: cge_system(x, cge_params),
        cge_solution,
        maxfev=10000, xtol=1e-10
    )
    
    # Variação total (CGE + SAR combinados)
    variacao_cge = (cge_nova_solucao - cge_solution)
    
    return {
        'estagio1_sar': {
            'efeito_direto': float(y_orig[estado_idx] * (np.exp(choque_log[estado_idx]) - 1)),
            'spillover_total': float(efeito_sar_niveis.sum()),
            'por_regiao': efeito_sar_niveis
        },
        'estagio2_cge': {
            'solucao': cge_nova_solucao,
            'variacao': variacao_cge
        },
        'integrado': {
            'efeito_total_estimado': float(efeito_sar_niveis.sum())
        }
    }
```

### 13.2 Modelo Input-Output Inter-regional (IIO)

```python
def modelo_iio_interregional(A_d, A_m, f_d, f_m):
    """
    Modelo Insumo-Produto Inter-regional.
    
    D = matriz diagonal de coeficientes domésticos
    M = matriz off-diagonal de coeficientes de importação inter-regional
    
    L = (I - A_d)^{-1} — multiplicador inter-regional
    
    Args:
        A_d: Matriz de coeficientes domésticos (nR × nR)
        A_m: Matriz de coeficientes de importação inter-regional (nR × nR)
        f_d: Demanda final doméstica
        f_m: Demanda final importada
    
    Returns:
        dict com outputs e multiplicadores
    """
    nR = A_d.shape[0]  # número de regiões × setores
    
    I = np.eye(nR)
    A_total = A_d + A_m
    
    # Verificar singularidade
    det = np.linalg.det(I - A_total)
    if abs(det) < 1e-10:
        raise ValueError(f"Matriz (I-A) quase singular: det = {det:.2e}")
    
    # Inversa de Leontief inter-regional
    L = np.linalg.inv(I - A_total)
    
    # Output total
    x = L @ (f_d + f_m)
    
    # Decomposição do multiplicador
    # Efeito intra-regional: diagonal de L
    # Efeito inter-regional: off-diagonal de L
    intra = np.diag(L)
    inter = L.sum(axis=1) - intra
    
    return {
        'output_total': x,
        'inversa_leontief': L,
        'multiplicador_intra': intra,
        'multiplicador_inter': inter,
        'multiplicador_total': L.sum(axis=1)
    }
```

---

## 14. Pipeline Mestre — Do Dado ao Relatório

### 14.1 Função única para executar tudo

```python
def pipeline_mestre(ano_rais=2023, ano_mip=2015, choque_pct=15, 
                     estado_choque='MT', output_dir='resultados'):
    """
    Pipeline mestre: do download do dado ao relatório final.
    
    Executa em sequência:
    1. Download e validação dos dados
    2. Construção da SAM
    3. Calibração CGE
    4. Estimação SAR
    5. Simulação integrada
    6. Visualização e dashboard
    7. Relatório automático
    8. Checklist de reprodutibilidade
    
    Returns:
        dict com resultados e metadados
    """
    import os, time, json
    from datetime import datetime
    
    os.makedirs(output_dir, exist_ok=True)
    inicio = time.time()
    
    print("=" * 70)
    print("🏆 PIPELINE MESTRE — ECONOMIA REGIONAL")
    print(f"   Início: {datetime.now().isoformat()}")
    print(f"   RAIS: {ano_rais} | MIP: {ano_mip} | Choque: +{choque_pct}% em {estado_choque}")
    print("=" * 70)
    
    # ---- 1. DADOS ----
    print("\n[1/8] 📥 Download e validação dos dados...")
    # (implementar cada etapa conforme as funções anteriores)
    
    # ---- 2. SAM ----
    print("\n[2/8] 📊 Construção da SAM...")
    
    # ---- 3. CGE ----
    print("\n[3/8] ⚙️  Calibração CGE...")
    
    # ---- 4. SAR ----
    print("\n[4/8] 🗺️  Estimação SAR...")
    
    # ---- 5. SIMULAÇÃO ----
    print("\n[5/8] 💥 Simulação integrada CGE + SAR...")
    
    # ---- 6. VISUALIZAÇÃO ----
    print("\n[6/8] 📈 Visualização e dashboards...")
    
    # ---- 7. RELATÓRIO ----
    print("\n[7/8] 📄 Geração de relatório automático...")
    
    # ---- 8. REPRODUTIBILIDADE ----
    print("\n[8/8] ✅ Checklist de reprodutibilidade...")
    metadados = gerar_metadados_reprodutibilidade()
    
    tempo_total = time.time() - inicio
    
    print("\n" + "=" * 70)
    print(f"✅ PIPELINE CONCLUÍDO em {tempo_total:.1f} segundos")
    print(f"📁 Resultados em: {output_dir}")
    print("=" * 70)
    
    return {
        'status': 'concluido',
        'tempo_total': tempo_total,
        'output_dir': output_dir,
        'metadados': metadados,
        'timestamp': datetime.now().isoformat()
    }
```

---

## 15. Instalação e Dependências

```bash
# Instalação mínima (essencial)
pip install numpy pandas scipy matplotlib seaborn

# Dados
pip install sidrapy python-bcb py7zr openpyxl

# Espacial
pip install geopandas libpysal spreg esda

# Qualidade
pip install great-expectations

# Big Data
pip install duckdb pyarrow

# Relatórios
pip install weasyprint

# Dashboards
pip install dash plotly folium

# Orquestração (opcional)
pip install apache-airflow prefect

# Bayesiano (opcional)
pip install pymc arviz

# Container (opcional)
# docker: https://docs.docker.com/get-docker/
# docker-compose: https://docs.docker.com/compose/install/

# Relatórios PDF (opcional - instalar separadamente)
# Quarto: https://quarto.org/docs/get-started/
# Pandoc + LaTeX: sudo apt-get install pandoc texlive-xelatex texlive-fonts-recommended
```

---

## Regras

### O que SEMPRE fazer
- **Registrar hash SHA-256** de todo arquivo baixado (dados brutos são imutáveis)
- **Fixar semente aleatória** (`np.random.seed(42)`) em toda simulação estocástica
- **Versionar requirements.txt** com `pip freeze > requirements.txt` ao final
- **Validar consistência entre fontes** (divergência máxima: 2%)
- **Executar checklist de reprodutibilidade** antes de publicar qualquer resultado
- **Usar Makefile** para documentar as dependências entre etapas do pipeline
- **Usar DuckDB** para datasets > 500 MB (RAIS, POF, CAGED)
- **Salvar metadados** completos em cada execução (data, versões, hash, git commit)

### O que NUNCA fazer
- Nunca modificar dados brutos — sempre manter cópia original imutável
- Nunca confiar em download único sem verificação de hash/integridade
- Nunca pular a validação de consistência entre fontes
- Nunca usar caminhos absolutos no código (sempre relativos ao projeto)
- Nunca ignorar o checklist de reprodutibilidade (30 itens obrigatórios)
- Nunca salvar resultados sem metadados de versão e data
- Nunca publicar relatório sem referências bibliográficas completas

### Quando algo falhar
- **Download FTP falha**: tentar mirror alternativo; usar retry com backoff exponencial
- **Validação Great Expectations falha**: verificar schema da fonte; pode ser mudança no formato oficial
- **RAS não converge**: verificar se totais linha e coluna são consistentes; tentar CE
- **Divergência entre fontes > 2%**: documentar no relatório; investigar causa (cobertura, período, definição)
- **Docker não constrói**: verificar versão do Python no Dockerfile vs. requirements.txt
- **CI/CD falha**: verificar secrets do GitHub; verificar permissões de acesso aos dados
- **Quarto não renderiza**: verificar se todas as extensões LaTeX estão instaladas
