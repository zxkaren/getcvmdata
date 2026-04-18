## CVM SRE Data Pipeline

Pipeline em Python para coleta automatizada de dados públicos da CVM, com processamento e geração de arquivos prontos para análise em ferramentas de BI. Útil para advogados e contadores na prospecção de clientes.

---

## Objetivo

Automatizar a ingestão e preparação de dados da CVM, garantindo:

- Consumo estruturado de API
- Atualização periódica dos dados
- Disponibilização em formatos analíticos (JSON e CSV)

---

## Funcionalidades

- Requisição HTTP POST para API pública
- Paginação automática dos dados
- Filtragem por período configurável (últimos N meses)
- Geração de arquivos:
  - JSON com metadados
  - CSV normalizado
- Agendamento automático via CRON

---

## Configuração

Arquivo `.env.` com base em `.env.example`:

```env
API_URL=https://web.cvm.gov.br/sre-publico-cvm/rest/sitePublico/pesquisar/detalhado

REQUEST_TIMEOUT=30
PAGE_SIZE=100
TARGET_DIRECTORY_NAME=results_cvmdata

LOOKBACK_MONTHS=3

CRON_EXPRESSION=* * * * *
```
## Execução
```
python -m venv .venv

.venv\Scripts\activate

pip install requests python-decouple python-dateutil apscheduler

python main.py
```
---

## Saída

Os arquivos são gerados automaticamente no diretório configurado:

- JSON: dados completos + metadados
- CSV: dados normalizados para análise 

## Integração

- Compatível com Power BI via importação direta de arquivos JSON ou CSV.

## Valor
- Automatiza coleta de dados públicos regulatórios
- Reduz esforço manual
- Estrutura dados para análise rápida e confiável

---
[developed by khashimoto]
