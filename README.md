# GetCVMData

O GetCVMData é um projeto Python para coleta automatizada de dados públicos da Comissão de Valores Mobiliários (CVM). Altamento útil para contadores, advogados e pessoas que atuam com mercado financeiro.

Atualmente, realiza consultas à CVM SRE, disponibilizando informações sobre ofertas públicas de valores mobiliários em arquivos JSON e CSV, preparados para exploração no Excel, Power BI e outras ferramentas de análise de dados.

O projeto utiliza uma arquitetura modular, preparada para incorporar novas fontes de informações empresariais e financeiras.

## Funcionalidades

- Coleta de dados por meio da API pública da CVM SRE.
- Paginação automática e validação dos registros recebidos.
- Período global de consulta configurável.
- Exportação em JSON, com metadados, e CSV, com estrutura tabular.
- Execução manual ou agendada com APScheduler.
- Testes automatizados com pytest.

## Instalação

**1. Clone o repositório:**

```bash
git clone https://github.com/zxkaren/getcvmdata.git
cd getcvmdata
```

**2. Crie e ative um ambiente virtual:**

```bash
python -m venv .venv
```

Windows (PowerShell):

```powershell
.venv\Scripts\Activate.ps1
```

Linux:

```bash
source .venv/bin/activate
```

**3. Instale o projeto e suas dependências:**

```bash
python -m pip install -r requirements.txt
python -m pip install -e .
```

**4. Configure o ambiente:**

Crie um arquivo `.env` na raiz do projeto, utilizando `.env.example` como referência.

Nele, é possível definir os parâmetros da API, o período de consulta, o diretório de armazenamento e a frequência de execução.

## Execução

**Execução automática:**

```bash
python main.py
```

A aplicação realiza uma coleta ao iniciar e permanece ativa para executar as próximas coletas conforme o agendamento configurado.

**Execução única, sem agendamento:**

```bash
python -c "from financial_data.application.orchestrator import run_data_pipelines; run_data_pipelines()"
```

**Testes automatizados:**

Para instalar o pytest e executar os testes:

```bash
python -m pip install -e ".[test]"
python -m pytest tests/ -v
```

## Resultados

Os arquivos gerados são armazenados em `data/raw/cvm_sre/`.

- **JSON:** registros completos e metadados da coleta.
- **CSV:** registros organizados em colunas para facilitar análises.

Cada execução gera arquivos identificados por data e horário, permitindo manter um histórico das extrações. Os arquivos de dados não são versionados no Git.

## Evolução

O GetCVMData está preparado para evoluir com novas fontes, como demonstrações financeiras ITR e DFP, e posteriormente permitir o cruzamento de informações empresariais e a construção de dashboards.

Essas integrações ainda não fazem parte da versão atual.

---

Desenvolvido por [zxkaren](https://github.com/zxkaren).