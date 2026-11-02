# GetCVMData — Contrato de Dados do Dashboard

## 1. Objetivo

Definir o contrato entre os pipelines Python, o processamento
dos dados e o dashboard web.

O dashboard consumirá exclusivamente os arquivos publicados
em data/processed/dashboard/v1/.

A arquitetura deverá permitir a inclusão futura de fontes
como CVM ITR e DFP.

## 2. Fluxo de dados

Extract:
- Coleta dos dados das APIs.
- Preservação dos arquivos originais em data/raw/.

Transform:
- Validação dos registros.
- Normalização de datas, CNPJs e valores financeiros.
- Identificação e tratamento de duplicidades.
- Consolidação dos registros.
- Identificação de alterações entre coletas.

Load:
- Geração dos arquivos processados.
- Preparação dos indicadores para o dashboard.
- Publicação dos arquivos JSON para consumo do frontend.

## 3. Estrutura de armazenamento

data/processed/
    cvm_sre/
        offers_current.json
        offers_history.json

    dashboard/
        v1/
            manifest.json
            overview.json
            offers.json
            participants.json

## 4. Responsabilidade dos arquivos

offers_current.json:
- Estado consolidado mais recente de cada oferta.
- Identificação única por fonte e idRequerimento.

offers_history.json:
- Histórico de alterações relevantes entre coletas.
- Preservação de mudanças de status e valores.

manifest.json:
- Versão do contrato.
- Data e horário da geração.
- Fontes disponíveis.
- Período de referência dos dados.
- Referências aos arquivos publicados.

overview.json:
- Indicadores executivos.
- Quantidade de ofertas.
- Valores financeiros agregados.
- Distribuição por instrumento, status e data.

offers.json:
- Registros normalizados para consulta.
- Informações necessárias para filtros e detalhamentos.
- Identificadores para acesso à fonte oficial.

participants.json:
- Emissores e coordenadores.
- Quantidade e volume de ofertas por participante.
- Relacionamentos identificados nos registros.

## 5. Contrato mínimo de uma oferta

{
    "source": "cvm_sre",
    "offer_key": "cvm_sre:28562",
    "request_id": "28562",
    "issuer": {
        "name": "YUMA CRED FUNDO DE INVESTIMENTO...",
        "cnpj": "62550985000186"
    },
    "lead_coordinator": {
        "name": "ID CORRETORA DE TITULOS...",
        "cnpj": "16695922000109"
    },
    "security_type": "Cotas de FIDC",
    "offer_type": "PRIMARIA",
    "status": "Registro Concedido",
    "amount_brl": "2697000.0400",
    "reference_date": "2026-10-30",
    "protocol_number": "3561/2026",
    "process_number": "SRE/3561/2026",
    "registration_number": "CVM/SRE/AUT/FDC/PRI/2026/1949",
    "cvm_url": "https://web.cvm.gov.br/sre-publico-cvm/#/oferta-publica/28562"
}

## 6. Regras de integridade

- Preservar os arquivos originais.
- Não duplicar ofertas entre execuções.
- Utilizar fonte + idRequerimento como identificação.
- Preservar alterações relevantes no histórico.
- Não duplicar valores financeiros em reprocessamentos.
- Preservar a precisão decimal dos valores monetários.
- Representar datas no padrão ISO 8601.
- Representar dados ausentes como null.
- Preservar a referência à origem dos dados.
- Publicar somente conjuntos completos e validados.
- Garantir que reprocessamentos sejam idempotentes.

## 7. Regras para o frontend

- Não acessar diretamente data/raw/.
- Não implementar regras de consolidação na interface.
- Consumir somente arquivos publicados para o dashboard.
- Respeitar a versão do contrato de dados.
- Exibir data de atualização e período consultado.
- Permitir acesso ao detalhamento oficial da CVM.
- Não interpretar valores ofertados como captação efetiva.

## 8. Expansão futura

Novas fontes deverão possuir processamento próprio.

O dashboard poderá incorporar dados de ITR e DFP
sem modificar os contratos existentes da CVM SRE.

Relacionamentos entre fontes dependerão de identificadores
válidos e correspondência comprovada.

Campos adicionais deverão preservar a compatibilidade
com as versões anteriores do contrato.