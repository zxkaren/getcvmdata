import { renderMetricCard } from '../../components/metricCard';

export function renderOverviewPage(): string {
  return `
    <section class="page-intro" aria-labelledby="overview-title">
      <p class="eyebrow">PANORAMA EXECUTIVO</p>

      <h1 id="overview-title">
        Atividade do Mercado de Capitais
      </h1>

      <p class="page-description">
        Acompanhamento de ofertas públicas registradas
        nos dados disponibilizados pela CVM.
      </p>
    </section>

    <p id="load-status" role="status">
      Carregando indicadores executivos...
    </p>

    <section
      class="metrics-grid"
      aria-label="Indicadores executivos"
    >
      ${renderMetricCard({
        id: 'total-offers',
        label: 'Ofertas identificadas',
        description: 'Registros únicos da CVM SRE',
      })}

      ${renderMetricCard({
        id: 'total-volume',
        label: 'Volume financeiro informado',
        description: 'Não representa captação efetiva',
        detailId: 'total-volume-exact',
      })}

      ${renderMetricCard({
        id: 'distinct-issuers',
        label: 'Emissores distintos',
        description: 'Identificados por CNPJ',
      })}

      ${renderMetricCard({
        id: 'distinct-coordinators',
        label: 'Coordenadores líderes',
        description: 'Identificados por CNPJ',
      })}
    </section>

    <section class="charts-section" aria-labelledby="charts-title">
      <div class="charts-heading">
        <h2 id="charts-title">Distribuição e evolução das ofertas</h2>

        <p>
          Análises dos registros consolidados da CVM SRE.
          Passe o cursor sobre os gráficos para consultar os valores.
        </p>
      </div>

      <div class="charts-grid">
        <article class="chart-panel">
          <h3>Ofertas por instrumento</h3>
          <p class="chart-description">
            Quantidade de ofertas por tipo de valor mobiliário.
          </p>
          <div
            id="offers-by-instrument-chart"
            class="chart-container chart-container--long"
            role="img"
            aria-label="Gráfico de barras: quantidade de ofertas por instrumento financeiro"
          ></div>
        </article>

        <article class="chart-panel">
          <h3>Situação das ofertas</h3>
          <p class="chart-description">
            Distribuição dos registros por situação informada.
          </p>
          <div
            id="offers-by-status-chart"
            class="chart-container"
            role="img"
            aria-label="Gráfico de rosca: distribuição das ofertas por situação"
          ></div>
        </article>

        <article class="chart-panel">
          <h3>Volume por instrumento</h3>
          <p class="chart-description">
            Valores financeiros informados, apresentados em bilhões de reais.
          </p>
          <div
            id="volume-by-instrument-chart"
            class="chart-container chart-container--long"
            role="img"
            aria-label="Gráfico de barras: volume financeiro informado por instrumento"
          ></div>
        </article>

        <article class="chart-panel">
          <h3>Evolução das ofertas</h3>
          <p class="chart-description">
            Quantidade por data de referência. Somente datas com registros.
          </p>
          <div
            id="offers-by-date-chart"
            class="chart-container"
            role="img"
            aria-label="Gráfico de linha: quantidade de ofertas por data de referência"
          ></div>
        </article>
      </div>

    </section>

    <section
      class="ranking-section"
      aria-labelledby="ranking-title"
    >
      <div class="ranking-heading">
        <h2 id="ranking-title">
          Maiores ofertas identificadas
        </h2>

        <p id="ranking-description">
          Dez maiores valores financeiros informados
          entre as ofertas do conjunto publicado.
          A classificação não representa valores captados.
        </p>
      </div>

      <div class="ranking-panel">
        <div
          id="largest-offers-table"
          class="ranking-table-wrapper"
          role="region"
          aria-label="Tabela das maiores ofertas"
          tabindex="0"
        ></div>
      </div>
    </section>

    <section class="data-panel" aria-labelledby="source-title">

      <div class="panel-heading">
        <div>
          <p class="eyebrow">TRANSPARÊNCIA DOS DADOS</p>
          <h2 id="source-title">Origem e Atualização</h2>
        </div>

        <span class="panel-tag">CVM SRE</span>
      </div>

      <p id="reference-period"></p>
      <p id="last-collection"></p>

      <p class="source-disclaimer">
        Os indicadores representam exclusivamente os registros
        disponíveis no conjunto publicado. Não abrangem
        necessariamente todas as operações do mercado brasileiro.
        O volume financeiro informado não representa,
        obrigatoriamente, valores efetivamente captados.
      </p>
    </section>
  `;
}
