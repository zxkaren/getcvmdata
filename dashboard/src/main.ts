import './style.css';

import { loadDashboardData } from './services/dashboardDataService';

const appElement = document.querySelector<HTMLDivElement>('#app');

if (!appElement) {
  throw new Error('Elemento principal da aplicação não encontrado.');
}

appElement.innerHTML = `
  <main class="dashboard-container">
    <header class="dashboard-header">
      <span class="eyebrow">GetCVMData</span>
      <h1>Inteligência de Mercado</h1>
      <p>Fundação do dashboard — CVM SRE</p>
    </header>

    <section class="data-panel" aria-label="Dados da coleta">
      <p id="load-status" role="status">Carregando dados processados...</p>

      <div class="metric">
        <span>Ofertas consolidadas</span>
        <strong id="total-offers">—</strong>
      </div>

      <p id="last-collection"></p>

      <a
        id="sample-offer-link"
        target="_blank"
        rel="noopener noreferrer"
        hidden
      ></a>
    </section>
  </main>
`;

async function initializeDashboard(): Promise<void> {
  const statusElement = document.querySelector<HTMLElement>('#load-status');
  const totalElement = document.querySelector<HTMLElement>('#total-offers');
  const dateElement = document.querySelector<HTMLElement>('#last-collection');
  const offerLink = document.querySelector<HTMLAnchorElement>(
    '#sample-offer-link'
  );

  if (!statusElement || !totalElement || !dateElement || !offerLink) {
    throw new Error('Elementos do dashboard não encontrados.');
  }

  try {
    const dashboardData = await loadDashboardData();

    totalElement.textContent = dashboardData.totalOffers.toLocaleString(
      'pt-BR'
    );

    dateElement.textContent =
      `Última coleta: ${dashboardData.lastSourceExecutionAt.replace('T', ' ')}` +
      ' (fuso não informado)';

    const sampleOffer = dashboardData.offers[0];

    if (sampleOffer) {
      offerLink.href = sampleOffer.cvm_url;
      offerLink.textContent =
        `Consultar oferta ${sampleOffer.request_id} na CVM`;
      offerLink.hidden = false;
    }

    statusElement.textContent = 'Dados carregados com sucesso.';
    statusElement.classList.add('success');
  } catch (error) {
    statusElement.textContent =
      error instanceof Error
        ? error.message
        : 'Não foi possível carregar os dados.';

    statusElement.classList.add('error');
  }
}

void initializeDashboard();