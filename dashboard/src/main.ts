import './style.css';
import './styles/overview.css';

import { renderAppShell } from './app/appShell';
import { initializeOverviewCharts } from './pages/overview/overviewCharts';
import { renderOverviewPage } from './pages/overview/overviewPage';
import { loadDashboardData } from './services/dashboardDataService';
import { formatBrlAmount } from './utils/formatCurrency';
import { renderLargestOffersTable } from './pages/overview/overviewRanking';

const appElement = document.querySelector<HTMLDivElement>('#app');

if (!appElement) {
  throw new Error('Elemento principal da aplicação não encontrado.');
}

appElement.innerHTML = renderAppShell(renderOverviewPage());

function getRequiredElement<T extends HTMLElement>(
  selector: string,
): T {
  const element = document.querySelector<T>(selector);

  if (!element) {
    throw new Error(`Elemento não encontrado: ${selector}`);
  }

  return element;
}

function formatIsoDate(value: string): string {
  const [year, month, day] = value.split('-');

  return `${day}/${month}/${year}`;
}

async function initializeDashboard(): Promise<void> {
  const statusElement = getRequiredElement<HTMLElement>(
    '#load-status'
  );

  const totalOffersElement = getRequiredElement<HTMLElement>(
    '#total-offers'
  );

  const totalVolumeElement = getRequiredElement<HTMLElement>(
    '#total-volume'
  );

  const exactVolumeElement = getRequiredElement<HTMLElement>(
    '#total-volume-exact'
  );

  const issuersElement = getRequiredElement<HTMLElement>(
    '#distinct-issuers'
  );

  const coordinatorsElement = getRequiredElement<HTMLElement>(
    '#distinct-coordinators'
  );

  const referencePeriodElement = getRequiredElement<HTMLElement>(
    '#reference-period'
  );

  const collectionDateElement = getRequiredElement<HTMLElement>(
    '#last-collection'
  );

  try {
    const dashboardData = await loadDashboardData();
    const overview = dashboardData.overview;

    totalOffersElement.textContent =
      overview.total_offers.toLocaleString('pt-BR');

    const formattedAmount = formatBrlAmount(
      overview.total_amount_brl
    );

    totalVolumeElement.textContent = formattedAmount.abbreviated;

    exactVolumeElement.textContent =
      `Total: ${formattedAmount.exact}`;

    issuersElement.textContent =
      overview.distinct_issuers.toLocaleString('pt-BR');

    coordinatorsElement.textContent =
      overview.distinct_lead_coordinators.toLocaleString('pt-BR');

    const { start, end } = overview.reference_date_range;

    referencePeriodElement.textContent =
      start && end
        ? `Datas de referência dos registros: ${formatIsoDate(start)} a ${formatIsoDate(end)}.`
        : 'Período de referência não disponível.';

    collectionDateElement.textContent =
      `Última coleta: ${dashboardData.lastSourceExecutionAt.replace('T', ' ')}` +
      ' (fuso não informado)';

    initializeOverviewCharts(overview);

    const rankingContainer = getRequiredElement<HTMLElement>(
  '#largest-offers-table'
);

rankingContainer.replaceChildren(
  renderLargestOffersTable(overview.largest_offers)
);

    if (overview.total_offers === 0) {
      statusElement.textContent =
        'Nenhuma oferta disponível neste conjunto de dados.';
    } else {
      statusElement.textContent = 'Dados carregados com sucesso.';
      statusElement.classList.add('success');
    }
  } catch (error) {
    statusElement.textContent =
      error instanceof Error
        ? error.message
        : 'Não foi possível carregar os indicadores.';

    statusElement.classList.add('error');
  }
}

void initializeDashboard();
