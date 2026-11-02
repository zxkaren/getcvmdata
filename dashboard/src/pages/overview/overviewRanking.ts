import type { RankedOffer } from '../../types/dashboard';
import { formatBrlAmount } from '../../utils/formatCurrency';

function appendTableCell(
  row: HTMLTableRowElement,
  value: string,
  className?: string,
): HTMLTableCellElement {
  const cell = row.insertCell();

  cell.textContent = value;

  if (className) {
    cell.className = className;
  }

  return cell;
}

export function renderLargestOffersTable(
  offers: RankedOffer[],
): HTMLElement {
  if (offers.length === 0) {
    const emptyMessage = document.createElement('p');

    emptyMessage.className = 'ranking-empty';
    emptyMessage.textContent =
      'Nenhuma oferta disponível para o ranking.';

    return emptyMessage;
  }

  const table = document.createElement('table');
  table.className = 'ranking-table';
  table.setAttribute('aria-describedby', 'ranking-description');

  const caption = table.createCaption();
  caption.className = 'visually-hidden';
  caption.textContent =
    'Ranking das maiores ofertas por valor financeiro informado';

  const headerRow = table.createTHead().insertRow();

  const headers = [
    'Posição',
    'Emissor / Requerimento',
    'Instrumento',
    'Volume informado',
  ];

  for (const headerText of headers) {
    const header = document.createElement('th');

    header.scope = 'col';
    header.textContent = headerText;

    headerRow.appendChild(header);
  }

  const tableBody = table.createTBody();

  offers.forEach((offer, index) => {
    const row = tableBody.insertRow();

    appendTableCell(
      row,
      String(index + 1).padStart(2, '0'),
      'ranking-position',
    );

    const issuerCell = row.insertCell();
    issuerCell.className = 'ranking-issuer';

    const issuerName = document.createElement('span');
    issuerName.className = 'ranking-issuer-name';
    issuerName.textContent = offer.issuer_name ?? 'Não informado';

    const requestId = document.createElement('small');
    requestId.className = 'ranking-request';
    requestId.textContent = `Requerimento ${offer.request_id}`;

    issuerCell.append(issuerName, requestId);

    appendTableCell(
      row,
      offer.security_type ?? 'Não informado',
      'ranking-instrument',
    );

    appendTableCell(
      row,
      formatBrlAmount(offer.amount_brl).exact,
      'ranking-amount',
    );
  });

  return table;
}
