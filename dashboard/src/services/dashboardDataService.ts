const SCHEMA_VERSION = 1;
const OFFERS_FILE = 'offers.json';
const CVM_URL_BASE =
  'https://web.cvm.gov.br/sre-publico-cvm/#/oferta-publica/';

export interface DashboardOffer {
  offer_key: string;
  request_id: string;
  issuer: {
    name: string | null;
  };
  cvm_url: string;
}

export interface DashboardData {
  totalOffers: number;
  lastSourceExecutionAt: string;
  offers: DashboardOffer[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isDashboardOffer(value: unknown): value is DashboardOffer {
  if (!isRecord(value) || !isRecord(value.issuer)) {
    return false;
  }

  return (
    typeof value.request_id === 'string' &&
    /^\d+$/.test(value.request_id) &&
    value.offer_key === `cvm_sre:${value.request_id}` &&
    (
      typeof value.issuer.name === 'string' ||
      value.issuer.name === null
    ) &&
    value.cvm_url === `${CVM_URL_BASE}${value.request_id}`
  );
}

async function fetchJson(fileName: string): Promise<unknown> {
  const response = await fetch(`/${fileName}`, {
    cache: 'no-store',
  });

  if (!response.ok) {
    throw new Error(`Falha na leitura de ${fileName}: ${response.status}`);
  }

  return response.json() as Promise<unknown>;
}

export async function loadDashboardData(): Promise<DashboardData> {
  const manifest = await fetchJson('manifest.json');

  if (
    !isRecord(manifest) ||
    manifest.schema_version !== SCHEMA_VERSION ||
    !isRecord(manifest.datasets) ||
    manifest.datasets.offers !== OFFERS_FILE
  ) {
    throw new Error('Manifesto incompatível com o dashboard.');
  }

  const dataset = await fetchJson(OFFERS_FILE);

  if (
    !isRecord(dataset) ||
    !isRecord(dataset.metadata) ||
    !Array.isArray(dataset.offers)
  ) {
    throw new Error('Estrutura dos dados de ofertas inválida.');
  }

  const metadata = dataset.metadata;

  if (
    metadata.source !== 'cvm_sre' ||
    metadata.schema_version !== SCHEMA_VERSION ||
    typeof metadata.total_offers !== 'number' ||
    !Number.isInteger(metadata.total_offers) ||
    typeof metadata.last_source_execution_at !== 'string'
  ) {
    throw new Error('Metadados de ofertas inválidos.');
  }

  const offers: DashboardOffer[] = [];

  for (const offer of dataset.offers) {
    if (!isDashboardOffer(offer)) {
      throw new Error('Oferta incompatível com o contrato.');
    }

    offers.push(offer);
  }

  const offerKeys = new Set(offers.map((offer) => offer.offer_key));

  if (
    metadata.total_offers !== offers.length ||
    offerKeys.size !== offers.length
  ) {
    throw new Error('Quantidade ou unicidade das ofertas inconsistente.');
  }

  return {
    totalOffers: metadata.total_offers,
    lastSourceExecutionAt: metadata.last_source_execution_at,
    offers,
  };
}