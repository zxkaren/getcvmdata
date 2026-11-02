
import type {
  DashboardData,
  DashboardMetadata,
  DashboardOffer,
  DashboardOverview,
  OverviewDate,
  OverviewInstrument,
  OverviewStatus,
  RankedOffer,
} from '../types/dashboard';

const SCHEMA_VERSION = 1;
const SOURCE_NAME = 'cvm_sre';
const MAX_RANKED_OFFERS = 10;

const CVM_URL_BASE =
  'https://web.cvm.gov.br/sre-publico-cvm/#/oferta-publica/';

function isRecord(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value)
  );
}

function isNonNegativeInteger(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isSafeInteger(value) &&
    value >= 0
  );
}

function isOptionalText(value: unknown): value is string | null {
  return typeof value === 'string' || value === null;
}

function isDecimalString(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    /^\d+(?:\.\d+)?$/.test(value)
  );
}

function isIsoDate(value: unknown): value is string {
  if (
    typeof value !== 'string' ||
    !/^\d{4}-\d{2}-\d{2}$/.test(value)
  ) {
    return false;
  }

  const date = new Date(`${value}T00:00:00Z`);

  return (
    !Number.isNaN(date.getTime()) &&
    date.toISOString().slice(0, 10) === value
  );
}

function isDashboardMetadata(
  value: unknown,
): value is DashboardMetadata {
  return (
    isRecord(value) &&
    value.source === SOURCE_NAME &&
    value.schema_version === SCHEMA_VERSION &&
    typeof value.last_source_execution_at === 'string' &&
    isNonNegativeInteger(value.total_offers) &&
    typeof value.generation_id === 'string' &&
    /^[0-9a-f]{32}$/.test(value.generation_id)
  );
}

function isDashboardOffer(value: unknown): value is DashboardOffer {
  if (!isRecord(value) || !isRecord(value.issuer)) {
    return false;
  }

  return (
    typeof value.request_id === 'string' &&
    /^\d+$/.test(value.request_id) &&
    value.offer_key === `${SOURCE_NAME}:${value.request_id}` &&
    isOptionalText(value.issuer.name) &&
    value.cvm_url === `${CVM_URL_BASE}${value.request_id}`
  );
}

function isInstrument(value: unknown): value is OverviewInstrument {
  return (
    isRecord(value) &&
    isOptionalText(value.security_type) &&
    isNonNegativeInteger(value.offer_count) &&
    isDecimalString(value.amount_brl)
  );
}

function isStatus(value: unknown): value is OverviewStatus {
  return (
    isRecord(value) &&
    isOptionalText(value.status) &&
    isNonNegativeInteger(value.offer_count)
  );
}

function isDate(value: unknown): value is OverviewDate {
  return (
    isRecord(value) &&
    isIsoDate(value.date) &&
    isNonNegativeInteger(value.offer_count)
  );
}

function isRankedOffer(value: unknown): value is RankedOffer {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.request_id === 'string' &&
    /^\d+$/.test(value.request_id) &&
    value.offer_key === `${SOURCE_NAME}:${value.request_id}` &&
    isOptionalText(value.issuer_name) &&
    isOptionalText(value.security_type) &&
    isDecimalString(value.amount_brl) &&
    value.cvm_url === `${CVM_URL_BASE}${value.request_id}`
  );
}

function isDashboardOverview(
  value: unknown,
): value is DashboardOverview {
  if (
    !isRecord(value) ||
    !isRecord(value.reference_date_range)
  ) {
    return false;
  }

  const dateRange = value.reference_date_range;

  const hasValidDateRange =
    (dateRange.start === null || isIsoDate(dateRange.start)) &&
    (dateRange.end === null || isIsoDate(dateRange.end)) &&
    (dateRange.start === null) === (dateRange.end === null) &&
    (
      dateRange.start === null ||
      (
        typeof dateRange.end === 'string' &&
        dateRange.start <= dateRange.end
      )
    );

  return (
    isNonNegativeInteger(value.total_offers) &&
    isDecimalString(value.total_amount_brl) &&
    isNonNegativeInteger(value.distinct_issuers) &&
    isNonNegativeInteger(value.distinct_lead_coordinators) &&
    isNonNegativeInteger(value.offers_without_issuer_cnpj) &&
    isNonNegativeInteger(value.offers_without_coordinator_cnpj) &&
    hasValidDateRange &&
    Array.isArray(value.offers_by_security_type) &&
    value.offers_by_security_type.every(isInstrument) &&
    Array.isArray(value.offers_by_status) &&
    value.offers_by_status.every(isStatus) &&
    Array.isArray(value.offers_by_date) &&
    value.offers_by_date.every(isDate) &&
    Array.isArray(value.largest_offers) &&
    value.largest_offers.every(isRankedOffer)
  );
}

function sumOfferCounts(
  groups: Array<{ offer_count: number }>,
): number {
  return groups.reduce(
    (total, group) => total + group.offer_count,
    0,
  );
}

export function validateDashboardData(
  manifest: unknown,
  offersPayload: unknown,
  overviewPayload: unknown,
): DashboardData {
  if (
    !isRecord(manifest) ||
    manifest.schema_version !== SCHEMA_VERSION ||
    typeof manifest.generation_id !== 'string' ||
    !isRecord(manifest.datasets) ||
    manifest.datasets.offers !== 'offers.json' ||
    manifest.datasets.overview !== 'overview.json' ||
    !isRecord(manifest.sources) ||
    !isRecord(manifest.sources.cvm_sre)
  ) {
    throw new Error('Manifesto incompatível com o dashboard.');
  }

  const source = manifest.sources.cvm_sre;

  if (
    !isNonNegativeInteger(source.total_offers) ||
    typeof source.last_source_execution_at !== 'string' ||
    !isRecord(offersPayload) ||
    !isDashboardMetadata(offersPayload.metadata) ||
    !Array.isArray(offersPayload.offers) ||
    !offersPayload.offers.every(isDashboardOffer) ||
    !isRecord(overviewPayload) ||
    !isDashboardMetadata(overviewPayload.metadata) ||
    !isDashboardOverview(overviewPayload.overview)
  ) {
    throw new Error('Estrutura dos dados publicados inválida.');
  }

  const offers = offersPayload.offers;
  const offersMetadata = offersPayload.metadata;
  const overviewMetadata = overviewPayload.metadata;
  const overview = overviewPayload.overview;

  if (
    offersMetadata.generation_id !== manifest.generation_id ||
    overviewMetadata.generation_id !== manifest.generation_id ||
    offersMetadata.total_offers !== overviewMetadata.total_offers ||
    offersMetadata.last_source_execution_at !==
      overviewMetadata.last_source_execution_at ||
    offersMetadata.total_offers !== source.total_offers ||
    offersMetadata.last_source_execution_at !==
      source.last_source_execution_at
  ) {
    throw new Error(
      'Arquivos de gerações diferentes ou metadados inconsistentes.',
    );
  }

  const offerKeys = new Set(
    offers.map((offer) => offer.offer_key),
  );

  const rankedKeys = new Set(
    overview.largest_offers.map((offer) => offer.offer_key),
  );

  const totalOffers = offersMetadata.total_offers;

  if (
    totalOffers !== offers.length ||
    offerKeys.size !== offers.length ||
    overview.total_offers !== totalOffers ||
    sumOfferCounts(overview.offers_by_security_type) !== totalOffers ||
    sumOfferCounts(overview.offers_by_status) !== totalOffers ||
    sumOfferCounts(overview.offers_by_date) !== totalOffers ||
    overview.distinct_issuers > totalOffers ||
    overview.distinct_lead_coordinators > totalOffers ||
    overview.offers_without_issuer_cnpj > totalOffers ||
    overview.offers_without_coordinator_cnpj > totalOffers ||
    overview.largest_offers.length !==
      Math.min(MAX_RANKED_OFFERS, totalOffers) ||
    rankedKeys.size !== overview.largest_offers.length ||
    overview.largest_offers.some(
      (offer) => !offerKeys.has(offer.offer_key),
    ) ||
    (
      totalOffers === 0 &&
      overview.reference_date_range.start !== null
    ) ||
    (
      totalOffers > 0 &&
      overview.reference_date_range.start === null
    )
  ) {
    throw new Error(
      'Indicadores executivos inconsistentes com as ofertas.',
    );
  }

  return {
    totalOffers,
    lastSourceExecutionAt: offersMetadata.last_source_execution_at,
    offers,
    overview,
  };
}
