
export interface DashboardMetadata {
  source: string;
  schema_version: number;
  last_source_execution_at: string;
  total_offers: number;
  generation_id: string;
}

export interface DashboardOffer {
  offer_key: string;
  request_id: string;
  issuer: {
    name: string | null;
  };
  cvm_url: string;
}

export interface OverviewInstrument {
  security_type: string | null;
  offer_count: number;
  amount_brl: string;
}

export interface OverviewStatus {
  status: string | null;
  offer_count: number;
}

export interface OverviewDate {
  date: string;
  offer_count: number;
}

export interface RankedOffer {
  offer_key: string;
  request_id: string;
  issuer_name: string | null;
  security_type: string | null;
  amount_brl: string;
  cvm_url: string;
}

export interface DashboardOverview {
  total_offers: number;
  total_amount_brl: string;
  distinct_issuers: number;
  distinct_lead_coordinators: number;
  offers_without_issuer_cnpj: number;
  offers_without_coordinator_cnpj: number;
  reference_date_range: {
    start: string | null;
    end: string | null;
  };
  offers_by_security_type: OverviewInstrument[];
  offers_by_status: OverviewStatus[];
  offers_by_date: OverviewDate[];
  largest_offers: RankedOffer[];
}

export interface DashboardData {
  totalOffers: number;
  lastSourceExecutionAt: string;
  offers: DashboardOffer[];
  overview: DashboardOverview;
}
