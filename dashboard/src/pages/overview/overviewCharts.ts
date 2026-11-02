import * as echarts from 'echarts/core';

import {
  BarChart,
  LineChart,
  PieChart,
} from 'echarts/charts';

import {
  AriaComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
} from 'echarts/components';

import { SVGRenderer } from 'echarts/renderers';

import type {
  DashboardOverview,
  OverviewInstrument,
} from '../../types/dashboard';

import { formatBrlAmount } from '../../utils/formatCurrency';

echarts.use([
  BarChart,
  LineChart,
  PieChart,
  AriaComponent,
  GridComponent,
  LegendComponent,
  TooltipComponent,
  SVGRenderer,
]);

const PRIMARY_COLOR = '#386d9b';
const SECONDARY_COLOR = '#6892b4';
const TEXT_COLOR = '#61738a';
const GRID_COLOR = '#e5eaf1';
const BILLION = 1_000_000_000;

const STATUS_COLORS = [
  PRIMARY_COLOR,
  SECONDARY_COLOR,
  '#adc3d6',
  '#d5dfe9',
];

const SECURITY_TYPE_LABELS: Record<string, string> = {
  'Certificados de Recebíveis Imobiliários': 'CRI',
  'Certificados de Recebíveis do Agronegócio': 'CRA',
  'Certificados de Recebíveis': 'Cert. de Recebíveis',
  'Cédula de Produto Rural Financeira': 'CPR Financeira',
  'Outros títulos de securitização': 'Outros títulos sec.',
  'Cotas de Fundos de Infra': 'Fundos de Infra',
};

function formatSecurityTypeLabel(
  securityType: string | null,
): string {
  if (securityType === null) {
    return 'Não informado';
  }

  return SECURITY_TYPE_LABELS[securityType] ?? securityType;
}

type ChartInstance = ReturnType<typeof echarts.init>;

function getChartElement(elementId: string): HTMLElement {
  const element = document.getElementById(elementId);

  if (!element) {
    throw new Error(`Contêiner do gráfico não encontrado: ${elementId}`);
  }

  return element;
}

function getTooltipIndex(params: unknown): number {
  const parameter = Array.isArray(params) ? params[0] : params;

  if (
    parameter !== null &&
    typeof parameter === 'object' &&
    'dataIndex' in parameter &&
    typeof parameter.dataIndex === 'number'
  ) {
    return parameter.dataIndex;
  }

  return -1;
}

function formatReferenceDate(value: string): string {
  const [year, month, day] = value.split('-');

  return `${day}/${month}/${year}`;
}

function createInstrumentBarChart(
  elementId: string,
  instruments: OverviewInstrument[],
  getValue: (instrument: OverviewInstrument) => number,
  formatValue: (instrument: OverviewInstrument) => string,
  axisName: string,
): ChartInstance {
  const chart = echarts.init(
    getChartElement(elementId),
    undefined,
    { renderer: 'svg' },
  );

  chart.setOption({
    color: [PRIMARY_COLOR],

    aria: {
      enabled: true,
    },

    grid: {
      left: 155,
      right: 28,
      top: 20,
      bottom: 42,
    },

    tooltip: {
      trigger: 'item',
      renderMode: 'richText',
      formatter: (params: unknown) => {
        const instrument = instruments[getTooltipIndex(params)];

        if (!instrument) {
          return '';
        }

        const instrumentName =
          instrument.security_type ?? 'Não informado';

        return `${instrumentName}\n${formatValue(instrument)}`;
      },
    },

    xAxis: {
      type: 'value',
      name: axisName,
      nameLocation: 'middle',
      nameGap: 28,
      minInterval: axisName === 'Ofertas' ? 1 : undefined,
      axisLabel: {
        color: TEXT_COLOR,
      },
      splitLine: {
        lineStyle: {
          color: GRID_COLOR,
        },
      },
    },

    yAxis: {
      type: 'category',
      inverse: true,
        data: instruments.map(
        (instrument) => formatSecurityTypeLabel(instrument.security_type),
        ),
      axisLabel: {
        color: TEXT_COLOR,
        width: 135,
        overflow: 'truncate',
        ellipsis: '…',
      },
      axisLine: {
        show: false,
      },
      axisTick: {
        show: false,
      },
    },

    series: [
      {
        type: 'bar',
        data: instruments.map(getValue),
        barMaxWidth: 18,
        itemStyle: {
          borderRadius: [0, 3, 3, 0],
        },
      },
    ],
  });

  return chart;
}

function createStatusChart(
  overview: DashboardOverview,
): ChartInstance {
  const chart = echarts.init(
    getChartElement('offers-by-status-chart'),
    undefined,
    { renderer: 'svg' },
  );

  chart.setOption({
    color: STATUS_COLORS,

    aria: {
      enabled: true,
    },

    tooltip: {
      trigger: 'item',
      renderMode: 'richText',
      valueFormatter: (value: unknown) =>
        `${Number(value).toLocaleString('pt-BR')} ofertas`,
    },

    legend: {
      type: 'scroll',
      bottom: 4,
      textStyle: {
        color: TEXT_COLOR,
        fontSize: 11,
      },
    },

    series: [
      {
        type: 'pie',
        radius: ['44%', '70%'],
        center: ['50%', '43%'],
        label: {
          show: false,
        },
        emphasis: {
          scale: true,
          scaleSize: 5,
        },
        data: overview.offers_by_status.map((item) => ({
          name: item.status ?? 'Não informado',
          value: item.offer_count,
        })),
      },
    ],
  });

  return chart;
}

function createDateChart(
  overview: DashboardOverview,
): ChartInstance {
  const chart = echarts.init(
    getChartElement('offers-by-date-chart'),
    undefined,
    { renderer: 'svg' },
  );

  chart.setOption({
    color: [PRIMARY_COLOR],

    aria: {
      enabled: true,
    },

    grid: {
      left: 48,
      right: 24,
      top: 25,
      bottom: 55,
    },

    tooltip: {
      trigger: 'axis',
      renderMode: 'richText',
    },

    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: overview.offers_by_date.map(
        (item) => formatReferenceDate(item.date),
      ),
      axisLabel: {
        color: TEXT_COLOR,
        hideOverlap: true,
        rotate: 35,
        fontSize: 10,
      },
      axisLine: {
        lineStyle: {
          color: GRID_COLOR,
        },
      },
    },

    yAxis: {
      type: 'value',
      minInterval: 1,
      axisLabel: {
        color: TEXT_COLOR,
      },
      splitLine: {
        lineStyle: {
          color: GRID_COLOR,
        },
      },
    },

    series: [
      {
        name: 'Ofertas',
        type: 'line',
        data: overview.offers_by_date.map(
          (item) => item.offer_count,
        ),
        smooth: false,
        showSymbol: false,
        symbolSize: 7,
        lineStyle: {
          width: 3,
        },
        areaStyle: {
          color: 'rgba(56, 109, 155, 0.12)',
        },
      },
    ],
  });

  return chart;
}

export function initializeOverviewCharts(
  overview: DashboardOverview,
): void {
  const chartIds = [
    'offers-by-instrument-chart',
    'offers-by-status-chart',
    'volume-by-instrument-chart',
    'offers-by-date-chart',
  ];

  if (overview.total_offers === 0) {
    for (const chartId of chartIds) {
      getChartElement(chartId).textContent =
        'Nenhuma oferta disponível para visualização.';
    }

    return;
  }

  const instrumentsByCount = overview.offers_by_security_type;

  const instrumentsByVolume = [
    ...overview.offers_by_security_type,
  ].sort(
    (first, second) =>
      Number(second.amount_brl) - Number(first.amount_brl),
  );

  const charts = [
    createInstrumentBarChart(
      'offers-by-instrument-chart',
      instrumentsByCount,
      (instrument) => instrument.offer_count,
      (instrument) =>
        `${instrument.offer_count.toLocaleString('pt-BR')} ofertas`,
      'Ofertas',
    ),

    createStatusChart(overview),

    createInstrumentBarChart(
      'volume-by-instrument-chart',
      instrumentsByVolume,
      (instrument) => Number(instrument.amount_brl) / BILLION,
      (instrument) => formatBrlAmount(instrument.amount_brl).exact,
      'R$ bilhões',
    ),

    createDateChart(overview),
  ];

  const resizeObserver = new ResizeObserver(() => {
    for (const chart of charts) {
      chart.resize();
    }
  });

  for (const chart of charts) {
    resizeObserver.observe(chart.getDom());
  }

  window.addEventListener(
    'pagehide',
    () => {
      resizeObserver.disconnect();

      for (const chart of charts) {
        chart.dispose();
      }
    },
    { once: true },
  );
}
