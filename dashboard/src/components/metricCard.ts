interface MetricCardOptions {
  id: string;
  label: string;
  description: string;
  detailId?: string;
}

export function renderMetricCard({
  id,
  label,
  description,
  detailId,
}: MetricCardOptions): string {
  return `
    <article class="metric-card">
      <p class="metric-card-label">${label}</p>

      <strong
        id="${id}"
        class="metric-card-value"
      >
        —
      </strong>

      ${
        detailId
          ? `<p id="${detailId}" class="metric-card-detail"></p>`
          : ''
      }

      <p class="metric-card-description">
        ${description}
      </p>
    </article>
  `;
}
