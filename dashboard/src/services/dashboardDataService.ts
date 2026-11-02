
import type { DashboardData } from '../types/dashboard';

import { validateDashboardData } from './dashboardDataValidator';

async function fetchJson(fileName: string): Promise<unknown> {
  const response = await fetch(`/${fileName}`, {
    cache: 'no-store',
  });

  if (!response.ok) {
    throw new Error(
      `Falha na leitura de ${fileName}: ${response.status}`,
    );
  }

  try {
    return await response.json() as unknown;
  } catch {
    throw new Error(
      `Conteúdo JSON inválido em ${fileName}.`,
    );
  }
}

export async function loadDashboardData(): Promise<DashboardData> {
  const manifest = await fetchJson('manifest.json');

  const [offersPayload, overviewPayload] = await Promise.all([
    fetchJson('offers.json'),
    fetchJson('overview.json'),
  ]);

  return validateDashboardData(
    manifest,
    offersPayload,
    overviewPayload,
  );
}
