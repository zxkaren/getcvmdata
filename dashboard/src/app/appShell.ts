export function renderAppShell(pageContent: string): string {
  return `
    <a class="skip-link" href="#main-content">
      Pular para o conteúdo
    </a>

    <div class="app-shell">
      <aside class="sidebar">
        <div class="brand">
          <span class="brand-name">GetCVMData</span>
          <span class="brand-description">
            Inteligência de Mercado
          </span>
        </div>

        <nav class="sidebar-nav" aria-label="Navegação principal">
          <a
            class="nav-item nav-item--active"
            href="#main-content"
            aria-current="page"
          >
            Panorama Executivo
          </a>

          <span class="nav-item nav-item--disabled">
            Radar de Oportunidades
            <small>Em breve</small>
          </span>

          <span class="nav-item nav-item--disabled">
            Emissores e Coordenadores
            <small>Em breve</small>
          </span>

          <span class="nav-item nav-item--disabled">
            Consulta de Ofertas
            <small>Em breve</small>
          </span>
        </nav>

        <div class="sidebar-footer">
          Dados públicos do mercado financeiro brasileiro
        </div>
      </aside>

      <div class="workspace">
        <header class="topbar">
          <span class="topbar-title">
            Mercado de Capitais
          </span>

          <span class="source-tag">
            Fonte: CVM SRE
          </span>
        </header>

        <main
          id="main-content"
          class="dashboard-content"
          tabindex="-1"
        >
          ${pageContent}
        </main>
      </div>
    </div>
  `;
}