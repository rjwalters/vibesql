# VibeSQL Web UI - Português (Brasil)

# Page titles
page-title = VibeSQL - Banco de Dados SQL:1999 com IA
demo-title = Demo do VibeSQL
benchmarks-title = Benchmarks de Desempenho - VibeSQL
benchmarks-heading = VibeSQL - Benchmarks de Desempenho
conformance-title = Relatório de Conformidade - VibeSQL
conformance-heading = Relatório de Conformidade
conformance-subtitle = Testes de Conformidade com o Padrão SQL:1999

# Navigation
nav-showcase = Demonstração SQL:1999
nav-conformance = Ver resultados do sqltest
nav-sqllogictest = Ver resultados do SQLLogicTest

# Editor section
editor-title = Editor SQL
editor-storage = Armazenamento
editor-storage-init = Inicializando...
editor-execute = Executar Consulta

# Results section
results-title = Resultados
results-empty = Execute uma consulta para ver os resultados
results-loading = Carregando...
results-rows = { $count } { $count ->
    [one] linha
   *[other] linhas
}
results-rows-with-time = { $count } { $count ->
    [one] linha
   *[other] linhas
} ({ $time }ms)
results-copy = Copiar para área de transferência
results-export = Exportar CSV
results-limit-warning = Mostrando as primeiras { $limit } de { $total } linhas. Use a cláusula LIMIT para refinar sua consulta.

# Examples sidebar
examples-title = Exemplos
examples-basic = Consultas Básicas
examples-advanced = Consultas Avançadas

# Database selector
db-select-label = Banco de dados

# Footer
footer-tagline = VibeSQL - Banco de Dados SQL:1999 em WebAssembly
footer-deployed = Implantado: { $date }

# Theme
theme-toggle-dark = Mudar para modo escuro
theme-toggle-light = Mudar para modo claro

# Locale
locale-select = Selecionar idioma

# Messages
msg-query-success = Consulta executada com sucesso
msg-rows-affected = { $count } { $count ->
    [one] linha afetada
   *[other] linhas afetadas
}

# Errors
error-generic = Ocorreu um erro
error-query-failed = A consulta falhou
