# VibeSQL CLI Localização - Português Brasileiro (pt-BR)
# Este arquivo contém todas as strings visíveis ao usuário da interface de linha de comando.

# =============================================================================
# Banner do REPL e Mensagens Básicas
# =============================================================================

cli-banner = VibeSQL v{ $version } - Banco de Dados com Conformidade COMPLETA ao SQL:1999
cli-help-hint = Digite \help para ajuda, \quit para sair
cli-goodbye = Até logo!
locale-changed = Idioma alterado para { $locale }

# =============================================================================
# Texto de Ajuda dos Comandos (Argumentos Clap)
# =============================================================================

cli-about = VibeSQL - Banco de Dados com Conformidade COMPLETA ao SQL:1999

cli-long-about = Interface de linha de comando do VibeSQL

    MODOS DE USO:
      REPL Interativo:       vibesql (--database <ARQUIVO>)
      Executar Comando:      vibesql -c "SELECT * FROM usuarios"
      Executar Arquivo:      vibesql -f script.sql
      Executar via stdin:    cat dados.sql | vibesql
      Gerar Tipos:           vibesql codegen --schema schema.sql --output types.ts

    REPL INTERATIVO:
      Quando iniciado sem -c, -f ou entrada por pipe, o VibeSQL entra em um REPL
      interativo com suporte a readline, histórico de comandos e meta-comandos como:
        \d (tabela)  - Descrever tabela ou listar todas as tabelas
        \dt          - Listar tabelas
        \f <formato> - Definir formato de saída
        \copy        - Importar/exportar CSV/JSON
        \help        - Mostrar todos os comandos do REPL

    SUBCOMANDOS:
      codegen           Gerar tipos TypeScript a partir do esquema do banco de dados

    CONFIGURAÇÃO:
      As configurações podem ser definidas em ~/.vibesqlrc (formato TOML).
      Seções: display, database, history, query

    EXEMPLOS:
      # Iniciar REPL interativo com banco de dados em memória
      vibesql

      # Usar arquivo de banco de dados persistente
      vibesql --database meusdados.db

      # Executar um único comando
      vibesql -c "CREATE TABLE usuarios (id INT, nome VARCHAR(100))"

      # Executar arquivo de script SQL
      vibesql -f schema.sql -v

      # Importar dados de CSV
      echo "\copy usuarios FROM 'dados.csv'" | vibesql --database meusdados.db

      # Exportar resultados da consulta como JSON
      vibesql -d meusdados.db -c "SELECT * FROM usuarios" --format json

      # Gerar tipos TypeScript a partir de um arquivo de esquema
      vibesql codegen --schema schema.sql --output src/types.ts

      # Gerar tipos TypeScript a partir de um banco de dados em execução
      vibesql codegen --database meusdados.db --output src/types.ts

# Strings de ajuda dos argumentos
arg-database-help = Caminho do arquivo de banco de dados (se não especificado, usa banco de dados em memória)
arg-file-help = Executar comandos SQL de um arquivo
arg-command-help = Executar comando SQL diretamente e sair
arg-stdin-help = Ler comandos SQL do stdin (detectado automaticamente quando usado pipe)
arg-verbose-help = Mostrar saída detalhada durante a execução de arquivo/stdin
arg-format-help = Formato de saída para resultados de consultas
arg-lang-help = Definir o idioma de exibição (ex., en-US, es, pt-BR)

# =============================================================================
# Subcomando Codegen
# =============================================================================

codegen-about = Gerar tipos TypeScript a partir do esquema do banco de dados

codegen-long-about = Gerar definições de tipos TypeScript a partir de um esquema de banco de dados VibeSQL.

    Este comando cria interfaces TypeScript para todas as tabelas no banco de dados,
    junto com objetos de metadados para verificação de tipos em tempo de execução e suporte a IDE.

    FONTES DE ENTRADA:
      --database <ARQUIVO>  Gerar a partir de um arquivo de banco de dados existente
      --schema <ARQUIVO>    Gerar a partir de um arquivo de esquema SQL (instruções CREATE TABLE)

    SAÍDA:
      --output <ARQUIVO>    Gravar tipos gerados neste arquivo (padrão: types.ts)

    OPÇÕES:
      --camel-case          Converter nomes de colunas para camelCase
      --no-metadata         Ignorar geração do objeto de metadados das tabelas

    EXEMPLOS:
      # A partir de um arquivo de banco de dados
      vibesql codegen --database meusdados.db --output src/db/types.ts

      # A partir de um arquivo de esquema SQL
      vibesql codegen --schema schema.sql --output src/db/types.ts

      # Com nomes de propriedades em camelCase
      vibesql codegen --schema schema.sql --output types.ts --camel-case

codegen-schema-help = Arquivo de esquema SQL contendo instruções CREATE TABLE
codegen-output-help = Caminho do arquivo de saída para TypeScript gerado
codegen-camel-case-help = Converter nomes de colunas para camelCase
codegen-no-metadata-help = Ignorar geração do objeto de metadados das tabelas

codegen-from-schema = Gerando tipos TypeScript a partir do arquivo de esquema: { $path }
codegen-from-database = Gerando tipos TypeScript a partir do banco de dados: { $path }
codegen-written = Tipos TypeScript gravados em: { $path }
codegen-error-no-source = É necessário especificar --database ou --schema.
    Use 'vibesql codegen --help' para informações de uso.

# =============================================================================
# Ajuda dos Meta-comandos (saída do \help)
# =============================================================================

help-title = Meta-comandos:
help-describe = \d (tabela)     - Descrever tabela ou listar todas as tabelas
help-tables = \dt             - Listar tabelas
help-schemas = \ds             - Listar esquemas
help-indexes = \di             - Listar índices
help-roles = \du             - Listar roles/usuários
help-format = \f <formato>    - Definir formato de saída (table, json, csv, markdown, html)
help-timing = \timing         - Alternar tempo de consulta
help-copy-to = \copy <tabela> TO <arquivo>   - Exportar tabela para arquivo CSV/JSON
help-copy-from = \copy <tabela> FROM <arquivo> - Importar arquivo CSV para tabela
help-save = \save (arquivo) - Salvar banco de dados em arquivo de dump SQL
help-errors = \errors         - Mostrar histórico de erros recentes
help-help = \h, \help      - Mostrar esta ajuda
help-quit = \q, \quit      - Sair

help-sql-title = Introspecção SQL:
help-show-tables = SHOW TABLES                  - Listar todas as tabelas
help-show-databases = SHOW DATABASES               - Listar todos os esquemas/bancos de dados
help-show-columns = SHOW COLUMNS FROM <tabela>   - Mostrar colunas da tabela
help-show-index = SHOW INDEX FROM <tabela>     - Mostrar índices da tabela
help-show-create = SHOW CREATE TABLE <tabela>   - Mostrar instrução CREATE TABLE
help-describe-sql = DESCRIBE <tabela>            - Alias para SHOW COLUMNS

help-examples-title = Exemplos:
help-example-create = CREATE TABLE usuarios (id INT PRIMARY KEY, nome VARCHAR(100));
help-example-insert = INSERT INTO usuarios VALUES (1, 'Alice'), (2, 'Roberto');
help-example-select = SELECT * FROM usuarios;
help-example-show-tables = SHOW TABLES;
help-example-show-columns = SHOW COLUMNS FROM usuarios;
help-example-describe = DESCRIBE usuarios;
help-example-format-json = \f json
help-example-format-md = \f markdown
help-example-copy-to = \copy usuarios TO '/tmp/usuarios.csv'
help-example-copy-from = \copy usuarios FROM '/tmp/usuarios.csv'
help-example-copy-json = \copy usuarios TO '/tmp/usuarios.json'
help-example-errors = \errors

# =============================================================================
# Mensagens de Status
# =============================================================================

format-changed = Formato de saída definido para: { $format }
database-saved = Banco de dados salvo em: { $path }
no-database-file = Erro: Nenhum arquivo de banco de dados especificado. Use \save <nome_arquivo> ou inicie com a flag --database

# =============================================================================
# Exibição de Erros
# =============================================================================

no-errors = Nenhum erro nesta sessão.
recent-errors = Erros recentes:

# =============================================================================
# Mensagens de Execução de Script
# =============================================================================

script-no-statements = Nenhuma instrução SQL encontrada no script
script-executing = Executando instrução { $current } de { $total }...
script-error = Erro ao executar instrução { $index }: { $error }
script-summary-title = === Resumo da Execução do Script ===
script-total = Total de instruções: { $count }
script-successful = Bem-sucedidas: { $count }
script-failed = Falhas: { $count }
script-failed-error = { $count } instruções falharam

# =============================================================================
# Formatação de Saída
# =============================================================================

rows-with-time = { $count } linhas no conjunto ({ $time }s)
rows-count = { $count } linhas

# =============================================================================
# Avisos
# =============================================================================

warning-config-load = Aviso: Não foi possível carregar o arquivo de configuração: { $error }
warning-auto-save-failed = Aviso: Falha ao salvar automaticamente o banco de dados: { $error }
warning-save-on-exit-failed = Aviso: Falha ao salvar o banco de dados ao sair: { $error }

# =============================================================================
# Operações de Arquivo
# =============================================================================

file-read-error = Falha ao ler arquivo '{ $path }': { $error }
stdin-read-error = Falha ao ler do stdin: { $error }
database-load-error = Falha ao carregar banco de dados: { $error }
