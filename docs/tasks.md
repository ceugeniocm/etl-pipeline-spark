# Lista de Tarefas Técnicas

## Fase 1: Setup
- [x] T01: Criar ambiente virtual Python (`.venv`). (Plano: P01, Req: NFR-001)
- [x] T02: Instalar dependências do `requirements.txt`. (Plano: P01, Req: NFR-002)
- [x] T03: Criar arquivo `create_tables.sql` com o esquema MER. (Plano: P02, Req: FR-005)

## Fase 2: Core Features
- [x] T04: Implementar leitura de Excel com Spark no `etl_spark.py`. (Plano: P03, Req: FR-001)
- [x] T05: Aplicar `dropna()` e `fillna()` para tratar nulos. (Plano: P04, Req: FR-002)
- [x] T06: Aplicar `dropDuplicates()` para remover redundâncias. (Plano: P04, Req: FR-003)
- [x] T07: Validar tipos de dados e formatar colunas de data/string. (Plano: P04, Req: FR-004)
- [x] T08: Configurar `config_bigdata.json` com credenciais do banco. (Plano: P05, Req: FR-006)
- [x] T09: Implementar escrita no MySQL via JDBC. (Plano: P05, Req: FR-006)

## Fase 3: Advanced Features & Quality
- [x] T10: Adicionar acumuladores ou contadores de registros. (Plano: P06, Req: FR-007)
- [x] T11: Implementar blocos `try-except` para tratamento de erros. (Plano: P06, Req: FR-007)
- [x] T12: Ajustar código para conformidade com PEP 8. (Plano: P07, Req: NFR-004)

## Fase 4: Testing & QA
- [x] T13: Executar `python -m unittest discover` para validar o ETL. (Plano: P08, Req: FR-001, FR-002, FR-003, FR-004)
- [x] T14: Gerar relatório final de tratamento de dados. (Plano: P09, Req: Todos)
- [x] T15: Atualizar documentação do projeto (Requirements, Plan, Tasks). (Plano: P09, Req: NFR-003)
