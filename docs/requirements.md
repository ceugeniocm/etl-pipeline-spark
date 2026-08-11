# Documento de Requisitos

## Introdução
Este documento descreve os requisitos funcionais e não funcionais para o projeto de Processamento de Dados com Python e Big Data. O objetivo principal é desenvolver uma aplicação ETL (Extract, Transform, Load) capaz de processar dados de arquivos Excel e inseri-los em um banco de dados em nuvem, seguindo um Modelo Entidade-Relacionamento específico.

## Requisitos Funcionais

### FR-001: Leitura de Arquivos Excel
- **User Story**:
  > Como um desenvolvedor, eu quero que o sistema leia dados de arquivos `.xlsx` para que as informações originais possam ser processadas.
- **Acceptance Criteria**:
  > QUANDO o sistema for executado ENTÃO ele DEVE ler com sucesso os dados do arquivo Excel fornecido.
- **Prioridade**: High
- **Status**: Completed

### FR-002: Tratamento de Dados - Valores Nulos
- **User Story**:
  > Como um analista de dados, eu quero que valores nulos sejam tratados para garantir a integridade dos dados no banco.
- **Acceptance Criteria**:
  > QUANDO um valor nulo for encontrado em campos obrigatórios ENTÃO o sistema DEVE aplicar uma regra de tratamento (ignorar ou preencher) conforme a lógica de negócio.
- **Prioridade**: High
- **Status**: Completed

### FR-003: Tratamento de Dados - Registros Duplicados
- **User Story**:
  > Como um administrador de banco de dados, eu quero que registros duplicados sejam removidos para evitar redundância.
- **Acceptance Criteria**:
  > QUANDO registros com a mesma chave ou conteúdo idêntico forem identificados ENTÃO o sistema DEVE manter apenas uma instância.
- **Prioridade**: Medium
- **Status**: Completed

### FR-004: Padronização e Validação de Tipos
- **User Story**:
  > Como um usuário do sistema, eu quero que os dados estejam padronizados e com tipos corretos para facilitar consultas futuras.
- **Acceptance Criteria**:
  > QUANDO os dados forem lidos ENTÃO o sistema DEVE validar os tipos (ex: datas, números) e padronizar formatos (ex: strings em maiúsculas/minúsculas).
- **Prioridade**: High
- **Status**: Completed

### FR-005: Criação de Tabelas via SQL
- **User Story**:
  > Como um desenvolvedor, eu quero criar as tabelas do banco de dados automaticamente para garantir que a estrutura MER seja respeitada.
- **Acceptance Criteria**:
  > QUANDO o script SQL de criação for executado ENTÃO as tabelas, chaves primárias e estrangeiras DEVEM ser criadas conforme o modelo.
- **Prioridade**: High
- **Status**: Completed

### FR-006: Conexão com Banco de Dados em Nuvem
- **User Story**:
  > Como um integrador, eu quero estabelecer uma conexão segura com o banco de dados em nuvem para persistir os dados processados.
- **Acceptance Criteria**:
  > QUANDO a aplicação iniciar o processo de carga ENTÃO ela DEVE conectar-se ao banco de dados remoto usando as credenciais fornecidas.
- **Prioridade**: High
- **Status**: Completed

### FR-007: Relatórios de Execução
- **User Story**:
  > Como um operador, eu quero ver estatísticas da execução para monitorar o sucesso do processamento.
- **Acceptance Criteria**:
  > QUANDO o processamento terminar ENTÃO o sistema DEVE exibir a quantidade de registros lidos, inseridos, ignorados e erros encontrados.
- **Prioridade**: Medium
- **Status**: Completed

## Requisitos Não Funcionais

### NFR-001: Linguagem de Programação
- **User Story**:
  > Como um arquiteto, eu quero que o projeto utilize Python para manter a compatibilidade com as ferramentas de Big Data estudadas.
- **Acceptance Criteria**:
  > O sistema DEVE ser desenvolvido inteiramente em Python 3.
- **Prioridade**: High
- **Status**: Completed

### NFR-002: Uso de Tecnologias Big Data
- **User Story**:
  > Como um professor da disciplina, eu quero que o aluno utilize Spark ou bibliotecas similares para demonstrar conhecimento em processamento distribuído/larga escala.
- **Acceptance Criteria**:
  > O sistema DEVE utilizar bibliotecas como PySpark ou Pandas de forma eficiente para o processamento de dados.
- **Prioridade**: High
- **Status**: Completed

### NFR-003: Internacionalização e Idioma
- **User Story**:
  > Como um usuário brasileiro, eu quero que as mensagens da UI e erros estejam em Português (pt_BR).
- **Acceptance Criteria**:
  > O sistema DEVE exibir strings de interface e logs em Português brasileiro, mantendo termos técnicos em Inglês.
- **Prioridade**: Low
- **Status**: Completed

### NFR-004: Qualidade de Código (PEP 8)
- **User Story**:
  > Como um mantenedor de código, eu quero que o código siga o padrão PEP 8 para facilitar a leitura e manutenção.
- **Acceptance Criteria**:
  > O código DEVE passar em verificações de linting seguindo as diretrizes da PEP 8.
- **Prioridade**: Medium
- **Status**: Completed
