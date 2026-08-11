# Diagrama de Entidade-Relacionamento (D.E.R)

Este documento descreve a estrutura das tabelas, campos, tipos de dados e relacionamentos do banco de dados.

---

### 🏥 PROFISSIONAL
Tabela que armazena os dados dos profissionais de saúde.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `PROF_ID` | `integer` | **PK** |
| `PROF_NOME` | `varchar(150)` | |
| `PROF_CONSELHONUM` | `varchar(20)` | |
| `PROF_CONSELHOUF` | `varchar(2)` | |
| `PROF_SEXO` | `varchar(1)` | |
| `PROF_STATUS` | `integer` | |

---

### 🩺 ESPECIALIDADE
Tabela de especialidades médicas.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `ESP_ID` | `integer` | **PK** |
| `ESP_DESCRICAO` | `varchar(100)` | |
| `ESP_CMED` | `integer` | |

---

### 👤 BENEFICIARIO
Dados dos beneficiários (pacientes).

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `BENEF_ID` | `integer` | **PK** |
| `BENEF_NOME` | `varchar(150)` | |
| `BENEF_DTNASC` | `datetime` | |
| `BENEF_CPF` | `varchar(11)` | |
| `BENEF_SEXO` | `varchar(1)` | |
| `BENEF_NUMCARTEIRA` | `varchar(30)` | |

---

### 📝 PROCEDIMENTO
Tabela de procedimentos e seus valores.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `TBLPROCED_ID` | `integer` | **PK** |
| `PROC_CODIGO` | `integer` | |
| `PROC_DESCRICAO` | `varchar(200)` | |
| `AGP_VALOR` | `decimal(10,2)` | |

---

### 🏢 CONVENIO
Dados das operadoras de saúde.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `CONV_ANS` | `varchar(20)` | **PK** |
| `CONV_SIGLA` | `varchar(30)` | |
| `CONV_CNPJ` | `varchar(14)` | |

---

### 📜 PLANO
Planos de saúde associados aos convênios.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `PLANO_ID` | `integer` | **PK** |
| `PLANO_DESCRICAO` | `varchar(100)` | |
| `CONV_ANS` | `varchar(20)` | **FK** |

---

### 📅 AGENDAMENTO
Tabela principal que relaciona todas as entidades para compor o agendamento.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `AG_ID` | `integer` | **PK** |
| `PROF_ID` | `integer` | **FK** |
| `ESP_ID` | `integer` | **FK** |
| `BENEF_ID` | `integer` | **FK** |
| `TBLPROCED_ID` | `integer` | **FK** |
| `PLANO_ID` | `integer` | **FK** |
| `CLI_ID` | `integer` | **FK** |
| `SALA_ID` | `integer` | **FK** |
| `TPA_ID` | `integer` | **FK** |
| `TBL_IDCID` | `integer` | **FK** |
| `USER_ID` | `integer` | **FK** |
| `DTHORAAGENDA` | `datetime` | |
| `AG_STATUSAGENDAMENTO` | `varchar(1)` | |

---

### 🏠 CLINICA
Informações sobre as clínicas.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `CLI_ID` | `integer` | **PK** |
| `CLI_CORPORATIVO` | `integer` | |

---

### 🚪 SALA
Identificação das salas de atendimento.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `SALA_ID` | `integer` | **PK** |

---

### 🆔 CID
Classificação Internacional de Doenças.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `TBL_IDCID` | `integer` | **PK** |
| `CID_ID` | `integer` | |

---

### 🏷️ TIPO AGENDAMENTO
Tipos de agendamentos realizados.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `TPA_ID` | `integer` | **PK** |
| `ΤΡΑ_DESCRICAO` | `varchar(100)` | |

---

### 👤 USUARIO
Usuários do sistema que realizam o agendamento.

| Campo | Tipo | Chave |
| :--- | :--- | :--- |
| `USER_ID` | `integer` | **PK** |
| `NOME` | `varchar(150)` | |
