-- ============================================================
-- Script DDL completo - Agenda Médica (MySQL)
-- Gerado a partir do Diagrama Entidade-Relacionamento
-- ============================================================

-- Remove as tabelas se já existirem (ordem inversa das dependências)
DROP TABLE IF EXISTS tb_agendamentos;
DROP TABLE IF EXISTS tb_planos;
DROP TABLE IF EXISTS tb_profissionais;
DROP TABLE IF EXISTS tb_especialidades;
DROP TABLE IF EXISTS tb_beneficiarios;
DROP TABLE IF EXISTS tb_procedimentos;
DROP TABLE IF EXISTS tb_convenios;
DROP TABLE IF EXISTS tb_clinicas;
DROP TABLE IF EXISTS tb_salas;
DROP TABLE IF EXISTS tb_cids;
DROP TABLE IF EXISTS tb_tipos_agendamento;
DROP TABLE IF EXISTS tb_usuarios;

-- ============================================================
-- Tabelas independentes
-- ============================================================

CREATE TABLE tb_profissionais (
    prof_id             BIGINT          NOT NULL,
    prof_nome           VARCHAR(150)    NOT NULL,
    prof_conselhonum    VARCHAR(20),
    prof_conselhouf     VARCHAR(2),
    prof_sexo           VARCHAR(1),
    prof_status         INT,
    PRIMARY KEY (prof_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_especialidades (
    esp_id              BIGINT          NOT NULL,
    esp_descricao       VARCHAR(100)    NOT NULL,
    esp_cmed            INT,
    PRIMARY KEY (esp_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_beneficiarios (
    benef_id            BIGINT          NOT NULL,
    benef_nome          VARCHAR(150)    NOT NULL,
    benef_dtnasc        DATETIME,
    benef_cpf           VARCHAR(11),
    benef_sexo          VARCHAR(1),
    benef_numcarteira   VARCHAR(30),
    PRIMARY KEY (benef_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_procedimentos (
    tblproced_id        BIGINT          NOT NULL,
    proc_codigo         BIGINT,
    proc_descricao      VARCHAR(200),
    agp_valor           DECIMAL(10,2),
    PRIMARY KEY (tblproced_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_convenios (
    conv_ans            VARCHAR(20)     NOT NULL,
    conv_sigla          VARCHAR(30),
    conv_cnpj           VARCHAR(14),
    PRIMARY KEY (conv_ans)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_clinicas (
    cli_id              BIGINT          NOT NULL,
    cli_corporativo     INT,
    PRIMARY KEY (cli_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_salas (
    sala_id             BIGINT          NOT NULL,
    PRIMARY KEY (sala_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_cids (
    tbl_idcid           BIGINT          NOT NULL,
    cid_id              BIGINT,
    PRIMARY KEY (tbl_idcid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_tipos_agendamento (
    tpa_id              BIGINT          NOT NULL,
    tpa_descricao       VARCHAR(100),
    PRIMARY KEY (tpa_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE tb_usuarios (
    user_id             BIGINT          NOT NULL,
    nome                VARCHAR(150),
    PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- Tabela dependente de tb_convenios
-- ============================================================

CREATE TABLE tb_planos (
    plano_id            BIGINT          NOT NULL,
    plano_descricao     VARCHAR(100),
    conv_ans            VARCHAR(20),
    PRIMARY KEY (plano_id),
    CONSTRAINT FK_TB_PLANO_CONVENIO
        FOREIGN KEY (conv_ans)
        REFERENCES tb_convenios (conv_ans)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- Tabela central de tb_agendamentos
-- ============================================================

CREATE TABLE tb_agendamentos (
    ag_id                   BIGINT          NOT NULL,
    prof_id                 BIGINT          NOT NULL,
    esp_id                  BIGINT          NOT NULL,
    benef_id                BIGINT          NOT NULL,
    tblproced_id            BIGINT          NOT NULL,
    plano_id                BIGINT          NOT NULL,
    cli_id                  BIGINT          NOT NULL,
    sala_id                 BIGINT          NOT NULL,
    tpa_id                  BIGINT          NOT NULL,
    tbl_idcid               BIGINT,
    user_id                 BIGINT          NOT NULL,
    dthoraagenda            DATETIME        NOT NULL,
    ag_statusagendamento    VARCHAR(1),
    PRIMARY KEY (ag_id),

    -- Relacionamentos (1:N)
    CONSTRAINT FK_TB_AGENDAMENTO_PROFISSIONAL
        FOREIGN KEY (prof_id)
        REFERENCES tb_profissionais (prof_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_ESPECIALIDADE
        FOREIGN KEY (esp_id)
        REFERENCES tb_especialidades (esp_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_BENEFICIARIO
        FOREIGN KEY (benef_id)
        REFERENCES tb_beneficiarios (benef_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_PROCEDIMENTO
        FOREIGN KEY (tblproced_id)
        REFERENCES tb_procedimentos (tblproced_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_PLANO
        FOREIGN KEY (plano_id)
        REFERENCES tb_planos (plano_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_CLINICA
        FOREIGN KEY (cli_id)
        REFERENCES tb_clinicas (cli_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_SALA
        FOREIGN KEY (sala_id)
        REFERENCES tb_salas (sala_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_TIPO_AGENDAMENTO
        FOREIGN KEY (tpa_id)
        REFERENCES tb_tipos_agendamento (tpa_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_CID
        FOREIGN KEY (tbl_idcid)
        REFERENCES tb_cids (tbl_idcid)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT FK_TB_AGENDAMENTO_USUARIO
        FOREIGN KEY (user_id)
        REFERENCES tb_usuarios (user_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Fim do script