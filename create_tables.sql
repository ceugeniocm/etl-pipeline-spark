CREATE TABLE IF NOT EXISTS tb_profissionais (
    prof_id INT PRIMARY KEY,
    prof_nome VARCHAR(150),
    prof_conselhonum VARCHAR(20),
    prof_conselhouf VARCHAR(2),
    prof_sexo VARCHAR(1),
    prof_status INT
);

CREATE TABLE IF NOT EXISTS tb_especialidades (
    esp_id INT PRIMARY KEY,
    esp_descricao VARCHAR(100),
    esp_cmed INT
);

CREATE TABLE IF NOT EXISTS tb_beneficiarios (
    benef_id INT PRIMARY KEY,
    benef_nome VARCHAR(150),
    benef_dtnasc DATETIME,
    benef_cpf VARCHAR(11),
    benef_sexo VARCHAR(1),
    benef_numcarteira VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS tb_procedimentos (
    tblproced_id INT PRIMARY KEY,
    proc_codigo INT,
    proc_descricao VARCHAR(200),
    agp_valor DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS tb_convenios (
    conv_ans VARCHAR(20) PRIMARY KEY,
    conv_sigla VARCHAR(30),
    conv_cnpj VARCHAR(14)
);

CREATE TABLE IF NOT EXISTS tb_planos (
    plano_id INT PRIMARY KEY,
    plano_descricao VARCHAR(100),
    conv_ans VARCHAR(20),
    FOREIGN KEY (conv_ans) REFERENCES tb_convenios(conv_ans)
);

CREATE TABLE IF NOT EXISTS tb_clinicas (
    cli_id INT PRIMARY KEY,
    cli_corporativo INT
);

CREATE TABLE IF NOT EXISTS tb_salas (
    sala_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS tb_cids (
    tbl_idcid INT PRIMARY KEY,
    cid_id INT
);

CREATE TABLE IF NOT EXISTS tb_tipos_agendamento (
    tpa_id INT PRIMARY KEY,
    tpa_descricao VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS tb_usuarios (
    user_id INT PRIMARY KEY,
    nome VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS tb_agendamentos (
    ag_id INT PRIMARY KEY,
    prof_id INT,
    esp_id INT,
    benef_id INT,
    tblproced_id INT,
    plano_id INT,
    cli_id INT,
    sala_id INT,
    tpa_id INT,
    tbl_idcid INT,
    user_id INT,
    dthoraagenda DATETIME,
    ag_statusagendamento VARCHAR(1),
    FOREIGN KEY (prof_id) REFERENCES tb_profissionais(prof_id),
    FOREIGN KEY (esp_id) REFERENCES tb_especialidades(esp_id),
    FOREIGN KEY (benef_id) REFERENCES tb_beneficiarios(benef_id),
    FOREIGN KEY (tblproced_id) REFERENCES tb_procedimentos(tblproced_id),
    FOREIGN KEY (plano_id) REFERENCES tb_planos(plano_id),
    FOREIGN KEY (cli_id) REFERENCES tb_clinicas(cli_id),
    FOREIGN KEY (sala_id) REFERENCES tb_salas(sala_id),
    FOREIGN KEY (tpa_id) REFERENCES tb_tipos_agendamento(tpa_id),
    FOREIGN KEY (tbl_idcid) REFERENCES tb_cids(tbl_idcid),
    FOREIGN KEY (user_id) REFERENCES tb_usuarios(user_id)
);