CREATE USER user_data WITH PASSWORD '12345678';
GRANT CONNECT ON DATABASE project_db TO user_data;

CREATE SCHEMA IF NOT EXISTS ds AUTHORIZATION user_data;

SET ROLE user_data;

CREATE TABLE IF NOT EXISTS ds.md_account_d(
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE NOT NULL,
	account_rk INT NOT NULL,
	account_number VARCHAR(20) NOT NULL,
	char_type VARCHAR(1) NOT NULL,
	currency_rk INT NOT NULL,
	currency_code VARCHAR(3) NOT NULL,
	CONSTRAINT md_account_d_pkey PRIMARY KEY (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d(
	currency_rk INT NOT NULL,
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_code VARCHAR(3),
	code_iso_char VARCHAR(3),
	CONSTRAINT md_currency_d_pkey PRIMARY KEY (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d(
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_rk INT NOT NULL,
	reduced_cource REAL,
	code_iso_num VARCHAR(3),
	CONSTRAINT md_exchange_rate_d_pkey PRIMARY KEY (data_actual_date, currency_rk),
	CONSTRAINT check_reduced_cource CHECK (reduced_cource > 0)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s(
	chapter CHAR(1),
	chapter_name VARCHAR(16),
	section_number INT,
	section_name VARCHAR(22),
	subsection_name VARCHAR(21),
	ledger1_account INT,
	ledger1_account_name VARCHAR(47),
	ledger_account INT NOT NULL,
	ledger_account_name VARCHAR(153),
	characteristic CHAR(1),
	is_resident INT,
	is_reserve INT,
	is_reserved INT,
	is_loan INT,
	is_reserved_assets INT,
	is_overdue INT,
	is_interest INT,
	pair_account VARCHAR(5),
	start_date DATE NOT NULL,
	end_date DATE,
	is_rub_only INT,
	min_term VARCHAR(1),
	min_term_measure VARCHAR(1),
	max_term VARCHAR(1),
	max_term_measure VARCHAR(1),
	ledger_acc_full_name_translit VARCHAR(1),
	is_revaluation VARCHAR(1),
	is_correct VARCHAR(1),
	CONSTRAINT md_ledger_account_s_pkey PRIMARY KEY (ledger_account, start_date)
);

CREATE TABLE IF NOT EXISTS ds.ft_balance_f(
	on_date DATE NOT NULL,
	account_rk INT NOT NULL,
	currency_rk INT,
	balance_out REAL,
	CONSTRAINT ft_balance_f_pkey PRIMARY KEY (on_date, account_rk),
	CONSTRAINT check_balance_out CHECK (balance_out >= 0)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f(
	oper_date DATE NOT NULL,
	credit_account_rk INT NOT NULL,
	debet_account_rk INT NOT NULL,
	credit_amount REAL,
	debet_amount REAL,
	CONSTRAINT check_credit_amount CHECK (credit_amount >= 0),
	CONSTRAINT check_debet_amount CHECK (debet_amount >= 0)
);







