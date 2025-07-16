SET ROLE user_data;

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 AS
	TABLE dm.dm_f101_round_f;

ALTER TABLE dm.dm_f101_round_f_v2
	ADD CONSTRAINT dm_f101_round_f_v2_pkey 
		PRIMARY KEY (from_date, to_date, ledger_account);