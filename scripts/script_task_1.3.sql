-- Шаг 1
SET ROLE user_data;

CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f(
	from_date DATE NOT NULL,
	to_date DATE NOT NULL,
	chapter CHAR,
	ledger_account CHAR(5) NOT NULL,
	characteristic CHAR,
	balance_in_rub NUMERIC(23,8),
	balance_in_val NUMERIC(23,8),
	balance_in_total NUMERIC(23,8),
	turn_deb_rub NUMERIC(23,8),
	turn_deb_val NUMERIC(23,8),
	turn_deb_total NUMERIC(23,8),
	turn_cre_rub NUMERIC(23,8),
	turn_cre_val NUMERIC(23,8),
	turn_cre_total NUMERIC(23,8),
	balance_out_rub NUMERIC(23,8),
	balance_out_val NUMERIC(23,8),
	balance_out_total NUMERIC(23,8),
	CONSTRAINT dm_f101_round_f_pkey PRIMARY KEY (from_date, to_date, ledger_account)
);


-- Шаг 2

CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (IN i_OnDate DATE) AS
$$ 
DECLARE
	period_start_date DATE = i_OnDate - INTERVAL '1 month';
	period_end_date DATE = i_OnDate - INTERVAL '1 day';
	reporting_period TEXT = period_start_date::TEXT ||' - '||period_end_date::TEXT;
BEGIN

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'CALL PROCEDURE', 'fill_f101_round_f', 'dm', 'STARTED', 'target period: '||reporting_period);	

	DELETE FROM dm.dm_f101_round_f
	WHERE from_date = period_start_date AND "to_date" = period_end_date;
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'DELETE FROM TABLE', 'dm_f101_round_f', 'dm', 'COMPLETED', 'target period: '||reporting_period);
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'INSERT INTO TABLE', 'dm_f101_round_f', 'dm', 'STARTED', 'target period: '||reporting_period);
	
	WITH
	balance_before AS (
		SELECT on_date, account_rk, balance_out, balance_out_rub
		FROM dm.dm_account_balance_f dabf
		WHERE dabf.on_date = period_start_date - INTERVAL '1 days'
	),
	balance_before_ledger_account AS (
		SELECT ledger_account,
				SUM(CASE WHEN currency_code IN ('643', '810') THEN balance_out_rub
						ELSE 0 END) AS balance_in_rub,
				SUM(CASE WHEN currency_code NOT IN ('643', '810') THEN balance_out_rub
						ELSE 0 END) AS balance_in_val,
				SUM(balance_out_rub) AS balance_in_total
		FROM (
			SELECT  SUBSTR(mad.account_number, 1, 5) AS ledger_account,
					mad.currency_code,
					bb.balance_out_rub
			FROM ds.md_account_d mad 
				JOIN balance_before bb USING(account_rk)  
			WHERE period_start_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date) t
		GROUP BY ledger_account
	),
	balance_last AS (
		SELECT on_date, account_rk, balance_out, balance_out_rub
		FROM dm.dm_account_balance_f dabf
		WHERE dabf.on_date = period_end_date
	),
	balance_last_ledger_account AS (
		SELECT ledger_account,
				SUM(CASE WHEN currency_code IN ('643', '810') THEN balance_out_rub
						ELSE 0 END) AS balance_out_rub,
				SUM(CASE WHEN currency_code NOT IN ('643', '810') THEN balance_out_rub
						ELSE 0 END) AS balance_out_val,
				SUM(balance_out_rub) AS balance_out_total
		FROM (
			SELECT  SUBSTR(mad.account_number, 1, 5) AS ledger_account,
					mad.char_type,
					mad.currency_code,
					bl.balance_out_rub
			FROM ds.md_account_d mad 
				JOIN balance_last bl USING(account_rk)  
			WHERE period_end_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date) t
		GROUP BY ledger_account
	),
	target_turnover AS (
		SELECT on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
		FROM dm.dm_account_turnover_f datf
		WHERE datf.on_date BETWEEN period_start_date AND period_end_date
	),
	target_turnover_ledger_account AS (
		SELECT ledger_account,
				SUM(CASE WHEN currency_code IN ('643', '810') THEN debet_amount_rub
						ELSE 0 END) AS turn_deb_rub,
				SUM(CASE WHEN currency_code NOT IN ('643', '810') THEN debet_amount_rub
						ELSE 0 END) AS turn_deb_val,
				SUM(debet_amount_rub) AS turn_deb_total,
				SUM(CASE WHEN currency_code IN ('643', '810') THEN credit_amount_rub
						ELSE 0 END) AS turn_cre_rub,
				SUM(CASE WHEN currency_code NOT IN ('643', '810') THEN credit_amount_rub
						ELSE 0 END) AS turn_cre_val,
				SUM(credit_amount_rub) AS turn_cre_total
		FROM (
			SELECT SUBSTR(mad.account_number, 1, 5) AS ledger_account,
					mad.currency_code,
					tt.credit_amount_rub,
					tt.debet_amount_rub
			FROM ds.md_account_d mad
				JOIN target_turnover tt ON mad.account_rk = tt.account_rk
					AND tt.on_date BETWEEN mad.data_actual_date AND mad.data_actual_end_date) t
		GROUP BY ledger_account
	)
	INSERT INTO dm.dm_f101_round_f (
		from_date, to_date, chapter, ledger_account, characteristic,
		balance_in_rub, balance_in_val, balance_in_total,
		turn_deb_rub, turn_deb_val, turn_deb_total,
		turn_cre_rub, turn_cre_val, turn_cre_total,
		balance_out_rub, balance_out_val, balance_out_total)
	SELECT period_start_date AS from_date,
			period_end_date AS to_date,
			mlas.chapter,
			bbla.ledger_account,
			mlas.characteristic,
			bbla.balance_in_rub,
			bbla.balance_in_val,
			bbla.balance_in_total,
			COALESCE(ttla.turn_deb_rub, 0) AS turn_deb_rub,
			COALESCE(ttla.turn_deb_val, 0) AS turn_deb_val,
			COALESCE(ttla.turn_deb_total, 0) AS turn_deb_total,
			COALESCE(ttla.turn_cre_rub, 0) AS turn_cre_rub,
			COALESCE(ttla.turn_cre_val, 0) AS turn_cre_val,
			COALESCE(ttla.turn_cre_total, 0) AS turn_cre_total,
			blla.balance_out_rub,
			blla.balance_out_val,
			blla.balance_out_total
	FROM balance_before_ledger_account bbla
		LEFT JOIN target_turnover_ledger_account ttla USING(ledger_account)
		LEFT JOIN balance_last_ledger_account blla USING(ledger_account)
		LEFT JOIN ds.md_ledger_account_s mlas ON mlas.ledger_account::TEXT = bbla.ledger_account;
	
	PERFORM PG_SLEEP(2);
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'INSERT INTO TABLE', 'dm_f101_round_f', 'dm', 'COMPLETED', 'target date: '||reporting_period);

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'CALL PROCEDURE', 'fill_f101_round_f', 'dm', 'COMPLETED', 'target date: '||reporting_period);	

END
$$ LANGUAGE plpgsql;


-- Шаг 3

CALL dm.fill_f101_round_f('2018-02-01');

















