-- Шаг 1
GRANT USAGE ON SCHEMA logs TO user_data;
GRANT INSERT ON logs.data_logs TO user_data;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA logs TO user_data;

-- Шаг 2 
CREATE SCHEMA IF NOT EXISTS dm AUTHORIZATION user_data;

SET ROLE user_data;

CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f(
	on_date DATE,
	account_rk INT,
	credit_amount NUMERIC(23,8),
	credit_amount_rub NUMERIC(23,8),
	debet_amount NUMERIC(23,8),
	debet_amount_rub NUMERIC(23,8),
	CONSTRAINT dm_account_turnover_f_pkey PRIMARY KEY (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f(
	on_date DATE NOT NULL,
	account_rk INT NOT NULL,
	balance_out NUMERIC(23,8),
	balance_out_rub NUMERIC(23,8),
	CONSTRAINT dm_account_balance_f_pkey PRIMARY KEY (on_date, account_rk)
);


-- Шаг 3
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f (IN i_OnDate DATE) AS
$$
BEGIN

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'CALL PROCEDURE', 'fill_account_turnover_f', 'ds', 'STARTED', 'target date: '||i_OnDate::TEXT);	

	DELETE FROM dm.dm_account_turnover_f 
	WHERE on_date = i_OnDate;
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'DELETE FROM TABLE', 'dm_account_turnover_f', 'dm', 'COMPLETED', 'target date: '||i_OnDate::TEXT);
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'INSERT INTO TABLE', 'dm_account_turnover_f', 'dm', 'STARTED', 'target date: '||i_OnDate::TEXT);
	
	WITH 
	target_ft_posting_f AS (
		SELECT * FROM ds.ft_posting_f f WHERE f.oper_date = i_OnDate
	),
	credit_accounts AS (
		SELECT f.oper_date, f.credit_account_rk, SUM(f.credit_amount) AS sum_credit_amount
		FROM target_ft_posting_f f 
		GROUP BY f.oper_date, f.credit_account_rk
	),
	debet_accounts AS (
		SELECT f.oper_date, f.debet_account_rk, SUM(f.debet_amount) AS sum_debet_amount
		FROM target_ft_posting_f f 
		GROUP BY f.oper_date, f.debet_account_rk
	),
	credit_debet AS (
		SELECT COALESCE(ca.credit_account_rk, da.debet_account_rk) AS account_rk,
				COALESCE(ca.sum_credit_amount, 0) AS sum_credit_amount,
				COALESCE(da.sum_debet_amount, 0) AS sum_debet_amount
		FROM  credit_accounts ca 
			FULL JOIN debet_accounts da ON ca.credit_account_rk = da.debet_account_rk
	)
	INSERT INTO dm.dm_account_turnover_f 
		(on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
	SELECT i_OnDate, 
			cd.account_rk,
			cd.sum_credit_amount AS credit_amount,
			cd.sum_credit_amount * COALESCE(merd.reduced_cource, 1) AS credit_amount_rub,
			cd.sum_debet_amount AS debet_amount,
			cd.sum_debet_amount * COALESCE(merd.reduced_cource, 1) AS debet_amount_rub
	FROM credit_debet cd
		LEFT JOIN ds.md_account_d mad ON cd.account_rk = mad.account_rk
			AND i_OnDate BETWEEN mad.data_actual_date AND mad.data_actual_end_date
		LEFT JOIN ds.md_exchange_rate_d merd ON merd.currency_rk = mad.currency_rk
			AND i_OnDate BETWEEN merd.data_actual_date AND merd.data_actual_end_date;
	
	PERFORM PG_SLEEP(2);
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'INSERT INTO TABLE', 'dm_account_turnover_f', 'dm', 'COMPLETED', 'target date: '||i_OnDate::TEXT);

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'CALL PROCEDURE', 'fill_account_turnover_f', 'ds', 'COMPLETED', 'target date: '||i_OnDate::TEXT);	

END
$$ LANGUAGE plpgsql;

-- Шаг 4

CALL ds.fill_account_turnover_f('2018-01-01');
CALL ds.fill_account_turnover_f('2018-01-09');

-- Шаг 5
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(IN i_OnDate DATE) AS
$$
BEGIN
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'CALL PROCEDURE', 'fill_account_balance_f', 'ds', 'STARTED', 'target date: '||i_OnDate::TEXT);	

	DELETE FROM dm.dm_account_balance_f 
	WHERE on_date = i_OnDate;	

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'DELETE FROM TABLE', 'dm_account_balance_f', 'dm', 'COMPLETED', 'target date: '||i_OnDate::TEXT);
	
	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'INSERT INTO TABLE', 'dm_account_balance_f', 'dm', 'STARTED', 'target date: '||i_OnDate::TEXT);

	WITH
	last_balance AS (
		SELECT on_date, account_rk, balance_out, balance_out_rub
		FROM dm.dm_account_balance_f
		WHERE on_date = i_OnDate - INTERVAL '1 day'
	)
	INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
	SELECT i_OnDate, mad.account_rk,
			CASE mad.char_type
				WHEN 'А' THEN COALESCE(lb.balance_out, 0) + COALESCE(dat.debet_amount, 0) - COALESCE(dat.credit_amount, 0)
				WHEN 'П' THEN COALESCE(lb.balance_out, 0) - COALESCE(dat.debet_amount, 0) + COALESCE(dat.credit_amount, 0)
			END AS balance_out,
			CASE mad.char_type
				WHEN 'А' THEN COALESCE(lb.balance_out_rub, 0) + COALESCE(dat.debet_amount_rub, 0) - COALESCE(dat.credit_amount_rub, 0)
				WHEN 'П' THEN COALESCE(lb.balance_out_rub, 0) - COALESCE(dat.debet_amount_rub, 0) + COALESCE(dat.credit_amount_rub, 0)
			END AS balance_out_rub
	FROM ds.md_account_d mad
		LEFT JOIN ds.md_exchange_rate_d merd ON merd.currency_rk = mad.currency_rk
			AND i_OnDate BETWEEN merd.data_actual_date AND merd.data_actual_end_date
		LEFT JOIN last_balance lb USING(account_rk)
		LEFT JOIN dm.dm_account_turnover_f dat USING(on_date, account_rk)
	WHERE i_OnDate BETWEEN mad.data_actual_date AND mad.data_actual_end_date;

	PERFORM PG_SLEEP(2);

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'INSERT INTO TABLE', 'dm_account_balance_f', 'dm', 'COMPLETED', 'target date: '||i_OnDate::TEXT);

	INSERT INTO logs.data_logs(datetime, "event", object_name, schema_name, status, value)
	VALUES (CLOCK_TIMESTAMP(), 'CALL PROCEDURE', 'fill_account_balance_f', 'ds', 'COMPLETED', 'target date: '||i_OnDate::TEXT);

END
$$ LANGUAGE plpgsql;

-- Шаг 6
INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out, balance_out_rub)
SELECT fbf.on_date, fbf.account_rk, fbf.balance_out,
		fbf.balance_out * COALESCE(merd.reduced_cource, 1) AS balance_out_rub
FROM ds.ft_balance_f fbf
	LEFT JOIN ds.md_exchange_rate_d merd ON merd.currency_rk = fbf.currency_rk
		AND '2017-12-31' BETWEEN merd.data_actual_date AND merd.data_actual_end_date
WHERE fbf.on_date = '2017-12-31';

CALL ds.fill_account_balance_f('2018-01-01');



