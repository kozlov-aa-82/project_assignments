-- Шаг 1 - анализ таблицы dm.loan_holiday_info

SELECT effective_from_date,
		COUNT(1) AS all_deal_rk,
		COUNT(DISTINCT deal_rk) AS unique_deal_rk
FROM dm.loan_holiday_info
GROUP BY effective_from_date
HAVING COUNT(1) > 1;

WITH 
t_deal_rk AS (
	SELECT deal_rk
	FROM dm.loan_holiday_info
	GROUP BY effective_from_date, deal_rk
	HAVING COUNT(1) > 1
)
SELECT *
FROM dm.loan_holiday_info l 
WHERE l.deal_rk IN (SELECT deal_rk FROM t_deal_rk);


-- Шаг 2 - анализ таблицы rd.deal_info

SELECT d.effective_from_date,
		COUNT(1) AS all_deal_rk,
		COUNT(DISTINCT deal_rk) AS unique_deal_rk
FROM rd.deal_info d 
GROUP BY d.effective_from_date;

WITH 
t_deal_rk AS (
	SELECT deal_rk
	FROM rd.deal_info d 
	GROUP BY effective_from_date, deal_rk
	HAVING COUNT(deal_rk) > 1)
SELECT *
FROM rd.deal_info d  
WHERE d.deal_rk IN (SELECT deal_rk FROM t_deal_rk)
ORDER BY d.effective_from_date, d.deal_rk;


-- Шаг 3 - анализ таблицы rd.loan_holiday 

SELECT l.effective_from_date,
		COUNT(1) AS all_deal_rk,
		COUNT(DISTINCT deal_rk) AS unique_deal_rk
FROM rd.loan_holiday l 
GROUP BY l.effective_from_date;

WITH 
t_deal_rk AS (
	SELECT deal_rk
	FROM rd.loan_holiday l
	GROUP BY effective_from_date, deal_rk
	HAVING COUNT(deal_rk) > 1)
SELECT *
FROM rd.loan_holiday l 
WHERE l.deal_rk IN (SELECT deal_rk FROM t_deal_rk)
ORDER BY l.effective_from_date, l.deal_rk;


-- Шаг 4 - анализ таблицы rd.product

SELECT p.effective_from_date,
		COUNT(1) AS all_product_rk,
		COUNT(DISTINCT product_rk) AS unique_product_rk
FROM rd.product p 
GROUP BY p.effective_from_date;

WITH 
t_product_rk AS (
	SELECT product_rk
	FROM rd.product p 
	GROUP BY effective_from_date, product_rk
	HAVING COUNT(product_rk) > 1)
SELECT *
FROM rd.product p 
WHERE p.product_rk IN (SELECT product_rk FROM t_product_rk)
ORDER BY p.effective_from_date, p.product_rk;


-- Шаг 5 - анализ данных из файла product_info.csv

CREATE TABLE rd.product_v2 AS TABLE rd.product WITH NO DATA; 

SELECT p.effective_from_date,
		COUNT(1) AS all_product_rk,
		COUNT(DISTINCT product_rk) AS unique_product_rk
FROM rd.product_v2 p 
GROUP BY p.effective_from_date;

WITH 
t_product_rk AS (
	SELECT product_rk
	FROM rd.product_v2 p 
	GROUP BY effective_from_date, product_rk
	HAVING COUNT(product_rk) > 1)
SELECT *
FROM rd.product_v2 p 
WHERE p.product_rk IN (SELECT product_rk FROM t_product_rk)
ORDER BY p.effective_from_date, p.product_rk;


(SELECT *
FROM rd.product_v2 p2
WHERE p2.effective_from_date = '2023-03-15'
EXCEPT
SELECT *
FROM rd.product p)
UNION ALL
(SELECT *
FROM rd.product p
EXCEPT
SELECT *
FROM rd.product_v2 p2
WHERE p2.effective_from_date = '2023-03-15');


-- Шаг 6 - дедубликация данных таблицы rd.product

BEGIN; --  открываем транзакцию

LOCK TABLE rd.product IN SHARE MODE; -- блокируем заблицу от возможности параллельных изменений

CREATE TEMP TABLE new_product ON COMMIT DROP AS -- создаём всременную таблицу и добавляем в неё дедублицированные данные
	SELECT DISTINCT ON (product_rk, product_name, effective_from_date, effective_to_date) *
	FROM rd.product
	ORDER BY product_rk, product_name, effective_from_date, effective_to_date;

TRUNCATE rd.product; -- очищаем исходную таблицу

INSERT INTO rd.product SELECT * FROM new_product; -- вставляем обратно обработанные данные из временной таблицы

COMMIT;


-- Шаг 7 - проверка работы прототипа процедуры

WITH
loan_holiday_info_v2 AS(
	SELECT d.deal_rk
        ,lh.effective_from_date
        ,lh.effective_to_date
        ,d.deal_num AS deal_number --Номер сделки
	    ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
        ,d.deal_name --Наименование сделки
        ,d.deal_sum --Сумма сделки
        ,d.client_rk --Ссылка на контрагента
        ,d.agreement_rk --Ссылка на договор
        ,d.deal_start_date --Дата начала действия сделки
        ,d.department_rk --Ссылка на ГО/филиал
        ,d.product_rk -- Ссылка на продукт
        ,p.product_name -- Наименование продукта
        ,d.deal_type_cd -- Наименование типа сделки
	FROM rd.deal_info d
	LEFT JOIN rd.loan_holiday lh ON 1=1
	                             AND d.deal_rk = lh.deal_rk
	                             AND d.effective_from_date = lh.effective_from_date
	LEFT JOIN rd.product p ON p.product_rk = d.product_rk
						   AND p.effective_from_date = d.effective_from_date
						   AND p.product_name = d.deal_name
),
t_deal_rk AS (
	SELECT deal_rk
	FROM loan_holiday_info_v2
	GROUP BY effective_from_date, deal_rk
	HAVING COUNT(1) > 1
)
SELECT *
FROM loan_holiday_info_v2 l 
WHERE l.deal_rk IN (SELECT deal_rk FROM t_deal_rk);






CALL rd.fill_loan_holiday_info();

















