from psycopg2 import connect, Error
import logging
import sys
import os


def main():
    if len(sys.argv) == 1:
        logging.warning('The file name parameter is missing!')
        return
    elif len(sys.argv) == 2:
        csv_filename = sys.argv[1]
        if not os.path.exists(csv_filename):
            logging.warning(f'The file <{csv_filename}> does not exist!')            
            return
    else:
        logging.warning(f'There are too many parameters. - <{sys.argv[1:]}>')  
        return
    
    try:
        connection = connect(
            user="user_data",
            password="12345678",
            host="127.0.0.1",
            port="5432",
            database="project_db"
        )
        logging.info('The database connection is open.')
        connection.autocommit = False
        cursor = connection.cursor()
        cursor.execute('CREATE TEMP TABLE tmp ON COMMIT DROP AS TABLE dm.dm_f101_round_f_v2 WITH NO DATA')
        sql = "COPY tmp FROM STDIN DELIMITER ';' CSV HEADER"
        with open(csv_filename, "r") as f:
            cursor.copy_expert(sql, f)
        cursor.execute('''
            INSERT INTO dm.dm_f101_round_f_v2(
                from_date, to_date, chapter, ledger_account, characteristic,
                balance_in_rub, balance_in_val, balance_in_total,
                turn_deb_rub, turn_deb_val, turn_deb_total,
                turn_cre_rub, turn_cre_val, turn_cre_total,
                balance_out_rub, balance_out_val, balance_out_total
            )
            SELECT * FROM tmp
            ON CONFLICT ON CONSTRAINT dm_f101_round_f_v2_pkey
            DO UPDATE 
                SET chapter = EXCLUDED.chapter, characteristic = EXCLUDED.characteristic,
                    balance_in_rub = EXCLUDED.balance_in_rub, balance_in_val = EXCLUDED.balance_in_val, balance_in_total = EXCLUDED.balance_in_total,
                    turn_deb_rub = EXCLUDED.turn_deb_rub, turn_deb_val = EXCLUDED.turn_deb_val, turn_deb_total = EXCLUDED.turn_deb_total,
                    turn_cre_rub = EXCLUDED.turn_cre_rub, turn_cre_val = EXCLUDED.turn_cre_val, turn_cre_total = EXCLUDED.turn_cre_total,
                    balance_out_rub = EXCLUDED.balance_out_rub, balance_out_val = EXCLUDED.balance_out_val, balance_out_total = EXCLUDED.balance_out_total
        ''')
        connection.commit()
        logging.info(f'The data is loaded into the table dm_f101_round_f_v2.')
    except OSError as error:
        logging.error(f'Operating system error: {error}')
    except Error as error:
        logging.error(f'Database error: {error}')
    finally:
        if connection:
            cursor.close()
            connection.close()
            logging.info('The database connection is close.')

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        filename="csv_to_dm_f101_round_f_v2.log",
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(f'SCRIPT STARTED')
    try:
        main()
    except Exception as error:
        logging.error(f"Unexpected error: {error}")       
    finally:
        logging.info(f'SCRIPT FINISHED')