from psycopg2 import connect, Error
import logging
import sys
import datetime


def get_query_string(dt=None):
    condition = ''
    if dt:
        condition = f"WHERE from_date = '{dt.strftime('%Y-%m-%d')}'::DATE - INTERVAL '1 month'"
    query_string = f'''
        SELECT from_date, to_date, chapter, ledger_account, characteristic,
                balance_in_rub, balance_in_val, balance_in_total,
                turn_deb_rub, turn_deb_val, turn_deb_total,
                turn_cre_rub, turn_cre_val, turn_cre_total,
                balance_out_rub, balance_out_val, balance_out_total
        FROM dm.dm_f101_round_f {condition}
        ORDER BY from_date DESC, ledger_account
    '''
    return query_string

def main():
    if len(sys.argv) == 1:
        query_string = get_query_string()
        filename = 'dm_f101_round_f.csv'
        logging.info('Getting all reports')
    elif len(sys.argv) == 2:
        try:
            dt = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d')
            logging.info(f'Getting reports for a specific date - <{dt.strftime('%Y-%m-%d')}>')
            query_string = get_query_string(dt)
            filename = f'dm_f101_round_f_{dt.strftime('%Y-%m-%d')}.csv'
        except ValueError:
            logging.warning(f'The parameter value is not a date. - <{sys.argv[1]}>')  
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
        cursor = connection.cursor()
        sql = "COPY ({}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(query_string)
        with open(filename, 'w') as f:
            cursor.copy_expert(sql, f) 
        logging.info(f'Query results exported to {filename}!')
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
        filename="dm_f101_round_f_to_csv.log",
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(f'SCRIPT STARTED')
    try:
        main()
    except Exception as error:
        logging.error(f"Unexpected error: {error}")       
    finally:
        logging.info(f'SCRIPT FINISHED')