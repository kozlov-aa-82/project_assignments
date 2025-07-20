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
            user="developer",
            password="12345678",
            host="127.0.0.1",
            port="5432",
            database="dwh"
        )
        logging.info('The database connection is open.')
        cursor = connection.cursor()
        sql = "COPY rd.deal_info FROM STDIN DELIMITER ',' CSV HEADER"
        with open(csv_filename, "r") as f:
            cursor.copy_expert(sql, f)
        connection.commit()
        logging.info(f'The data is loaded into the table csv_deal_info.')
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
        filename="csv_deal_info.log",
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(f'SCRIPT STARTED')
    try:
        main()
    except Exception as error:
        logging.error(f"Unexpected error: {error}")       
    finally:
        logging.info(f'SCRIPT FINISHED')
