from psycopg2 import connect, Error
import logging
import sys
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

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
        url = 'postgresql+psycopg2://developer:12345678@127.0.0.1:5432/dwh'
        engine = create_engine(url)
        logging.info('The database connection is open.')
        df = pd.read_csv(csv_filename, sep=',', dtype=str, encoding = "utf-8")
        df_filtered = df[df['effective_from_date'].isin(['2023-01-01', '2023-08-11'])]
        df_filtered.to_sql(
            name='product',
            schema='rd',
            con=engine,
            index=False,
            if_exists='append'
        )
        logging.info(f'The data is loaded into the table product.')
    except OSError as error:
        logging.error(f'Operating system error: {error}')
    except SQLAlchemyError as error:
        logging.error(f'Database error: {error}')
    finally:
        logging.info('The database connection is close.')

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        filename="csv_to_product.log",
        format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.info(f'SCRIPT STARTED')
    try:
        main()
    except Exception as error:
        logging.error(f"Unexpected error: {error}")       
    finally:
        logging.info(f'SCRIPT FINISHED')