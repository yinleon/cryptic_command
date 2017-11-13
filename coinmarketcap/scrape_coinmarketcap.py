import os
import re
import datetime
import requests

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup


root_dir = '.'
table_url = 'https://coinmarketcap.com/all/views/all/'
table_id = 'currencies-all'
today = datetime.datetime.now()

col_name_w_currency = {
    'Market Cap' : 'market_cap_usd',
    'Price' : 'price_usd',
    'Volume (24h)': 'volume_24h_usd',
}

replace_symbols = {
    r'  [*]' : '',    # two spaces and any number of asterix
    r'[\$,%*]' : '',  # money signs, commas, percent signs, asterix
    r'[?]' : np.nan,  # question marks bec
    'Low Vol' : 0,    # low volume is simplified as zero...
}


def create_filename(root_dir, today):
    '''
    This function creates the filename, 
    it also creates the directory for the file if the directory doesn't exist.
    '''
    f_template = '{year}/{month}/{day}/{hour}/market_cap_USD_{time}.csv.gz'
    f = f_template.format(year = today.year,
                          month= today.month,
                          day  = today.day,
                          hour = today.strftime('%H'),
                          time = today.strftime('%H:%M:%S'))
    
    f_out = os.path.join(root_dir, f)
    
    dir_out = '/'.join(f_out.split('/')[:-1])
    if not os.path.exists(dir_out):
        os.makedirs(dir_out, exist_ok=True)
    
    return f_out


def clean_up_col(col):
    '''
    Adds currency to relevant the column names,
    removes spaces for underscores, 
    replaces % symbols for percent change,
    and returns the new column in lower case.
    '''
    col = col_name_w_currency.get(col, col)
    col = col.replace(' ', '_')
    col = col.replace('%', 'percent_change')
    
    return col.lower()


def is_minable(row):
    '''
    Check if `circulating_supply` contains an asterix.
    This function operates on each row of the dataframe.
    If the ICO is not minable, we'll find an asterix and return 0.
    
    Note:
    That when we apply a function across a row,
    the entire row is treated as a key-value pair.
    '''
    circulating_supply = row['circulating_supply']
    if '*' in circulating_supply:
        return 0
    
    else:
        return 1

    
def main():
    file = create_filename(root_dir, today)
    if not os.path.exists(file):
        r = requests.get(table_url)
        soup = BeautifulSoup(r.content, 'lxml')
        html_tbl = str(soup.find('table',{'id': table_id}))
        df = pd.read_html(html_tbl, index_col=0)[0]
        
        df.columns = [clean_up_col(c) for c in df.columns]
        df['scrape_timestamp'] = today
        df['is_minable'] = df.apply(is_minable, axis=1)
        
        df.replace(replace_symbols, regex=True, inplace=True)
        
        df.to_csv(file, index=None)

        
if __name__ == "__main__":
    main()