import aiohttp
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import asyncio
from aiohttp import ClientSession, ClientTimeout
from time import time

df_movies = pd.read_csv('data/movies_final.csv').head(1000)
url_head = 'http://www.imdb.com/title/'
ids = df_movies['imdb_id'].tolist()
links = [f'{url_head}{id}/' for id in ids]
headers = {
    "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3538.102 Safari/537.36 Edge/18.19582"
}


async def fetch(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            return 'bad'
        return await response.text()

async def fetch_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(fetch(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def main():    
    
    async with ClientSession(headers=headers) as session:
        opening_box_office = []
        soups = await fetch_all(session, links)
        for soup in soups:
            if soup == 'bad':
                opening_box_office.append(np.nan)
                continue
            try:
                soup = BeautifulSoup(soup, 'html5lib')
                box_office = soup.findAll(
                    'span', attrs={'class': 'ipc-metadata-list-item__list-content-item'})[6].text
                box_office = int(box_office[1:].replace(',', ''))
                opening_box_office.append(box_office)
            except:
                opening_box_office.append(np.nan)
        df_movies['opening_box_office'] = opening_box_office
        df_movies.dropna(subset=['opening_box_office'], inplace=True)
        print(df_movies.shape)
        df_movies.to_csv('data/movies_final_box1.csv', index=False)
            

if __name__ == '__main__':
    time_s = time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    print(f'{time() - time_s} seconds')
    