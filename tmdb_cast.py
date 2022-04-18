import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import asyncio
from aiohttp import ClientSession
from time import time

df_movies = pd.read_csv('data/movies_with_revenue_opening_box_office.csv', lineterminator='\n')
ids = df_movies['id'].tolist()

api_key = '71e2d5d46670a878e72d4f18fc6235b6'
url_head = 'https://api.themoviedb.org/3/movie/'
links = [f'{url_head}{id}/credits?api_key={api_key}' for id in ids]

async def fetch(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            return 'bad'
        return await response.json()

async def fetch_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(fetch(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def main():    
    
    async with ClientSession() as session:
        jsons = await fetch_all(session, links)
        casts = []
        for json in jsons:
            if json == 'bad':
                casts.append([])
                continue
            try:

                casts.append(json['cast'])
            except:
                casts.append([])
        df_movies['cast'] = casts
        df_movies.to_csv('data/movies_casts.csv', index=False)

            

if __name__ == '__main__':
    time_s = time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    print(f'{time() - time_s} seconds')
    