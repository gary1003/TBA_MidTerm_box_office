import pandas as pd
import requests
from bs4 import BeautifulSoup
import asyncio
from aiohttp import ClientSession
import time

latest_movie_id = 963268

api_key = '71e2d5d46670a878e72d4f18fc6235b6'
url_head = 'https://api.themoviedb.org/3/movie/'
links = [f'{url_head}{id}?api_key={api_key}' for id in range(latest_movie_id, 0, -1)]

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
        columns = ['adult', 'backdrop_path', 'belongs_to_collection', 'budget', 'genres',
            'homepage', 'id', 'imdb_id', 'original_language', 'original_title',
            'overview', 'popularity', 'poster_path', 'production_companies',
            'production_countries', 'release_date', 'revenue', 'runtime',
            'spoken_languages', 'status', 'tagline', 'title', 'video',
            'vote_average', 'vote_count']
        df = pd.DataFrame(columns=columns)
        for json in jsons:
            if json == 'bad':
                continue
            df = df.append(pd.DataFrame.from_dict(json, orient='index').T, ignore_index=True)
            # if(df.shape[0] % 100 == 0):
            #     print(f'{df.shape[0]} movies scraped')
        df.to_csv('data/movies.csv', index=False)

            

if __name__ == '__main__':
    time_s = time.time()
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    