import os
import requests
import pandas as pd
from datetime import date
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build

ROOT = os.path.dirname(__file__)

API_SERVICE_NAME = 'webmasters'
API_VERSION = 'v3'
SCOPE = [
    'https://www.googleapis.com/auth/webmasters.readonly'
]

pd.set_option('max_colwidth', 100)

def get_sitemap_links(url, all_links = []):
    response = requests.get(url)
    if response.status_code == 200:
        try:

            soup = BeautifulSoup(response.text, 'xml')
            links = [loc.text for loc in soup.find_all('loc') if 'wp-content' not in loc.text]

        except:
            return
        
        else:

            for link in links:
                if link[-3:] == 'xml':
                    get_sitemap_links(link, all_links)
                else:
                    all_links.append(link)
            
            return all_links
    else:
        print(f'Request failed with status code {response.status_code}')
        return
    
def auth_service(credentials_path):

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=SCOPE
    )

    service = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)

    return service

def query(service, url, payload):
    response = service.searchanalytics().query(siteUrl=url, body=payload).execute()

    results = []

    for row in response['rows']:
        data = {}

        for i in range(len(payload['dimensions'])):
            data[payload['dimensions'][i]] = row['keys'][i]

        data['clicks'] = row['clicks']
        data['impressions'] = row['impressions']
        data['ctr'] = round(row['ctr'] * 100, 2)
        data['position'] = round(row['position'], 2)

        results.append(data)
    
    return pd.DataFrame.from_dict(results)

if __name__ == '__main__':
    
    url = 'https://ak-codes.com/sitemap_index.xml'

    sitemap_links = get_sitemap_links(url)
    df_sitemap = pd.DataFrame(sitemap_links, columns=['page'])

    print('Total sitemap links:', len(sitemap_links))
    print(df_sitemap.head(20))

    payload = {
        'startDate': '2023-01-01',
        'endDate': date.today().strftime('%Y-%m-%d'),
        'dimensions': ['page'],
        'rowLimit': 10000,
        'startRow': 0
    }

    service = auth_service(os.path.join(ROOT, 'credentials.json'))

    df_gsc = query(service, service.sites().list().execute()['siteEntry'][0]['siteUrl'], payload)
    print(df_gsc.head(20))

    df_merged = pd.merge(df_gsc, df_sitemap, how='right', on=['page'])
    print(df_merged.head(20))

    print(df_sitemap.shape, df_gsc.shape, df_merged.shape)

    df_no_clicks = df_merged[df_merged['clicks'] < 1]
    df_no_clicks = df_no_clicks.sort_values(by='impressions', ascending=False)
    print(df_no_clicks)

    gsc_links = df_gsc['page'].tolist()
    all_links = list(set(sitemap_links + gsc_links))
    print('Total links:', len(all_links))

    shared_links = list(set(sitemap_links).intersection(set(gsc_links)))
    print('Total shared links:', len(shared_links))

    # links in sitemap but not in Google Search Console - non-ranking pages
    not_indexed = list(set(all_links).difference(set(gsc_links)))
    print('Total not indexed pages:', len(not_indexed))

    df_not_indexed = pd.DataFrame(not_indexed, columns=['page'])
    print(df_not_indexed)
    df_not_indexed.to_csv(os.path.join(ROOT, 'not indexed.csv'))

    # links in Google Search Console but not in sitemap - index bloat
    index_bloat = list(set(all_links).difference(set(sitemap_links)))
    print('Total index bloat pages:', len(index_bloat))
    df_index_bloat = pd.DataFrame(index_bloat, columns=['page'])
    print(df_index_bloat)
    df_index_bloat.to_csv(os.path.join(ROOT, 'index bloat.csv'))



