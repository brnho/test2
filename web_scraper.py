from bs4 import BeautifulSoup
import requests
import pandas as pd
from numba import njit, prange
import re
import time
import multiprocessing
import json
import pickle

def scrape(start, stop):
	data = {}
	xml = []
	for i in range(start, stop+1):
		if i % 50000 == 0:
			print(i) # monitor progress
		url = 'https://fraser.stlouisfed.org/metadata.php?type=item&id={}'.format(i)
		xml_data = requests.get(url).content
		soup = BeautifulSoup(xml_data, "xml")
		xml.append(xml_data)

		# Skip unwanted documents
		if soup.find('topic'):
			topic = soup.find_all('topic')[1].get_text()
		else:
			continue
		if topic not in ['National banks (United States)', 'State member banks']:
			continue
		abstract = soup.find('abstract').get_text()
		if 'Microfilm' in abstract:
			continue

		# Parse document
		try:
			location = soup.find('titleInfo').find('title').get_text()
			items = re.split('Country: |. Federal Reserve District: |. State: |. City: ', location)
			level = 'National' if 'National' in topic else 'State'
			sortDate = soup.find('sortDate').get_text()
			dateIssued = soup.find('dateIssued').get_text()
			pages = soup.find('extent').get_text().split(' pages')[0]
			web_url = soup.find('location').find('url').get_text()

			data[i] = []
			data[i].append(level)
			for j in range(1, 5):
				if j < len(items):
					data[i].append(items[j])
				else:
					data[i].append('NA') # city is missing
			data[i].append(sortDate)
			data[i].append(dateIssued)
			data[i].append(pages)
			data[i].append(web_url)
		except Exception as e:
			print(e)
	return data, xml


if __name__ == '__main__':
	num_processes = multiprocessing.cpu_count() # 8
	indices = []
	interval = (598597 - 1) // num_processes
	start = 2 
	for i in range(num_processes):
		indices.append((start, min(start + interval, 598597)))
		start = start + interval + 1
	pool = multiprocessing.Pool(processes=num_processes)
	results = pool.starmap(scrape, indices)

	with open('dict.json', 'w') as f:
		json.dump(results, f)






		import pickle

a = []
for i in range(200000, 200005):
    print(i)
    url = 'https://fraser.stlouisfed.org/metadata.php?type=item&id={}'.format(448952)
    xml_data = requests.get(url).content
    a.append(xml_data)

with open("test.txt", "wb") as fp:   #Pickling
    pickle.dump(a, fp)
