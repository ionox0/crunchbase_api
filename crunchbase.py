# Python 2 or 3?
import time
import concurrent.futures
import requests
from pprint import pprint

CATEGORIES_URL = 'https://api.crunchbase.com/v/3/categories'
ORGANIZATIONS_URL = 'https://api.crunchbase.com/v/3/organizations'
KEY = 'eacbc749ec28b08ddc8cd1d0a1846973'

# Fetch list of all categories
def get_categories():
  return requests.get(CATEGORIES_URL + '?user_key=' + KEY).json()['data']['items'][0:1]

# Fetch all organizations in a category
def get_organizations_for_category(category):
  url = ORGANIZATIONS_URL + '?category_uuids=' + category['uuid'] + '&user_key=' + KEY
  organizationsList = []
  page = 1
  while 1==1:
    organizations = requests.get(url + '&page=' + str(page)).json()
    for organization in organizations['data']['items']:
      organizationsList.append(organization)
    page += 1
    if page > organizations['data']['paging']['number_of_pages']:
      break
  return organizationsList

# For each category, fetch all organizations, write category-organization mapping to file
def run(categories):
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    with open('categories_organizations.txt', 'a') as outputFile:
      # Start the load operations and mark each future with its URL
      future_to_url = {executor.submit(get_organizations_for_category, category): category for category in categories}
      for future in concurrent.futures.as_completed(future_to_url):
        category = future_to_url[future]
        try:
          data = future.result()
          for company in data:
            outputFile.write(str(category['uuid'] + ', ' + company['uuid'] + '\n'))
        except Exception as exc:
          print('%r generated an exception: %s' % (category, exc))

def main():
  start = time.time()
  categories = get_categories()
  run(categories)
  print('time: ' + str(time.time() - start))

if __name__ == "__main__":
  main()