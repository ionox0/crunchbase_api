import time
import concurrent.futures
import requests
from pprint import pprint

CATEGORIES_URL = 'https://api.crunchbase.com/v/3/categories'
ORGANIZATIONS_URL = 'https://api.crunchbase.com/v/3/organizations'
KEY = 'eacbc749ec28b08ddc8cd1d0a1846973'
RATE_LIMIT_FACTOR = 5

# Fetch list of all categories
def get_categories():
  return requests.get(CATEGORIES_URL + '?user_key=' + KEY).json()['data']['items']

# Fetch & paginate through all organizations in a category
def get_organizations_for_category(category):
  url = ORGANIZATIONS_URL + '?category_uuids=' + category['uuid'] + '&user_key=' + KEY
  organizationsList = []
  page = 1
  while 1==1:
    print('getting organizations page ' + str(page) + ' for category ' + category['properties']['name'])
    try:
      response = requests.get(url + '&page=' + str(page))
      organizations = response.json()
      for organization in organizations['data']['items']:
        organizationsList.append(organization)
    except Exception as e:
      print('Error requesting ' + category['properties']['name'] + ' page ' + str(page) + '\n' + str(response.content))
      time.sleep(RATE_LIMIT_FACTOR)
      continue # Retry the request
    page += 1
    if page > organizations['data']['paging']['number_of_pages']:
      break
  return organizationsList, organizations['data']['paging']['number_of_pages']  # Return the organizations, and the number of pages (equal to # of requests sent)

# For each category, fetch all organizations, write category-organization mapping to file
def run(categories):
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    with open('categories_organizations.txt', 'w') as outputFile:
      ORGANIZATIONS_SUBTOTAL = 0
      REQUESTS_SUBTOTAL = 0
      # Start the load operations and mark each future with its URL
      future_to_url = {executor.submit(get_organizations_for_category, category): category for category in categories}
      for future in concurrent.futures.as_completed(future_to_url):
        category = future_to_url[future]
        ORGANIZATIONS_SUBTOTAL += category['properties']['organizations_in_category']
        print('Organizations subtotal: ' + str(ORGANIZATIONS_SUBTOTAL))
        try:
          data = future.result()
          organizations = data[0]
          REQUESTS_SUBTOTAL += data[1]
          print('Requests subtotal: ' + str(REQUESTS_SUBTOTAL))
          for company in organizations:
            outputFile.write(str(category['uuid'] + ', ' + company['uuid'] + '\n'))
        except Exception as exc:
          print('%r generated an exception: %s' % (category, exc))

def main():
  start = time.time()
  categories = get_categories()
  run(categories)
  #print('Total organizations: ' + TOTAL_ORGANIZATIONS);
  print('time: ' + str(time.time() - start))

if __name__ == "__main__":
  main()