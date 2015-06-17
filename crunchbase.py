import time
import sys
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
    ORGANIZATIONS_SUBTOTAL = 0
    REQUESTS_SUBTOTAL = 0
    if len(sys.argv) > 1:
      START_INDEX = next(index for (index, d) in enumerate(categories) if d['properties']['name'] == sys.argv[1])
    else:
      START_INDEX = 0
    # Start the load operations and mark each future with its category
    future_to_category = {executor.submit(get_organizations_for_category, category): category for category in categories [START_INDEX: ]}
    for future in concurrent.futures.as_completed(future_to_category):
      category = future_to_category[future]
      ORGANIZATIONS_SUBTOTAL += category['properties']['organizations_in_category']
      print('Organizations subtotal: ' + str(ORGANIZATIONS_SUBTOTAL))
      try:
        data = future.result()
        organizations = data[0]
        REQUESTS_SUBTOTAL += data[1]
        print('Requests subtotal: ' + str(REQUESTS_SUBTOTAL))
        for company in organizations:
          printDataToFile(category, company)
      except Exception as exc:
        print('%r generated an exception: %s' % (category, exc))

def printDataToFile(category, company):
  with open('categories_organizations.txt', 'a') as outputFile:
    outputFile.write('{0} \t {1} \t {2} \t {3} \t {4} \t {5} \t {6} \t {7} \t {8} \t {9} \t {10} \t {11} \t {12} \n'.format(
      category['uuid'],
      category['properties']['name'],
      company['uuid'],
      company['properties']['name'],
      company['properties']['twitter_url'],
      company['properties']['linkedin_url'],
      company['properties']['facebook_url'],
      company['properties']['profile_image_url'],
      company['properties']['city_name'],
      company['properties']['homepage_url'],
      company['properties']['domain'],
      company['properties']['short_description'],
      company['properties']['country_code'],
      company['properties']['region_name']))

def main():
  start = time.time()
  categories = get_categories()
  run(categories)
  print('time: ' + str(time.time() - start))

if __name__ == "__main__":
  main()