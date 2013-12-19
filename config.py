# coding: utf

PROXIES_API_URL = 'http://localhost:8000/api/proxies/?format=json&' \
                  'count=%(count)d&' \
                  'country_code=%(country_code)s'
PROXY_USE_LIMIT = 20

QUEUE_BACKEND = 'memory'

CACHE_ENABLED = False
CACHE_DATABASE = 'parsers-cache'
