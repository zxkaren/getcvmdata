from decouple import config


API_URL = config("CVM_SRE_API_URL", default="")
REQUEST_TIMEOUT = config("CVM_SRE_REQUEST_TIMEOUT", default=30, cast=int)
PAGE_SIZE = config("CVM_SRE_PAGE_SIZE", default=100, cast=int)
