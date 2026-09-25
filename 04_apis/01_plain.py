# 01 - The happy path. No error handling at all.
# Break it: misspell USER, or turn off Wi-Fi, and read the traceback.

import httpx
import json

USER = "schaconxyz"
URL = "https://api.github.com/users/{user}/events/public"

#can name whatever you want (doesn't have to be response)
#uses httpx package to pull url and replaces url with string we defined

response = httpx.get(URL.format(user=USER))

#prints it all out
data = response.json()

for item in data:
  print(item["repo"]["name"]," - ", item["type"])
