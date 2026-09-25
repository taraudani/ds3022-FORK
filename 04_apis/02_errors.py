# 01 - The happy path. No error handling at all.
# Break it: misspell USER, or turn off Wi-Fi, and read the traceback.

import httpx
import json

<<<<<<< HEAD
USER = "schacon"
URL = "https://api.github.com/users/{user}/events/public"

#can name whatever you want (doesn't have to be response)
#uses httpx package to pull url and replaces url with string we defined

try:
  response = httpx.get(URL.format(user=USER))
  #evaluates if response status is good or bad
  response.raise_for_status()
  #prints it all out
  data = response.json()

  for item in data:
    print(item["repo"]["name"]," - ", item["type"])

except httpx.HTTPerror as e:
  print(e)
