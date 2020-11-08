import requests
import sys

url = sys.argv[1]
out_file = sys.argv[2]

r = requests.get(url, allow_redirects=True)
open(out_file, "wb").write(r.content)