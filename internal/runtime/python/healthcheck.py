import sys
from http.client import HTTPConnection

port = int(sys.argv[1])
path = sys.argv[2]
timeout = float(sys.argv[3])
try:
    conn = HTTPConnection("127.0.0.1", port, timeout=timeout)
    conn.request("GET", path)
    response = conn.getresponse()
    response.read()
    conn.close()
    sys.exit(0 if 200 <= response.status < 400 else 1)
except Exception:
    sys.exit(1)
