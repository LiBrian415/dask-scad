import cloudpickle
import sys
import redis


def fetch(h, p, k):
    r = redis.Redis(host=h, port=p)
    print(cloudpickle.loads(r.get(k)))


if __name__ == '__main__':
    host = '127.0.0.1'
    port = 6379
    key = sys.argv[1]
    fetch(host, port, key)
