import argparse
import json
import math
import os
import requests
import sys
import time

# Disable annoying warning
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def load_wskprops():
    wskprops = os.path.join(os.path.expanduser('~'), '.wskprops')
    config = {}
    with open(wskprops, 'r') as f:
        for line in f.readlines():
            (key, value) = line.strip().split('=')
            config[key.upper()] = value
    return config

class WskProps:
    def __init__(self, host: str = None, auth: str = None):
        '''wsk CLI configuration variables
        '''
        props = None
        if host is None or auth is None:
            props = load_wskprops()
            if host is None:
                host = props['APIHOST']
            if auth is None:
                auth = props['AUTH']

        self.host = host
        split = auth.split(':')
        self.auth = (split[0], split[1])

    def __str__(self):
        return json.dumps({'host': self.host, 'auth': self.auth})



def read_example(filename: str):
    '''Read a JSON file from the examples directory
    '''
    def rf(path):
        with open(path, 'r') as f:
            return f.read()
    try:
        return rf(os.path.join(DIR_NAME, filename))
    except FileNotFoundError as e:
        return rf(filename)




DIR_NAME = os.path.dirname(__file__)


def send_request(req, verbose=False):
    '''send the request off to the API, printing additional information if
    verbosity is turned on
    '''
    prep = req.prepare()
    s = requests.Session()
    if verbose:
        print('request url: {}'.format(prep.url))
    if verbose:
        print('request content: {}'.format(req.data))
    resp = s.send(prep, verify=False)
    content = resp.content.decode('utf-8')
    if verbose:
        print("API responded with {}".format(resp.status_code))
    try:
        print(json.dumps(json.loads(content), indent=2))
    except json.JSONDecodeError as e:
        print(content)
    return resp



def do_get_activation(host, activation_id, auth, verbose=False):
    result = '{}/api/v1/namespaces/_/activations/{}'.format(host, activation_id)
    logs = '{}/api/v1/namespaces/_/activations/{}/logs'.format(host, activation_id)

    # send_request lambda with some params filled in
    sr = lambda url: send_request(requests.Request(url=url, method='GET', headers={'Content-Type': 'application/json'}, auth=auth), verbose=verbose)
    log_res = sr(logs)
    result_res = sr(result)
    return result_res

def do_action_update(host, method, json_content, app_name, auth, verbose=False, profile=False):
    url = '{}/api/v1/namespaces/_/actions/{}'.format(host, app_name)
    req = requests.Request(url=url, data=json_content,
        method=method, headers={'Content-Type': 'application/json'},
        params={'overwrite': 'true', 'profile': profile}, auth=auth)
    return send_request(req, verbose=verbose)

def main():
    parser = argparse.ArgumentParser('wskish')
    parser.add_argument('method', help="the type of change to make to the API: [put, delete, post, get].")
    parser.add_argument('id', help="The activation ID to query information on. Use in conjuction when [method] is `get`", nargs="?")
    parser.add_argument('--file', help="The example application to make as the argument to this request", default='application-example.json')
    parser.add_argument('--app', help='The name of the action to represent the application in openwhisk.', default='test-action')
    parser.add_argument('--host', help='The openwhisk controller host. This should at minimum be a host name, but the http(s) protocol may also be specified along with a port', default=None)
    parser.add_argument('--auth', help='The auth string to use against the openwhisk API', default=None)
    parser.add_argument('-n', '--non-blocking', help='Make a POST request non-blocking. If set, only the activation ID is returned', action='store_true')
    parser.add_argument('-p', '--profile', help="set flag to profile this activation", action='store_true')
    parser.add_argument('-t', '--timeout', help='timeout for blocking activation waiting for successful response', default=10)
    parser.add_argument('-v', '--verbose', help="enable to print debug logs", action='store_true')

    args = parser.parse_args()

    wskprops = WskProps(args.host, args.auth)

    json_content=None
    method = args.method.upper()
    if method == 'PUT':
        json_content = read_example(args.file)

    host = wskprops.host

    # default to https like wsk cli
    if not host.startswith('http'):
        host = 'https://' + host

    resp = None
    if method != 'GET':
        resp = do_action_update(host, method, json_content, args.app, wskprops.auth, verbose=args.verbose, profile=args.profile)
    elif method == 'GET' and args.id is None:
        print("ERROR: Must provide activation ID when using 'get'")
        sys.exit(1)
    else:
        do_get_activation(host, args.id, wskprops.auth, args.verbose)

    if method == 'POST':
        aid = json.loads(resp.content)['activationId']
        start = time.time()
        iters = 0
        while time.time() - start < args.timeout and not args.non_blocking:
            sleep_time = min(math.pow(2, iters), args.timeout)
            time.sleep(sleep_time)
            r = do_get_activation(host, aid, wskprops.auth, args.verbose)
            if r.status_code == 200:
                break
            iters += 1



if __name__ == "__main__":
    main()
