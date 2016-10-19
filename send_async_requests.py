#!/usr/bin/env python

import os
import sys
import json
import argparse
import datetime
import re
from UFrame import UFrame

def main(args):
    '''Send one or more asynchronous UFrame requests and write the JSON responses
    to the current working directory.  All urls prefixed with a # are ignored'''
    
    request_urls = args.request_urls
    if not request_urls and args.file:
        if not os.path.isfile(args.file):
            sys.stderr.write('Invalid request urls file specified: {:s}\n'.format(args.file))
            return 1
            
        try:
            with open(args.file, 'r') as fid:
                request_urls = fid.readlines()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(args.file))
            return 1
            
    if not request_urls:
        sys.stderr.write('No request urls specified\n')
        return 1
        
    json_destination = args.destination
    if not json_destination:
        json_destination = os.path.realpath(os.curdir)
        
    if not os.path.isdir(json_destination):
        sys.stderr.write('Invalid JSON response destination: {:s}\n'.format(json_destination))
        return 1
        
    # Regex to capture the UFrame base url from each request url
    http_regex = re.compile('(http|ftp|https)://([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?')

    uframe = None
    for url in request_urls:
        
        # Remove any trailing whitespace from the request
        url = url.strip()
        
        if url.startswith('#'):
            continue
            
        # Pull the UFrame base_url out of the request
        match = http_regex.match(url)
        if not match:
            sys.stderr.write('Cannot determine UFrame base URL from request URL: {:s}\n'.format(url))
            continue
        
        # Create the UFrame base url
        uframe_base_url = '://'.join(match.groups()[:2])
            
        # Create the UFrame client instance with the base url if it's different from
        # the previous request
        if not uframe or uframe.base_url != uframe_base_url:
            uframe = UFrame(base_url=uframe_base_url,
                timeout=args.timeout)
                
        # Send the request
        uframe_response = uframe.send_async_requests(url)
        
        # We're only sending one request, so are only receiving one response
        response = uframe_response[0]
        if response['status_code'] != 200:
            sys.stderr.write('Request failed: {:s}\n'.format(response['reason']))
        
        fname = '{:s}-{:s}.request.json'.format(response['stream'],
            datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S.%s'))
        
        response_json_file = os.path.join(json_destination, fname)
        try:
            if args.verbose:
                sys.stdout.write('Writing response: {:s}\n'.format(response_json_file))
                
            fid = open(response_json_file, 'w')
            json.dump(response, fid)
            fid.close()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            
    return
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('request_urls',
        nargs='*',
        help='A list of whitespace separated asynchronous UFrame request urls')
    arg_parser.add_argument('-f', '--file',
        help='Filename containing the list of whitespace separated asynchronous UFrame request urls')
    arg_parser.add_argument('-d', '--destination',
        help='Alternate location for writing response JSON files')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the UFrame request timeout, in seconds <Default:120>')
    arg_parser.add_argument('-v', '--verbose',
        help='Print the send status of each request')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))