#!/usr/bin/env python

import argparse
import sys
import os
import requests
import re
import datetime
import json

def main(args):
    '''Validate and send one or more asynchronous UFrame requests.  The JSON 
    response for each request is written to a .json file in the current working
    directory.  Response objects for any failed requests are written to a timestamped
    requests-YYYYmmddTHHMMSS.ssss.failed.json file in the current working directory'''
    
    exit_code = 0
    
    endpoint_urls = []
    if not args.json_files:
        sys.stderr.write('No UFrame asynchronous json response files specified\n')
        return 1
    
    for json_file in args.json_files:
        try:
            fid = open(json_file, 'r')
            response = json.load(fid)
            fid.close()
        except IOError as e:
            sys.stderr.write(e)
            continue   
    
        if type(response) != dict:
            sys.stderr.write('Invalid JSON response object: {:s}\n'.format(json_file))
            continue
        
        response_keys = response.keys()
        if 'status' not in response_keys:
            sys.stderr.write('JSON response object missing status key: {:s}\n'.format(json_file))
            continue
        elif 'response' not in response_keys or type(response['response']) != dict:
            sys.stderr.write('JSON response object missing respone dict: {:s}\n'.format(json_file))
            continue
        elif 'allURLs' not in response['response'].keys():
            sys.stderr.write('JSON respone object missing allURLs key: {:s}\n'.format(json_file))
            continue
            
        # Find all urls that do not contain 'thredds'
        async_urls = [url for url in response['response']['allURLs'] if url.find('thredds') == -1]
        if not async_urls:
            sys.stderr.write('No async result URL found: {:s}\n'.format(json_file))
            continue
            
        for async_url in async_urls:
            endpoint_urls.append(async_url)
            
    for url in endpoint_urls:
        sys.stdout.write('{:s}\n'.format(url))
                
    return exit_code
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('json_files',
        nargs='*',
        help='One or more UFrame asynchronous json response file(s)')

    parsed_args = arg_parser.parse_args()
    
    #print vars(parsed_args)
    #sys.exit(13)

    sys.exit(main(parsed_args))