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

    if not args.json_files:
        sys.stderr.write('No files specified\n')
        return 1

    root_location = ''
    if args.prefix:
        if not os.path.isdir(args.prefix):
            sys.stderr.write('Invalid destination specified: {:s}\n'.format(args.prefix))
            return 1
        root_location = args.prefix

    root_location = os.path.realpath(root_location)

    for json_file in args.json_files:
        try:
            fid = open(json_file, 'r')
            response = json.load(fid)
            fid.close()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
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
            if not args.short:
                endpoint_urls.append(async_url)
            else:
                tokens = async_url.split('/')
                loc = os.path.join(root_location, '/'.join(tokens[-2:]))
                endpoint_urls.append(loc)
            
    for url in endpoint_urls:
        sys.stdout.write('{:s}\n'.format(url))
                
    return exit_code
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('json_files',
        nargs='*',
        help='One or more UFrame asynchronous json response file(s)')
    arg_parser.add_argument('-s', '--short',
        dest='short',
        action='store_true',
        help='Create and display the user/product directory in the current working directory')
    arg_parser.add_argument('-p', '--prefix',
        dest='prefix',
        help='Used with -s, specify the directory location')

    parsed_args = arg_parser.parse_args()
    
#    print vars(parsed_args)
#    sys.exit(13)

    sys.exit(main(parsed_args))
