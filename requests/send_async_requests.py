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
    
    urls = []
    if args.url:
        urls = args.url
            
    elif args.infile:
        if not os.path.isfile(args.infile):
            sys.stderr.write('Invalid file specified: {:s}\n'.format(args.infile))
            return 1
        else:
            try:
                fid = open(args.infile, 'r')
                urls = fid.readlines()
            except IOError as e:
                sys.stderr.write(e)
                return 1    
        
    if not urls:
        sys.stderr.write('No url(s) or file specified\n')
    
    json_destination = os.curdir
    if args.dest:
        json_destination = args.dest
        
    json_destination = os.path.realpath(json_destination)
    if not os.path.exists(json_destination):
        sys.stderr.write('Invalid json destination directory: {:s}\n'.format(json_destination))
        return 1
        
    #sys.stdout.write('Request JSON destination: {:s}\n'.format(json_destination))
    
    failed_requests = []
    for url in urls:
        
        request_url = url.strip()
        
        if args.verbose:
            sys.stdout.write('Sending request: {:s}\n'.format(request_url))
        
        response = send_async_request(url)
        if not response['status']:
            failed_requests.append(response)
            continue
            
        fname = '{:s}-{:s}.request.json'.format(response['stream'],
            datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S.%s'))
        
        response_json_file = os.path.join(json_destination, fname)
        try:
            sys.stdout.write('Writing response: {:s}\n'.format(response_json_file))
            fid = open(response_json_file, 'w')
            json.dump(response, fid)
            fid.close()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue
    
    # Write the array of failed request responses to a separate file    
    if failed_requests:
        exit_code = 1
        if args.infile:
            (infile_path, infile_name) = os.path.split(args.infile)
            (infile_fname, ext) = os.path.splitext(infile_name)
            failed_fname = '{:s}.failed.json'.format(infile_fname)
        else:
            failed_fname = 'requests-{:s}.failed.json'.format(
                datetime.datetime.utcnow().strftime('%Y%d%mT%H%M%S.%s'))
            
        out_file = os.path.join(json_destination, failed_fname)
        sys.stdout.write('Writing failed requests: {:s}\n'.format(out_file))
        try:
            fid = open(out_file, 'w')
            json.dump(failed_requests, fid)
            fid.close()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
                
    return exit_code

def send_async_request(url, debug=False):
    '''Validate and send the UFrame request url.'''
    
    request_url = url.strip()
    
    response = {'requestUrl' : request_url,
        'status' : False,
        'status_code' : -1,
        'response' : None,
        'reason' : None,
        'stream' : {},
        'reference_designator' : None}
    
    # Parse the request url and grab everything after /sensor/inv/ up to the query (?)
    request_regexp = re.compile('^https?:\/\/.*\/sensor\/inv\/(.*)\?')
    match = request_regexp.match(url)
     
    # Match required   
    if not match:
        response['reason'] = 'Badly Formatted Request'
        return response
    
    # A properly formatted UFrame request url will split into 5 pieces    
    request_tokens = match.groups()[0].split('/')
    if len(request_tokens) != 5:
        response['reason'] = 'Badly Formatted Request'
        return response
    
    # Create the stream name from the 5 tokens
    response['reference_designator'] = '-'.join(request_tokens[:3])
    response['stream'] = '-'.join(request_tokens)
    response['instrument'] = {'subsite' : request_tokens[0],
        'node' : request_tokens[1],
        'sensor' : request_tokens[2],
        'telemetry' : request_tokens[3],
        'stream' : request_tokens[4]}
    
    if debug:
        return response
        
    try:
        r = requests.get(request_url)
    except requests.exceptions.RequestException as e:
        sys.stderr.write('{:s}\n'.format(e))
        response['reason'] = e
        return response
    
    response['status_code'] = r.status_code
    response['reason'] = r.reason
        
    if r.status_code != 200:
        return response
    
    # Decode the json UFrame response    
    try:
        response['response'] = r.json()
        response['status'] = True
    except requests.exceptions.ValueError as e:
        response['reason'] = e
        
    return response
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('url',
        nargs='*',
        help='One or more UFrame asynchronous request url(s)')
    arg_parser.add_argument('-f', '--file',
        dest='infile',
        help='A text file containing one or more newline separated asynchronous UFrame stream requests')
    arg_parser.add_argument('-d', '--destination',
        dest='dest',
        help='Directory for writing UFrame json responses')
    arg_parser.add_argument('-v', '--verbose',
        dest='verbose',
        action='store_true',
        help='Print request status to STDOUT')

    parsed_args = arg_parser.parse_args()
    
    #print vars(parsed_args)
    #sys.exit(13)

    sys.exit(main(parsed_args))
