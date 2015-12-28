#!/usr/bin/env python

import argparse
import json
import os
import sys
import datetime
from UFrame import UFrame

def main(args):
    '''Return the fully qualified reference designator list for all instruments
    producing the specified stream in the specified UFrame instance.  The stream may
    be a partial or complete stream name'''
    
    status = 0
    
    base_url = args.base_url
    if not base_url:
        if args.verbose:
            sys.stderr.write('No uframe_base specified.  Checking UFRAME_BASE_URL environment variable\n')
            
        base_url = os.getenv('UFRAME_BASE_URL')
        
    if not base_url:
        sys.stderr.write('No UFrame instance specified')
        sys.stderr.flush()
        return 1
    
    # Create a UFrame instance   
    if args.verbose:
        sys.stderr.write('Creating UFrame API instance\n')
         
    uframe = UFrame(base_url=base_url,
        timeout=args.timeout,
        validate=args.validate_uframe)
    
    # Fetch the table of contents from UFrame
    if args.verbose:
        t0 = datetime.datetime.utcnow()
        sys.stderr.write('Fetching and creating UFrame table of contents...')
        
    uframe.fetch_toc()
    
    if args.verbose:
        t1 = datetime.datetime.utcnow()
        dt = t1 - t0
        sys.stderr.write('Complete ({:d} seconds)\n'.format(dt.seconds))
    
    instruments = uframe.stream_to_instrument(args.stream_name)
        
    if args.json:
        json.dumps(instruments)
    else:
        for instrument in instruments:
            sys.stdout.write('{:s}\n'.format(instrument))
    
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('stream_name',
        help='Name of the stream to search')
    arg_parser.add_argument('-j', '--json',
        dest='json',
        action='store_true',
        help='Return response as json.  Default is ascii text.')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Value is taken from the UFRAME_BASE_URL environment variable, if set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds (Default is 120 seconds).')
    arg_parser.add_argument('-v', '--verbose',
        action='store_true',
        help='Verbose display')
    arg_parser.add_argument('--validate_uframe',
        action='store_true',
        help='Attempt to validate the UFrame instance <Default:False>')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
