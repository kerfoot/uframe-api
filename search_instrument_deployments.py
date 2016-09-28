#!/usr/bin/env python

import os
import sys
import argparse
import json
import csv
from UFrame import UFrame

def main(args):
    '''Display all deployment events for the full or partially qualified
    reference designator.  A reference designator uniquely identifies an
    instrument.'''
    
    base_url = args.base_url
    if not base_url:
        sys.stderr.write('No UFrame url specified.  Checking UFRAME_BASE_URL environment variable\n')    
        base_url = os.getenv('UFRAME_BASE_URL')
        
    if not base_url:
        sys.stderr.write('No UFrame instance specified')
        sys.stderr.flush()
        return 1
    
    # Create a UFrame instance    
    uframe = UFrame(base_url=base_url,
        timeout=args.timeout)
        
    events = uframe.search_instrument_deployments(args.reference_designator,
        ref_des_search_string=args.filter,
        status=args.status)
    
    if not events:
        sys.stderr.write('No events found for reference designator: {:s}\n'.format(args.reference_designator))
        
    if args.json:
        sys.stdout.write('{:s}\n'.format(json.dumps(events)))
        return 0
        
    if not events:
        return 0
        
    csv_writer = csv.writer(sys.stdout)
    cols = ['reference_designator',
        'deployment_number',
        'active',
        'event_start_ts',
        'event_stop_ts',
        'event_start_ms',
        'event_stop_ms',
        'valid_event']
    csv_writer.writerow(cols)
    for event in events:
        csv_writer.writerow([event['instrument']['reference_designator'],
            event['deployment_number'],
            event['active'],
            event['event_start_ts'],
            event['event_stop_ts'],
            event['event_start_ms'],
            event['event_stop_ms'],
            event['valid']])
        
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('-s', '--status',
        dest='status',
        type=str,
        default='all',
        choices=['active', 'inactive', 'all'],
        help='Specify the status of the deployment <default=\'all\'>')
    arg_parser.add_argument('-f', '--filter',
        dest='filter',
        type=str,
        help='A string used to filter reference designators')
    arg_parser.add_argument('-j', '--json',
        dest='json',
        action='store_true',
        help='Print deployment events as a JSON object')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Value is taken from the UFRAME_BASE_URL environment variable, if set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds (Default is 120 seconds).')
            
    parsed_args = arg_parser.parse_args()
    #print parsed_args
    #sys.exit(13)

    sys.exit(main(parsed_args))
