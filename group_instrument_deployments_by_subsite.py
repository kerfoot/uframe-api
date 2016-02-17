#!/usr/bin/env python

import argparse
import sys
import json
from UFrame import UFrame
from UFrame.Events import group_instrument_deployment_events_by_subsite

def main(args):
    '''Retrieve all instrument deployment events, group them by array subsite,
    and print the response as a JSON object'''
    
    status = 1
    
    # Create the UFrame instance
    if args.base_url:
        uframe = UFrame(base_url=args.base_url, timeout=args.timeout)
    else:
        uframe = UFrame(timeout=args.timeout)
        
    # Fetch the events request
    uframe.fetch_events()
    if not uframe.events:
        sys.stderr.write('No events requested: {:s}\n'.format(uframe))
        sys.stdout.write('[]\n')
        return status
        
    # Get only the .DeploymentEvents
    deployment_events = uframe.search_events_by_type('.DeploymentEvent')
    if not deployment_events:
        sys.stderr.write('No .DeploymentEvents found: {:s}\n'.format(uframe))
        sys.stdout.write('[]\n')
        return status
        
    # Create the grouping of instrument deployments organized by array subsite
    instruments = group_instrument_deployment_events_by_subsite(deployment_events)
    
    # JSON encode and print to STDOUT
    sys.stdout.write('{:s}\n'.format(json.dumps(instruments)))
    
    status = 0
     
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Must be specified if UFRAME_BASE_URL environment variable is not set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds <Default:120>')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))