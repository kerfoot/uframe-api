#!/usr/bin/env python

import os
import sys
import argparse
import json
import requests
import datetime
import csv
from operator import itemgetter

def main(args):
    '''Display all deployment events for the full or partially qualified
    reference designator.  A reference designator uniquely identifies an
    instrument.  Valid deployments are printed to STDOUT and invalid
    deployments are printed to STDERR.'''
    
    events = get_instrument_deployment_events(args.reference_designator)
    if not events:
        sys.stderr.write('{:s}: No deployment events found\n'.format(args.reference_designator))
        return 0
        
    instrument_deployment_events = parse_asset_deployment_events(events)
    
    # Filter the events by the optional args.status and args.filter
    filtered_events = filter_deployment_events(instrument_deployment_events,
        status=args.status,
        ref_des_search_string=args.filter)
        
    if not filtered_events:
        if args.active:
            sys.stderr.write('{:s}: No active deployment events found\n'.format(args.reference_designator))
        else:
            sys.stderr.write('{:s}: No deployment events found\n'.format(args.reference_designator))
        return 0
    
    # Sort the resulting events by reference designator
    sorted_events = sorted(filtered_events,
        key=itemgetter('reference_designator'))    
    if args.json:
        sys.stdout.write('{:s}\n'.format(json.dumps(sorted_events)))
        return 0
        
    csv_writer = csv.writer(sys.stdout)
    cols = ['reference_designator',
        'deployment_number',
        'active',
        'event_start_ts',
        'event_stop_ts',
        'event_start_ms',
        'event_stop_ms']
    csv_writer.writerow(cols)
    for event in sorted_events:
        csv_writer.writerow([event[c] for c in cols])
        
    return 0

def filter_deployment_events(events, status=None, ref_des_search_string=None):
    
    filtered_events = []

    if type(events) != list:
        sys.stderr.write('Invalid deployment events object\n')
        return filtered_events
        
    if not events:
        sys.stderr.write('No deployment events to filter\n')
        return filtered_events
    
    status_events = events
    if status:
        if status.lower().startswith('active'):
            status_events = [e for e in events if e['active']]
        elif status.lower().startswith('inactive'):
            status_events = [e for e in events if not e['active']]
    
    if not ref_des_search_string:
        return status_events
    
    # Search the reference designators for ref_des_search_string
    filtered_events = [e for e in status_events if e['reference_designator'].find(ref_des_search_string) > -1]
    
    return filtered_events

def get_instrument_deployment_events(ref_des, base_url=None):
    
    request_status = {'url' : None,
        'status_code' : None,
        'reason' : None,
        'status' : False}
    events = {'request' : request_status,
        'data' : [],
        'count' : 0}
    
    if not base_url:
        base_url = os.getenv('UFRAME_BASE_URL')
        if not base_url:
            sys.stderr.write('No UFrame base url specified\n')
            return events
            
    assets_url = '{:s}:12587/events/deployment/query?refdes={:s}'.format(base_url,
        ref_des)
    
    events['request']['url'] = assets_url
    
    # Fetch the url
    try:
        r = requests.get(assets_url)
    except requests.exceptions.MissingSchema as e:
        sys.stderr.write('{:s}\n'.format(e))
        events['request']['reason'] = e.message
        return events
        
    events['request']['status_code'] = r.status_code
    
    if r.status_code != 200:
        events['request']['reason'] = r.reason
        return events
        
    # Try to decode the response as json
    try:
        events['data'] = r.json()
    except ValueError as e:
        events['request']['reason'] = r.message
        return events
    
    # Update the return object    
    events['count'] = len(events['data'])
    events['request']['status'] = True
    
    return events
    
def parse_asset_deployment_events(events):
    
    deployment_events = []
    
    if not events['request']['status']:
        sys.stderr.write('Invalid/unsuccessful request: {:s}\n'.format(events['request']['url']))
        return deployment_events
        
    for event in events['data']:
        
        deployment_event = {'reference_designator' : None,
            'node' : event['referenceDesignator']['node'],
            'full' : event['referenceDesignator']['full'],
            'subsite' : event['referenceDesignator']['subsite'],
            'sensor' : event['referenceDesignator']['sensor'],
            'event_start_ms' : event['eventStartTime'],
            'event_stop_ms' : event['eventStopTime'],
            'deployment_number' : event['deploymentNumber'],
            'event_start_ts' : None,
            'event_stop_ts' : None,
            'active' : True}
        
        # Create the fully qualified reference designator if this is an instrument
        if event['referenceDesignator']['full']:
            deployment_event['reference_designator'] = '{:s}-{:s}-{:s}'.format(
                event['referenceDesignator']['subsite'],
                event['referenceDesignator']['node'],
                event['referenceDesignator']['sensor'])
    
        if not deployment_event['event_start_ms']:
            sys.stderr.write('{:s}: Deployment event has no eventStartTime\n'.format(deployment_event['reference_designator']))
            continue

        # Handle eventStartTime and eventStopTime
        if deployment_event['event_start_ms']:
            try:
                deployment_event['event_start_ts'] = datetime.datetime.utcfromtimestamp(deployment_event['event_start_ms']/1000).strftime('%Y-%m-%dT%H:%M:%S.%s')
            except ValueError as e:
                sys.stderr.write('Error parsing event_start_ms: {:s}\n'.format(e))
                continue

        if deployment_event['event_stop_ms']:
            try:
                deployment_event['event_stop_ts'] = datetime.datetime.utcfromtimestamp(deployment_event['event_stop_ms']/1000).strftime('%Y-%m-%dT%H:%M:%S.%s')
                deployment_event['active'] = False
            except ValueError as e:
                sys.stderr.write('Error parsing event_start_ms: {:s}\n'.format(e))
                continue
            
        deployment_events.append(deployment_event)
        
    return deployment_events
    
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
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify the base uFrame server URL. Must start with \'http://\'.  Must be specified if UFRAME_BASE_URL environment variable is not set')
    arg_parser.add_argument('-j', '--json',
        dest='json',
        action='store_true',
        help='Print deployment events as a JSON object')
            
    parsed_args = arg_parser.parse_args()
    #print parsed_args
    #sys.exit(13)

    sys.exit(main(parsed_args))
