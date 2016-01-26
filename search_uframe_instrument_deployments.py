#!/usr/bin/env python

import argparse
import json
import os
import sys
import datetime
import csv
import time
from pytz import timezone
from UFrame import UFrame

def main(args):
    '''Return the fully qualified reference designator list for all deployments of
    the instruments matching the partial or fully-qualified reference_designator.'''
    
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
    
    # Fetch the events info from UFrame
    if args.verbose:
        t0 = datetime.datetime.utcnow()
        sys.stderr.write('Fetching and creating UFrame table of contents...')
        
    uframe.fetch_events()
    
    if args.verbose:
        t1 = datetime.datetime.utcnow()
        dt = t1 - t0
        sys.stderr.write('Complete ({:d} seconds)\n'.format(dt.seconds))
    
    status = 1
    deployments = uframe.search_deployment_events_by_instrument(args.reference_designator)
        
    if args.json:
        json.dumps(deployments)
    else:
        cols = ['reference_designator',
            'startDateTs',
            'endDateTs',
            'startDate',
            'endDate',
            'deploymentNumber',
            'tense']
        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(cols)
        for deployment in deployments:
            
            deployment['reference_designator'] = '{:s}-{:s}-{:s}'.format(deployment['referenceDesignator']['subsite'], deployment['referenceDesignator']['node'], deployment['referenceDesignator']['sensor'])
            t0 = time.gmtime(deployment['startDate']/1000)
            dt0 = datetime.datetime(t0.tm_year,
                t0.tm_mon,
                t0.tm_mday,
                t0.tm_hour,
                t0.tm_min,
                t0.tm_sec,
                0,
                timezone('UTC'))
            deployment['startDateTs'] = dt0.strftime('%Y-%m-%dT%H:%M:%S.%sZ')

            # End date
            if not deployment['endDate']:
                deployment['endDateTs'] = None
            else:
                t1 = time.gmtime(deployment['endDate']/1000)
                dt1 = datetime.datetime(t1.tm_year,
                    t1.tm_mon,
                    t1.tm_mday,
                    t1.tm_hour,
                    t1.tm_min,
                    t1.tm_sec,
                    0,
                    timezone('UTC'))
                deployment['endDateTs'] = dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
            
            csv_writer.writerow([deployment[k] for k in cols])
    
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        help='Name of the instrument to search')
    arg_parser.add_argument('-j', '--json',
        dest='json',
        action='store_true',
        help='Return response as json.  Default is comma separated value.')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Value is taken from the UFRAME_BASE_URL environment variable, if set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds (Default is 120 seconds).')
    arg_parser.add_argument('--validate_uframe',
        action='store_true',
        help='Attempt to validate the UFrame instance <Default:False>')
    arg_parser.add_argument('-v', '--verbose',
        action='store_true',
        help='Verbose display')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
