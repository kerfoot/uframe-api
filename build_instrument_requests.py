#!/usr/bin/env python

import argparse
import os
import sys
import datetime
from UFrame import UFrame

def main(args):
    '''Return the list of request urls that conform to the UFrame API for the 
        partial or fully-qualified reference_designator and all telemetry types.  
        The URLs request all stream L0 dataset parameters over the entire 
        time-coverage.  The urls are printed to STDOUT
    '''
    
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
    
    if (args.reference_designator):
        instruments = uframe.search_instruments(args.reference_designator)
    else:
        instruments = uframe.instruments
        
    if not instruments:
        sys.stderr.write('No instruments found for reference designator: {:s}\n'.format(args.reference_designator))
        sys.stderr.flush()
    
    urls = []    
    for instrument in instruments:
        
        request_urls = uframe.instrument_to_query(instrument,
            telemetry=args.telemetry,
            time_delta_type=args.time_delta_type,
            time_delta_value=args.time_delta_value,
            begin_ts=args.start_date,
            end_ts=args.end_date,
            time_check=args.time_check,
            exec_dpa=args.no_dpa,
            application_type=args.format,
            provenance=args.no_provenance,
            limit=args.limit,
            annotations=args.no_annotations,
            user=args.user,
            email=args.email)
    
        for request_url in request_urls:
            urls.append(request_url)
            
    for url in urls:
        sys.stdout.write('{:s}\n'.format(url))
        
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('reference_designator',
        help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('--telemetry',
        help='Restricts urls to the specified telemetry type')
    arg_parser.add_argument('-s', '--start_date',
        help='An ISO-8601 formatted string specifying the start time/date for the data set')
    arg_parser.add_argument('-e', '--end_date',
        help='An ISO-8601 formatted string specifying the end time/data for the data set')
    arg_parser.add_argument('--time_delta_type',
        help='Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta')
    arg_parser.add_argument('--time_delta_value',
        type=int,
        help='Positive integer value to subtract from the end time to get the start time for subsetting.')
    arg_parser.add_argument('--no_time_check',
        dest='time_check',
        default=True,
        action='store_false',
        help='Do not replace invalid request start and end times with stream metadata values if they fall out of the stream time coverage')
    arg_parser.add_argument('--no_dpa',
        action='store_false',
        default=True,
        help='Execute all data product algorithms to return L1/L2 parameters <Default:False>')
    arg_parser.add_argument('--no_provenance',
        action='store_false',
        default=True,
        help='Include provenance information in the data sets <Default:False>')
    arg_parser.add_argument('-f', '--format',
        dest='format',
        default='netcdf',
        help='Specify the download format (<Default:netcdf> or json)')
    arg_parser.add_argument('--no_annotations',
        action='store_false',
        default=False,
        help='Include all annotations in the data sets <Default>:False')
    arg_parser.add_argument('-l', '--limit',
        type=int,
        default=-1,
        help='Integer ranging from -1 to 10000.  <Default:-1> results in a non-decimated dataset')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Must be specified if UFRAME_BASE_URL environment variable is not set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds <Default:120>')
    arg_parser.add_argument('-v', '--verbose',
        action='store_true',
        help='Verbose display')
    arg_parser.add_argument('-u', '--user',
        dest='user',
        type=str,
        help='Add a user name to the query')
    arg_parser.add_argument('--validate_uframe',
        action='store_true',
        help='Attempt to validate the UFrame instance <Default:False>')
    arg_parser.add_argument('--email',
        dest='email',
        type=str,
        help='Add an email address for emailing UFrame responses to the request once sent')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
