"""
UFrame class for interacting with an instance of the UFrame data management system.
This class provides properties and methods to query the specified UFrame instance
for information on OOI arrays, reference designators, streams and parameter as well
as to create one or more request urls for creating data products as NetCDF or JSON.
"""

import requests
import sys
import os
import datetime
import time
import re
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta
from pytz import timezone

HTTP_STATUS_OK = 200

_valid_relativedeltatypes = ('years',
    'months',
    'weeks',
    'days',
    'hours',
    'minutes',
    'seconds')

class UFrame(object):
    '''Class for interacting with the OOI UFrame data-services API
    
    Parameters:
        base_url: Base url of the UFrame instance, beginning with http://.  Will
            be taken from the UFRAME_BASE_URL environment variable, if set.
        port: server port (Default is 12576 and should not be changed)
        timeout: timeout duration (Default is 120 seconds)
    '''
    
    def __init__(self, base_url=None, port=12576, timeout=120, validate=False):
        if not base_url:
            base_url = os.getenv('UFRAME_BASE_URL')
        
        # UFrame configuration
        self._url = None
        self._port = port
        self._timeout = timeout
        self._validate_uframe = validate
        
        # Table of contents
        self._toc = []
        self._arrays = []
        self._instruments = []
        self._parameters = []
        self._streams = []
        
        # Deployment Events
        self._selected_deployment_events = []
        self._filtered_deployment_events = []
        self._filtered_parsed_deployment_events = []
        self._active_deployment_events = []
        
        # Set the base_url
        self.base_url = base_url

        # Fetch the UFrame Table of Contents
        self._fetch_toc()
        
        # Response from the last UFrame async request sent via self.send_async_request
        self._last_request_response = None
        
    @property
    def base_url(self):
        return self._base_url
    @base_url.setter
    def base_url(self, url):
        if not url:
            sys.stderr.write('No base_url specified\n')
            sys.stderr.flush()
            return
        
        # Send the base url request to see if this is a valid uframe instance
        if self._validate_uframe:
            try:
                r = requests.get(url)
            except requests.RequestException as e:
                sys.stderr.write('Invalid UFrame instance: {:s} (Reason={:s})\n'.format(url, e.message))
                sys.stderr.flush()
                return
                
            # Should get a 200 server response
            if r.status_code != HTTP_STATUS_OK:
                sys.stderr.write('Invalid UFrame instance: {:s} (Reason={:s})\n'.format(url, r.message))
                sys.stderr.flush()
                return
            
        # Store the base url    
        self._base_url = url
        # Create the data services url
        self._url = '{:s}:{:d}/sensor/inv'.format(self.base_url, self.port)

        # Fetch the table of contents at the new url
        self._fetch_toc()
        
        # Empty out the deployment event props
        self._selected_deployment_events = []
        self._filtered_deployment_events = []
        self._filtered_parsed_deployment_events = []
        self._active_deployment_events = []
        
        # Asynchronous requests
        self._last_async_request_urls = []
        self._last_async_request_responses = []
        
    @property
    def port(self):
        return self._port
    @port.setter
    def port(self, port):
        self._port = port
        self._url = '{:s}:{:d}/sensor/inv'.format(self.base_url, self.port)

    @property
    def timeout(self):
        return self._timeout
    @timeout.setter
    def timeout(self, value):
        self._timeout = value

    @property
    def toc(self):
        return self._toc
        
    @property
    def url(self):
        return self._url
      
    @property
    def instruments(self):
        return self._instruments
        
    @property
    def parameters(self):
        return self._parameters
        
    @property
    def streams(self):
        return self._streams
        
    @property
    def arrays(self):
        return self._arrays
        
    @property
    def all_deployment_events(self):
        return self._selected_deployment_events
        
    @property
    def deployment_events(self):
        return self._filtered_deployment_events
        
    @property
    def instrument_deployments(self):
        return self._filtered_parsed_deployment_events
        
    @property
    def last_async_request_urls(self):
        return self._last_async_request_urls
        
    @property
    def last_async_responses(self):
        return self._last_async_request_responses
    
    def search_instrument_deployments(self, ref_des, ref_des_search_string=None, status=None, raw=False):
        '''Return the list of all deployment events for the specified reference
        designator, which may be partial or fully-qualified reference designator
        identifying the subsite, node or sensor.  An optional keyword argument
        (status) may be set to all, active or inactive to return all <default>,
        active or inactive deployment events'''
        
        assets_url = '{:s}:12587/events/deployment/query?refdes={:s}'.format(self.base_url,
            ref_des)
        
        self._selected_deployment_events = []
        self._filtered_deployment_events = []
        self._filtered_parsed_deployment_events = []
        
        # Send the request
        try:
            r = requests.get(assets_url)
        except requests.exceptions.MissingSchema as e:
            sys.stderr.write('{:s}\n'.format(e))
            return self._filtered_parsed_deployment_events
        
        # Check the request status
        if r.status_code != 200:
            sys.stderr.write('{:s}\n'.format(r.reason))
            return self._filtered_parsed_deployment_events
         
        # Decode the json response
        try:
            self._selected_deployment_events = r.json()
        except ValueError as e:
            sys.stderr.write('{:s}\n'.format(r.message))
            return self._filtered_parsed_deployment_events
            
        for event in self._selected_deployment_events:
            
            # Event must have a fully qualified reference designator
            if not event['referenceDesignator']['full']:
                sys.stderr.write('{:s}: Invalid instrument for event id={:0.0f}\n'.format(event['eventName'], event['eventId']))
                continue
             
            # Create the fully qualified reference designator
            reference_designator = '{:s}-{:s}-{:s}'.format(
                event['referenceDesignator']['subsite'],
                event['referenceDesignator']['node'],
                event['referenceDesignator']['sensor'])   
                
            # Events must have a eventStartTime to be considered valid
            if not event['eventStartTime']:
                sys.stderr.write('{:s}: Deployment event id={:0.0f} has no eventStartTime\n'.format(event['eventName'], event['eventId']))
                continue
            
            # Create the concise instrument deployment event object    
            deployment_event = {'instrument' : None,
                'event_start_ms' : event['eventStartTime'],
                'event_stop_ms' : event['eventStopTime'],
                'deployment_number' : event['deploymentNumber'],
                'event_start_ts' : None,
                'event_stop_ts' : None,
                'active' : False,
                'valid' : False}
            instrument = {'reference_designator' : reference_designator,
                'node' : event['referenceDesignator']['node'],
                'full' : event['referenceDesignator']['full'],
                'subsite' : event['referenceDesignator']['subsite'],
                'sensor' : event['referenceDesignator']['sensor']}
            # Add the instrument info to the event
            deployment_event['instrument'] = instrument
    
            # Parse the deployment event start time
            try:
                deployment_event['event_start_ts'] = datetime.datetime.utcfromtimestamp(deployment_event['event_start_ms']/1000).strftime('%Y-%m-%dT%H:%M:%S.%sZ')
            except ValueError as e:
                sys.stderr.write('Error parsing event_start_ms: {:s}\n'.format(e))
                continue
    
            # Parse the deployment event end time, if there is one
            if deployment_event['event_stop_ms']:
                try:
                    deployment_event['event_stop_ts'] = datetime.datetime.utcfromtimestamp(deployment_event['event_stop_ms']/1000).strftime('%Y-%m-%dT%H:%M:%S.%sZ')
                except ValueError as e:
                    sys.stderr.write('Error parsing event_start_ms: {:s}\n'.format(e))
                    continue
            else:
                # If the event does not have an end time, mark the deployment as active
                deployment_event['active'] = True
                
            # Deployment is valid
            deployment_event['valid'] = True
        
            # Optionally filter the event based on it's status (None, 'all', 'active', 'inactive')
            if status:
                if status.lower() == 'active' and not deployment_event['active']:
                    continue
                elif status.lower() == 'inactive' and deployment_event['active']:
                    continue
                    
            # Search the reference_designator for ref_des_search_string if specified
            if ref_des_search_string:
                if reference_designator.find(ref_des_search_string) == -1:
                    continue
                    
            # If we've made it here, add the event and deployment_event
            self._filtered_deployment_events.append(event)
            self._filtered_parsed_deployment_events.append(deployment_event)
            
        return self._filtered_parsed_deployment_events
    
    def get_active_deployments(self, ref_des=None, ref_des_search_string=None):
        '''Retrieve the list of actively deployed instruments from the entire UFrame
        asset management schema.  A reference designator may be specified to retrieve
        only active deployment events for that instrument or array.  Resulting
        events may also be filtered by specifying a ref_des_search_string'''
        
        events = []

        if ref_des:
            # Get the list of fully-qualified instrument reference designators for 
            # the specified partial or fully qualified ref_des
            instruments = self.search_instruments(ref_des)
        else:
            instruments = self.instruments
            
        for i in instruments:
            new_events = self.search_instrument_deployments(i, status='active', ref_des_search_string=ref_des_search_string)
            if not new_events:
                continue
            events = events + new_events
            
        self._active_deployment_events = events
        
        return events

    def validate_reference_designator(self, reference_designator):
        '''Validates the reference designator'''

        match = re.compile(r'\w{8,}\-\w{5,}\-\w{2,}\-\w{1,}').search(reference_designator)
        
        if match:
            return True
        else:
            return False
            
    def search_instruments(self, target_string, metadata=False):
        '''Return the list of all instrument reference designators containing the 
        target_string from the current UFrame table of contents.
        
        Parameters:
            target_string: partial or fully-qualified reference designator
            metadata: set to True to return an array of dictionaries containing the
                instrument metadata.'''
        
        if not self._toc:
            sys.stderr.write('You must fetch the table of contents first\n')
            sys.stderr.flush()
            return []
            
        if metadata:
            return [self._toc[r] for r in self._instruments if r.find(target_string) >= 0]
        else:
            return [r for r in self._instruments if r.find(target_string) >= 0]
        
    def search_parameters(self, target_string, metadata=False):
        '''Return the list of all stream parameters containing the target_string
        from the current UFrame table of contents.
        
        Parameters:
            target_string: partial or fully-qualified parameter name
            metadata: set to True to return an array of dictionaries containing the
                parameter metadata.'''
        
        if not self._toc:
            sys.stderr.write('You must fetch the table of contents first\n')
            sys.stderr.flush()
            return []
            
        if metadata:
            return [p for p in self._parameters if p['particleKey'].find(target_string) >= 0]
        else:
            #return [p['particleKey'] for p in self._parameters if p['particleKey'].find(target_string) >= 0]
            return [p for p in self._parameters if p.find(target_string) >= 0]
    
    def search_streams(self, target_stream):
        '''Returns a the list of all streams containing the target_stream fragment
        
        Parameters:
            target_stream: partial or full stream name'''
        
        if not self._toc:
            sys.stderr.write('You must fetch the table of contents first\n')
            sys.stderr.flush()
            return []
            
        return [s for s in self._streams if s.find(target_stream) >= 0]
        
    def search_arrays(self, target_array):
        
        arrays = []
        
        if not self._toc:
            sys.stderr.write('You must fetch the table of contents first\n')
            sys.stderr.flush()
            return arrays
            
        # Create a dict of unique array names
        arrays = [a for a in self._arrays if a.find(target_array) >= 0]
        arrays.sort()
        
        return arrays
        
    def stream_to_instrument(self, target_stream):
        '''Returns a the list of all instrument reference designators producing
        the specified stream
        
        Parameters:
            target_stream: partial or full stream name'''
        
        instruments = []
        
        if not self._toc:
            sys.stderr.write('You must fetch the table of contents first\n')
            sys.stderr.flush()
            return instruments
            
        for r in self._toc.keys():
            streams = [s for s in self._toc[r]['streams'] if s['stream'].find(target_stream) >= 0]
            if not streams:
                continue
            for stream in streams:
                if stream['reference_designator'] in instruments:
                    continue
                instruments.append(stream['reference_designator'])
                
        instruments.sort()
        
        return instruments
        
    def instrument_to_streams(self, reference_designator):
        '''Return the list of all streams produced by the partial or fully-qualified
        reference designator.
        
        Parameters:
            reference_designator: partial or fully-qualified reference designator to search
        '''
        
        ref_des_streams = []
        
        instruments = self.search_instruments(reference_designator)
        if not instruments:
            return ref_des_streams
        
        for instrument in instruments:
            
            streams = self._toc[instrument]['streams']
            
            for stream in streams:
                
                # Add the reference designator
                stream['reference_designator'] = instrument
                
                # Parse stream beginTime and endTime to create a unix timestamp, in milliseconds
                try:
                    stream_dt0 = parser.parse(stream['beginTime'])
	        except ValueError as e:
                    sys.stderr.write('{:s}: {:s} ({:s})\n'.format(stream['stream'], stream['beginTime'], e.message))
	                sys.stderr.flush()
	                continue

                try:
                    stream_dt1 = parser.parse(stream['endTime'])
	        except ValueError as e:
                    sys.stderr.write('{:s}: {:s} ({:s})\n'.format(stream['stream'], stream['endTime'], e.message))
	                sys.stderr.flush()
	                continue
                
                # Format the endDT and beginDT values for the query
                stream['beginTimeEpochMs'] = None
                stream['endTimeEpochMs'] = None
                try:
                    stream['endTimeEpochMs'] = int(time.mktime(stream_dt1.timetuple()))*1000
                except ValueError as e:
                    sys.stderr.write('endTime conversion error: {:s}-{:s}: {:s}\n'.format(instrument, stream['stream'], e.message))

                try:
                    stream['beginTimeEpochMs'] = int(time.mktime(stream_dt0.timetuple()))*1000
                except ValueError as e:
                    sys.stderr.write('beginTime conversion error: {:s}-{:s}: {:s}\n'.format(instrument, stream['stream'], e.message))

                ref_des_streams.append(stream)
                
        return ref_des_streams
        
    def get_instrument_metadata(self, reference_designator):
        '''Returns the full metadata listing for all instruments matching the
        partial or fully qualified reference designator.
        
        Parameters:
            reference_designator: partial or fully-qualified reference designator to search
        '''
        
        metadata = {}
        
        instruments = self.search_instruments(reference_designator)
        if not instruments:
            return metadata
            
        for instrument in instruments:
            
            metadata[instrument] = self._toc[instrument]
            
        return metadata
    
    def instrument_to_query(self, ref_des, stream=None, telemetry=None, time_delta_type=None, time_delta_value=None, begin_ts=None, end_ts=None, time_check=True, exec_dpa=True, application_type='netcdf', provenance=True, limit=-1, annotations=False, user='_nouser', email=None, selogging=False):
        '''Return the list of request urls that conform to the UFrame API for the specified
        reference_designator.
        
        Parameters:
            ref_des: partial or fully-qualified reference designator
            telemetry: telemetry type (Default is all telemetry types
            time_delta_type: Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta'
            time_delta_value: Positive integer value to subtract from the end time to get the start time for subsetting.
            begin_dt: ISO-8601 formatted datestring specifying the dataset start time
            end_dt: ISO-8601 formatted datestring specifying the dataset end time
            exec_dpa: boolean value specifying whether to execute all data product algorithms to return L1/L2 parameters (Default is True)
            application_type: 'netcdf' or 'json' (Default is 'netcdf')
            provenance: boolean value specifying whether provenance information should be included in the data set (Default is True)
            limit: integer value ranging from -1 to 10000.  A value of -1 (default) results in a non-decimated dataset
            annotations: boolean value (True or False) specifying whether to include all dataset annotations
        '''
        
        self._last_async_request_urls = []
        self._last_async_request_responses = []
        
        instruments = self.search_instruments(ref_des)
        if not instruments:
            return []
        
        self._port = 12576
        self._url = '{:s}:{:d}/sensor/inv'.format(self._base_url, self._port)    
        
        if time_delta_type and time_delta_value:
            if time_delta_type not in _valid_relativedeltatypes:
                sys.stderr.write('Invalid dateutil.relativedelta type: {:s}\n'.format(time_delta_type))
                sys.stderr.flush()
                return []
        
        begin_dt = None
        end_dt = None
        if begin_ts:
            try:
                begin_dt = parser.parse(begin_ts)
            except ValueError as e:
                sys.stderr.write('Invalid begin_dt: {:s} ({:s})\n'.format(begin_ts, e.message))
                sys.stderr.flush()
                return []    
                
        if end_ts:
            try:
                end_dt = parser.parse(end_ts)
            except ValueError as e:
                sys.stderr.write('Invalid end_dt: {:s} ({:s})\n'.format(end_ts, e.message))
                sys.stderr.flush()
                return []
                
        for instrument in instruments:
            
            # Validate the reference designator format
            if not self.validate_reference_designator(instrument):
                sys.stderr.write('Invalid format for reference designator: {:s}\n'.format(instrument))
                sys.stderr.flush()
                continue
                
            #sys.stdout.write('Instrument: {:s}\n'.format(instrument))
                
            # Store the metadata for this instrument
            #meta = self.toc[instrument]
            # Get the streams produced by this instrument
            instrument_streams = self.instrument_to_streams(instrument)
            if stream:
                stream_names = [s['stream'] for s in instrument_streams]
                if stream not in stream_names:
                    sys.stderr.write('{:s}: Invalid stream specified: {:s}\n'.format(instrument, stream))
                    continue
                    
                i = stream_names.index(stream)
                instrument_streams = [instrument_streams[i]]
                
            if not instrument_streams:
                sys.stderr.write('{:s}: No valid streams found\n'.format(instrument))
                continue
                
            
            # Break the reference designator up
            r_tokens = instrument.split('-')
            
            for instrument_stream in instrument_streams:
                
                if telemetry and instrument_stream['method'].find(telemetry) == -1:
                    continue
                    
                #Figure out what we're doing for time
                dt0 = None
                dt1 = None
               
                try:
                    stream_dt0 = parser.parse(instrument_stream['beginTime'])
                except ValueError:
                    sys.stderr.write('{:s}-{:s}: Invalid beginTime ({:s})\n'.format(instrument, instrument_stream['stream'], instrument_stream['beginTime']))
                    continue

                try:
                    stream_dt1 = parser.parse(instrument_stream['endTime'])
                except ValueError:
                    sys.stderr.write('{:s}-{:s}: Invalid endTime ({:s})\n'.format('instrument', instrument_stream['stream'], instrument_stream['endTime']))
                    continue
                
                sys.stderr.flush()

                if time_delta_type and time_delta_value:
                    dt1 = stream_dt1
                    dt0 = dt1 - tdelta(**dict({time_delta_type : time_delta_value})) 
                else:
                    if begin_dt:
                        dt0 = begin_dt
                    else:
                        dt0 = stream_dt0
                        
                    if end_dt:
                        dt1 = end_dt
                    else:
                        dt1 = stream_dt1
                
                # Format the endDT and beginDT values for the query
                try:
                    ts1 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    sys.stderr.write('{:s}-{:s}: {:s}\n'.format(instrument, instrument_stream['stream'], e.message))
                    continue

                try:
                    ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    sys.stderr.write('{:s}-{:s}: {:s}\n'.format(instrument, instrument_stream['stream'], e.message))
                    continue
                        
                # Make sure the specified or calculated start and end time are within
                # the stream metadata times if time_check=True
                if time_check:
                    if dt1 > stream_dt1:
                        sys.stderr.write('time_check ({:s}-{:s}): End time exceeds stream endTime ({:s} > {:s})\n'.format(ref_des, instrument_stream['stream'], ts1, instrument_stream['endTime']))
                        sys.stderr.write('time_check ({:s}-{:s}): Setting request end time to stream endTime\n'.format(ref_des, instrument_stream['stream']))
                        sys.stderr.flush()
                        ts1 = instrument_stream['endTime']
                    
                    if dt0 < stream_dt0:
                        sys.stderr.write('time_check ({:s}-{:s}): Start time is earlier than stream beginTime ({:s} < {:s})\n'.format(ref_des, instrument_stream['stream'], ts0, instrument_stream['beginTime']))
                        sys.stderr.write('time_check ({:s}-{:s}): Setting request begin time to stream beginTime\n'.format(ref_des, instrument_stream['stream']))
                        ts0 = instrument_stream['beginTime']
                       
                    # Check that ts0 < ts1
                    dt0 = parser.parse(ts0)
                    dt1 = parser.parse(ts1)
                    if dt0 >= dt1:
                        sys.stderr.write('{:s}: Invalid time range specified ({:s} >= {:s})\n'.format(instrument_stream['stream'], ts0, ts1))
                        continue

                # Create the url
                stream_url = '{:s}/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&format=application/{:s}&limit={:d}&execDPA={:s}&include_provenance={:s}&selogging={:s}&user={:s}'.format(
                    self.url,
                    r_tokens[0],
                    r_tokens[1],
                    r_tokens[2],
                    r_tokens[3],
                    instrument_stream['method'],
                    instrument_stream['stream'],
                    ts0,
                    ts1,
                    application_type,
                    limit,
                    str(exec_dpa).lower(),
                    str(provenance).lower(),
                    str(selogging).lower(),
                    user)
                    
                #if user:
                #    stream_url = '{:s}&user={:s}'.format(stream_url, user)
                    
                if email:
                    stream_url = '{:s}&email={:s}'.format(stream_url, email)
                    
                self._last_async_request_urls.append(stream_url)
                            
        return self._last_async_request_urls
    
    def instrument_to_deployment_query(self, ref_des, deployment_number=0, tense=None, telemetry=None, begin_ts=None, end_ts=None, time_check=True, exec_dpa=True, application_type='netcdf', provenance=True, limit=-1, annotations=False, user=None, email=None):
        
        urls = []
        
        if not self._events:
            sys.stderr.write('No events fetched.')
            return urls
            
        # Get the deployment events for the specified instrument(s)
        deployment_events = self.search_deployment_events_by_instrument(ref_des, tense)
        
        if not deployment_events:
            return urls
            
        for d in deployment_events:
            
            if deployment_number and d['deploymentNumber'] != deployment_number:
                continue
                
            # Create the fully-qualified reference designator corresponding to this
            # instrument
            ref_des = '{:s}-{:s}-{:s}'.format(d['referenceDesignator']['subsite'], d['referenceDesignator']['node'], d['referenceDesignator']['sensor'])
            # Format startDate and endDate to UFrame compatible strings
            if not d['startDate']:
                sys.stderr.write('{:s}: No deployment startDate from DeploymentEvent\n'.format(ref_des))
                continue
            
            try: 
                t0 = time.gmtime(d['startDate']/1000)
                dt0 = datetime.datetime(t0.tm_year,
                    t0.tm_mon,
                    t0.tm_mday,
                    t0.tm_hour,
                    t0.tm_min,
                    t0.tm_sec,
                    0,
                    timezone('UTC'))
                ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
            except ValueError as e:
                sys.stderr.write('{:s} (Deployment {:d} start_date): {:s}\n'.format(ref_des, d['deploymentNumber'], e.message))
                continue

            # End date
            if not d['endDate']:
                ts1 = None
            else:
                try:
                    t1 = time.gmtime(d['endDate']/1000)
                    dt1 = datetime.datetime(t1.tm_year,
                        t1.tm_mon,
                        t1.tm_mday,
                        t1.tm_hour,
                        t1.tm_min,
                        t1.tm_sec,
                        0,
                        timezone('UTC'))
                    ts1 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
                except ValueError as e:
                    sys.stderr.write('{:s} (Deployment {:d} end date): {:s}\n'.format(ref_des, d['deploymentNumber'], e.message))
                    continue
                    
            # Create the queries
            instrument_urls = self.instrument_to_query(ref_des,
                telemetry=telemetry, 
                begin_ts=ts0, 
                end_ts=ts1,
                time_check=time_check,
                exec_dpa=exec_dpa,
                application_type=application_type, 
                provenance=provenance, 
                limit=limit, 
                annotations=annotations, 
                user=user, 
                email=email)
                
            if not instrument_urls:
                sys.stderr.write('{:s}: No queries created\n'.format(ref_des))
                continue
                
            for url in instrument_urls:
                urls.append(url)
                
        return urls
        
    def send_async_requests(self, urls=[], debug=False):
        '''Validate and send the request url directly to the UFrame instance.  The 
        request response is returned and also stored in UFrame.last_async_response'''
    
        # Send the last batch of requests created by the instance if no urls
        if not urls:
            urls = self._last_async_request_urls
        elif type(urls) == str:
            urls = [urls]
        elif type(urls) != list:
            sys.stderr.write('urls parameter must be either a single request url or list of request urls\n')
            return None
        
        if not urls:
            sys.stderr.write('No urls to send\n')
            return None
            
        # Clear the last set of request responses    
        self._last_async_request_responses = []
        
        for url in urls:
            
            # Remove leading and trailing whitespace from the url
            request_url = url.strip()
            
            response = {'requestUrl' : request_url,
                'status' : False,
                'status_code' : -1,
                'response' : None,
                'reason' : None,
                'stream' : {},
                'reference_designator' : None,
                'instrument' : None,
                'm2m' : {'status' : False, 'request_params' : None}}
            
            # The url must be sent to the UFrame.base_url UFrame instance
            if not url.startswith(self.base_url):
                response['requestUrl'] = 'URL points to alternate UFrame instance'
                # Store the response
                self._last_async_request_responses.append(response)
                continue 
            
            # Parse the request url and grab everything after /sensor/inv/ up to the query (?)
            request_regexp = re.compile('^https?:\/\/.*\/sensor\/inv\/(.*)\?')
            match = request_regexp.match(url)
            
            # Match required   
            if not match:
                response['reason'] = 'UFrame Instance: Badly Formatted Request'
                # Store the response
                self._last_async_request_responses.append(response)
                continue 
            
            # A properly formatted UFrame request url will split into 5 pieces    
            request_tokens = match.groups()[0].split('/')
            if len(request_tokens) != 5:
                response['reason'] = 'UFrame Instance: Badly Formatted Request'
                # Store the response
                self._last_async_request_responses.append(response)
                continue 
            
            # Create the stream name from the 5 tokens
            response['reference_designator'] = '-'.join(request_tokens[:3])
        #    response['stream'] = '-'.join(request_tokens)
            # 2016-09-30: kerfoot@marine - new stream name
            order = [0,1,2,4,3]
            response['stream'] = '-'.join([request_tokens[x] for x in order])
        
            response['instrument'] = {'subsite' : request_tokens[0],
                'node' : request_tokens[1],
                'sensor' : request_tokens[2],
                'telemetry' : request_tokens[3],
                'stream' : request_tokens[4]}
                
            try:
                r = requests.get(request_url)
            except requests.exceptions.RequestException as e:
                sys.stderr.write('{:s}\n'.format(e))
                response['reason'] = e
                # Store the response
                self._last_async_request_responses.append(response)
                continue 
            
            response['status_code'] = r.status_code
            response['reason'] = r.reason
                
            if r.status_code != 200:
                # Store the response
                self._last_async_request_responses.append(response)
                continue 
            
            # Decode the json UFrame response    
            try:
                response['response'] = r.json()
                response['status'] = True
            except requests.exceptions.ValueError as e:
                response['reason'] = e
            
            # Store the response
            self._last_async_request_responses.append(response)
        
        return self._last_async_request_responses
        
    def _fetch_toc(self):
        '''Fetch the response from the UFrame table of contents end point and create
        a data structure containing the streams and instruments from the Uframe instance.
        This should be the first method you call once you point the UFrame instance
        at a URL.'''
        
        self._port = 12576
        self._url = '{:s}:{:d}/sensor/inv/toc'.format(self._base_url, self._port)
        
        try:
            r = requests.get(self._url)
        except requests.RequestException as e:
            sys.stderr.write('{:s} ({:s})\n'.format(e.message, type(e)))
            return
            
        if r.status_code != HTTP_STATUS_OK:
            sys.stderr.write('Failed to fetch TOC: {:s}\n'.format(r.message))
            return
            
        try:
            toc_response = r.json()
        except ValueError as e:
            sys.stderr.write('{:s}\n'.format(e.message))
            return
        
        # Old TOC is an array of instruments.
        # New TOC is a dictionary
        # So we need to convert based on type(toc_response)
        if type(toc_response) == list:
            # Map the instrument metadata response to the reference designator
            self._toc = {i['reference_designator']:i for i in toc_response}
    
            # Create the sorted list of reference designators
            ref_des = self._toc.keys()
            ref_des.sort()
            self._instruments = ref_des
            
            # Create a list of unique parameters
            parameters = []
            streams = []
            for i in toc_response:
                for p in i['instrument_parameters']:
                    if not parameters:
                        parameters.append(p['particleKey'])
                        continue
                    elif p['particleKey'] in parameters:
                        continue
                        
                    parameters.append(p['particleKey'])
                        
                for s in i['streams']:
                    if not streams:
                        streams.append(s['stream'])
                    elif s['stream'] in streams:
                        continue
                        
                    streams.append(s['stream'])
        elif type(toc_response) == dict:
            # Map the instrument metadata response to the reference designator
            self._toc = {i['reference_designator']:i for i in toc_response['instruments']}
            
            # Create the sorted list of reference designators
            ref_des = self._toc.keys()
            ref_des.sort()
            self._instruments = ref_des
            
            # Create a dictionary mapping parameter id (pdId) to the parameter metadata
            param_defs = {p['pdId']:p for p in toc_response['parameter_definitions']}
            # Loop through the toc_response['parameters_by_stream'] and create
            # an array of dictionaries containing all paramters for the specified stream
            stream_defs = {}
            for s in toc_response['parameters_by_stream'].keys():
                stream_params = [param_defs[pdId] for pdId in toc_response['parameters_by_stream'][s]]
                for p in stream_params:
                    p[u'stream'] = s
                    
                stream_defs[s] = stream_params
                    
            # Loop through self._toc (instruments) and add the stream_params
            for i in self._toc.keys():
                self._toc[i]['instrument_parameters'] = []
                for s in self._toc[i]['streams']:
                    s['reference_designator'] = i
                    self._toc[i]['instrument_parameters'] = self._toc[i]['instrument_parameters'] + stream_defs[s['stream']]
                    
            # Create the full list of parameter names
            parameters = [p['particle_key'] for p in toc_response['parameter_definitions']]
            # Create the full list of streams
            streams = stream_defs.keys()
            
        else:
            sys.stderr.write('Unknown TOC response\n')
            return
            
        
        # Sort parameters
        parameters.sort()
        # Sort streams
        streams.sort()        
        self._parameters = parameters
        self._streams = streams
        
        # Create a dict of unique array names
        arrays = {t.split('-')[0]:True for t in self._toc.keys()}.keys()
        arrays.sort()
        self._arrays = arrays

    def __repr__(self):
        if self._base_url:
            return '<UFrame(url={:s})>'.format(self.base_url)
        else:
            return '<UFrame(url=None)>'

