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
import re
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta

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
        
        # Set the base_url
        self.base_url = base_url

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
          
    def fetch_toc(self):
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
        
        parameters.sort()
        streams.sort()        
        self._parameters = parameters
        self._streams = streams
        
        # Create a dict of unique array names
        arrays = {t.split('-')[0]:True for t in self._toc.keys()}.keys()
        arrays.sort()
        self._arrays = arrays
    
    def validate_reference_designator(self, reference_designator):
        '''Validates the reference designator'''

        match = re.compile(r'\w{8,}\-\w{5,}\-\w{2,}\-\w{9,}').search(reference_designator)
        
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
            return [p['particleKey'] for p in self._parameters if p['particleKey'].find(target_string) >= 0]
    
    def search_streams(self, target_array):
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
            for stream in streams:
                if stream['sensor'] in instruments:
                    continue
                instruments.append(stream['sensor'])
                
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
    
    def instrument_to_query(self, ref_des, telemetry=None, time_delta_type=None, time_delta_value=None, begin_ts=None, end_ts=None, time_check=True, exec_dpa=True, application_type='netcdf', provenance=True, limit=-1, annotations=False, user=None, email=None):
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
        
        urls = []
        
        instruments = self.search_instruments(ref_des)
        if not instruments:
            return urls
        
        self._port = 12576
        self._url = '{:s}:{:d}/sensor/inv'.format(self._base_url, self._port)    
        
        if time_delta_type and time_delta_value:
            if time_delta_type not in _valid_relativedeltatypes:
                sys.stderr.write('Invalid dateutil.relativedelta type: {:s}\n'.format(time_delta_type))
                sys.stderr.flush()
                return urls
        
        begin_dt = None
        end_dt = None
        if begin_ts:
            try:
                begin_dt = parser.parse(begin_ts)
            except ValueError as e:
                sys.stderr.write('Invalid begin_dt: {:s} ({:s})\n'.format(begin_ts, e.message))
                sys.stderr.flush()
                return urls    
                
        if end_ts:
            try:
                end_dt = parser.parse(begin_ts)
            except ValueError as e:
                sys.stderr.write('Invalid end_dt: {:s} ({:s})\n'.format(end_ts, e.message))
                sys.stderr.flush()
                return urls
                
        for instrument in instruments:
            
            # Validate the reference designator format
            if not self.validate_reference_designator(instrument):
                sys.stderr.write('Invalid format for reference designator: {:s}\n'.format(instrument))
                sys.stderr.flush()
                continue
                
            #sys.stdout.write('Instrument: {:s}\n'.format(instrument))
                
            # Store the metadata for this instrument
            meta = self.toc[instrument]
            
            # Break the reference designator up
            r_tokens = instrument.split('-')
            
            for stream in meta['streams']:
                
                #sys.stdout.write('Stream: {:s}\n'.format(stream['stream']))
                
                if telemetry and stream['method'].find(telemetry) == -1:
                    continue
                    
                #Figure out what we're doing for time
                dt0 = None
                dt1 = None
                
                stream_dt0 = parser.parse(stream['beginTime'])
                stream_dt1 = parser.parse(stream['endTime'])
                
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
                ts1 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')    
                ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        
                # Make sure the specified or calculated start and end time are within
                # the stream metadata times if time_check=True
                if time_check:
                    if dt1 > stream_dt1:
                        sys.stderr.write('time_check: End time exceeds stream endTime ({:s} > {:s})\n'.format(ts1, stream['endTime']))
                        sys.stderr.write('time_check: Setting request end time to stream endTime\n')
                        sys.stderr.flush()
                        dt1 = stream_dt1
                        #continue
                    
                    if dt0 < stream_dt0:
                        sys.stderr.write('time_check: Start time is earlier than stream beginTime ({:s} < {:s})\n'.format(ts0, stream['beginTime']))
                        sys.stderr.write('time_check: Setting request begin time to stream beginTime\n')
                        #continue
                        
                # Create the url
                stream_url = '{:s}/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&format=application/{:s}&limit={:d}&execDPA={:s}&include_provenance={:s}'.format(
                    self.url,
                    r_tokens[0],
                    r_tokens[1],
                    r_tokens[2],
                    r_tokens[3],
                    stream['method'],
                    stream['stream'],
                    ts0,
                    ts1,
                    application_type,
                    limit,
                    str(exec_dpa).lower(),
                    str(provenance).lower())
                    
                if user:
                    stream_url = '{:s}&user={:s}'.format(stream_url, user)
                    
                if email:
                    stream_url = '{:s}&email={:s}'.format(stream_url, email)
                    
                urls.append(stream_url)
                            
        return urls
            
    def __repr__(self):
        if self._url:
            return '<UFrame(url={:s})>'.format(self.url)
        else:
            return '<UFrame(url=None)>'

