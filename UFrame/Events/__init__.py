
import json
import time


def group_instrument_deployment_events_by_subsite(deployment_events):
    
    subsites = []
    
    deployment_keys = ['endDate',
        'startDate',
        'deploymentNumber']   
    
    # Grab all deployment events for fully-qualified reference designators (instruments)
    instruments = [d for d in deployment_events if d['referenceDesignator']['subsite'] and d['referenceDesignator']['node'] and d['referenceDesignator']['sensor']]
     
    for d in instruments:
        
        if not subsites or d['referenceDesignator']['subsite'] not in [s['name'] for s in subsites]:
            s = {'array' : d['referenceDesignator']['subsite'][:2],
                'name' : d['referenceDesignator']['subsite'],
                'children' : []}
            subsites.append(s)
        
        i = [s['name'] for s in subsites].index(d['referenceDesignator']['subsite'])
            
        sensor = {k:d[k] for k in deployment_keys}
        
        refdes = '{:s}-{:s}-{:s}'.format(d['referenceDesignator']['subsite'],
            d['referenceDesignator']['node'],
            d['referenceDesignator']['sensor'])
            
        sensor['refdes'] = refdes
        sensor['instrument'] = 'No Description'
        if d['referenceDesignator']['vocab']:
            sensor['instrument'] = d['referenceDesignator']['vocab']['instrument']
            
        sensor['sensor'] = d['referenceDesignator']['sensor']
        sensor_tokens = sensor['sensor'].split('-')
        sensor['class'] = sensor_tokens[1][:5]
        
        sensor['startDateTs'] = None
        if sensor['startDate']:
            sensor['startDateTs'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                time.gmtime(sensor['startDate']/1000))
        
        sensor['endDateTs'] = None
        if sensor['endDate']:
            sensor['endDateTs'] = time.strftime('%Y-%m-%dT%H:%M:%S',
                time.gmtime(sensor['endDate']/1000))
    
        subsites[i]['children'].append(sensor)
        
    return subsites

