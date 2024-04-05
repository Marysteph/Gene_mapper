from urllib.request import urlopen
from urllib.error import HTTPError
import logging
import urllib
import base64
import ujson as json
import os
 
os.environ['ENTEROBASE_API_TOKEN'] =''
SERVER_ADDRESS = 'https://enterobase.warwick.ac.uk/'
 
#  API Token
API_TOKEN = os.getenv('ENTEROBASE_API_TOKEN', None)

DATABASE = 'senterica'
 
 
BATCH_SIZE = 100
TOTAL = 450000
 
def __create_request(request_str):
    base64string = base64.b64encode('{0}: '.format(API_TOKEN).encode('utf-8'))
    headers = {"Authorization": "Basic {0}".format(base64string.decode())}
    request = urllib.request.Request(request_str, None, headers)
    return request
 
 
if not os.path.exists('temp'):
    os.mkdir('temp')
 
try:
        offset = 0
        strains = {}
        while offset < TOTAL:
            #   Get the strains in batches of 100
            address = (SERVER_ADDRESS + '/api/v2.0/{0}/strains?offset={1}&orderby=barcode&sortorder=desc&limit={2}'.
                       format(DATABASE, offset,BATCH_SIZE))
            response = urlopen(__create_request(address))
            data = json.load(response)
            # Finish if no more strains
            if len(data['Strains']) == 0:
                break
 
            # Get a list of the assembly barcodes as the AMR data is indexed by assembly barcodes
            assembly_barcodes = [x['assembly_barcode'] for x in data['Strains']]
 
            # Use comma separated list of assembly barcodes to get AMR data
            assembly_barcodes = ','.join(filter(None, assembly_barcodes))
            #assembly_barcodes = ','.join(assembly_barcodes)
            address = (SERVER_ADDRESS + '/api/v2.0/{0}/AMRdata?barcode={1}'.format(DATABASE, assembly_barcodes))
            response = urlopen(__create_request(address))
            amr_data = json.load(response)
            # Index results by assembly barcode
            amr_data_dict = {x['assembly_barcode']: x['amrfinder_results'] for x in amr_data['AMRAnalysis']}
            #print(amr_data_dict)
            
 
            # Add the AMR data to the strain data
            for strain in data['Strains']:
                assembly_barcode = strain['assembly_barcode']
                if assembly_barcode in amr_data_dict:
                    strain.update({'amrfinder_results': amr_data_dict[assembly_barcode]})
                else:
                    # Handle the case where AMR data is not available for this strain
                    # set 'amrfinder_results' to an empty dictionary
                    strain.update({'amrfinder_results': {}})

            # for strain in data['Strains']:
            #     strain.update({'amrfinder_results': amr_data_dict[strain['assembly_barcode']]})
 
            # create a consolidate list
            strains.update({x['strain_barcode']: x for x in data['Strains']})
 
            offset += BATCH_SIZE
            print(str(offset) + " sets of data downloaded")
 
        # output the consolidated list
        with open(os.path.join('temp', 'json.txt'), 'w') as json_data:
            json_data.write(json.dumps(strains, indent=2))
 
except HTTPError as Response_error:
    logging.error('%d %s. <%s>\n Reason: %s' %(Response_error.code,
                                              Response_error.reason,
                                              Response_error.geturl(),
                                              Response_error.read()))