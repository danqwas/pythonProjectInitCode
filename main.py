from grpc import CallCredentials

#call the credentials for the gcp project
import os
import chardet
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="hale-mercury-281113-90b44c2c83f0.json"




def async_detect_document(gcs_source_uri, gcs_destination_uri):
    """OCR with PDF/TIFF as source files on GCS"""
    import json
    import re
    from google.cloud import vision
    from google.cloud import storage
    import pandas as pd
    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'
    
    # How many pages should be grouped into each json output file.
    batch_size = 2

     
    client = vision.ImageAnnotatorClient()

    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print('Waiting for the operation to finish.')
    operation.result(timeout=420)

    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.
    storage_client = storage.Client()

    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    # List objects with the given prefix, filtering out folders.
    blob_list = [blob for blob in list(bucket.list_blobs(
        prefix=prefix)) if not blob.name.endswith('/')]
    print('Output files:')
    for blob in blob_list:
        print(blob.name)

    # Process the first output file from GCS.
    # Since we specified batch_size=2, the first response contains
    # the first two pages of the input file.
    output = blob_list[0]
   

    json_string = output.download_as_string()
    # encode the json string to latin-1 
    json_string = json_string.decode( 'latin-1' )
   
    # convert the json string to a utf-8 string
    json_string = json_string.encode('utf-8')
 
    
    
     # search for the json generate in the bucket and print the first page of the pdf
    for blob in blob_list:
        if blob.name.endswith('json'):
            print('Processing {}'.format(blob.name))
            json_string = blob.download_as_string()
            # encode the json string to latin-1 
            json_string = json_string.decode( 'latin-1' )
            print(json_string)
            # convert the json string to a utf-8 string
            json_string = json_string.encode('utf-8')
            print(json_string)
            # convert the json string to a python dictionary
            json_dict = json.loads(json_string)
            print(json_dict)
            # convert the json dictionary to a pandas dataframe
            df = pd.DataFrame(json_dict['responses'][0]['fullTextAnnotation']['pages'][0]['blocks'])
            print(df)
           
          
        
    
   

 
   
    
    
# Path: main.py
def main():
    """Runs the data processing scripts to generate a dataset for training."""
    # string for pdf
    pdf = "nombredelpdf.pdf"
    gcs_source_uri = 'gs://pdf_news/recibo_TF01_ASESOR METODOLOGICO_ECHEGARAY APAC.docx.pdf'
    gcs_destination_uri = 'gs://pdf_news/recibo_TF01_ASESOR METODOLOGICO_ECHEGARAY APAC.docx.pdf'
    async_detect_document(gcs_source_uri, gcs_destination_uri)
    
#run main
if __name__ == '__main__':
    main()