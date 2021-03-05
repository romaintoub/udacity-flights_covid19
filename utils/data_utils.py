import os
import requests
import pandas as pd 

SOURCE_DATA_PATH = "./data/source/"
SOURCE_URL_PATH = "./data/source_url.csv"

def download_and_cache(filename, url, data_path = SOURCE_DATA_PATH):
    """ Downloads the file from ``url``, saves it to a local file,
        and returns that path. Will not re-download files which have
        already been cached.

        For example::

            >>> sales_data_csv = download_and_cache(
            ...     "sales-data.csv",
            ...     "https://example.com/sales-data.csv",
            ... )
            >>> sales_data_csv
            'data/sales-data.csv'
    """
    output_file = os.path.join( data_path, filename)
    if os.path.exists(output_file):
        return output_file
    print(f"{output_file} not found; downloading from: {url}...")
    outdir = os.path.dirname(output_file)
    if not os.path.exists(outdir):
        os.makedirs(os.path.dirname(output_file))
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(output_file + '.temp', 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
        os.rename(f.name, output_file)
    return output_file


def load_source_url( name, gzip=False):
    """
    Download source data with url provided in source_url.csv file
    Returns series with information such as the path of the csv file
    """
    print( f"Downloading if not exists {name}...")
    source = pd.read_csv( SOURCE_URL_PATH, index_col='name').loc[ name]

    filename = source.name + '.csv.gz' if gzip else source.name + '.csv'
    source_path = download_and_cache( filename, url= source.url, data_path = SOURCE_DATA_PATH)

    # assign source path into series
    source['path'] = source_path
    return source


