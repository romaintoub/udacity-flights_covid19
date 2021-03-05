import os
import configparser
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lit, split, count, isnan, broadcast
from pyspark.sql.functions import to_date, to_timestamp, year, month
from pyspark.sql.types import *
from utils.data_utils import download_and_cache, load_source_url


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create a Spark session
    """
    return SparkSession \
	        .builder \
	        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .config("spark.sql.shuffle.partitions",100) \
            .config("spark.debug.maxToStringFields", 100) \
	        .getOrCreate()

def is_alnum(x):
        """
        Return x if the value is alpha-numeric else return None (for iso codes)
        """
        if x: 
            return x if x.isalnum() else None
        else:
            return None

def quality_checks(data_table_name):
    """
    Performs two data checks on a table when called
    """
    
    if data_table_name is not None:
        if data_table_name.count() != 0:
            print('Table exists and contains records.')
        else:
            raise ValueError('Data Quality Check Failed: table exists, but does not contain records.')
    else:
        raise ValueError('Data Quality Check Failed: table does not exists.')


def process_airlines( spark, output_data):
    """
    Process airlines data
    1. Download airlines.csv
    2. Cleaning
    3. Quality Check
    4. Write airlines_table to S3
    """
    airlines = load_source_url('airlines')
    print( airlines)

    schema = StructType([
                StructField( "airline_id", IntegerType(), True),
                StructField( "name", StringType(), True),
                StructField( "alias", StringType(), True),
                StructField( "iata", StringType(), True),
                StructField( "icao", StringType(), True),
                StructField( "callsign", StringType(), True),
                StructField( "country", StringType(), True),
                StructField( "active", StringType(), True)])    

    df = spark \
            .read.format('csv') \
            .options( header='false') \
            .schema( schema) \
            .load( airlines.path)

    udf_isalnum = udf( is_alnum, StringType())
    df = df \
            .replace( '\\N', None) \
            .withColumn( 'icao', udf_isalnum('icao')) \
            .withColumn( 'iata', udf_isalnum('iata'))
    
    airlines_table = df \
            .filter( df.active == True)  \
            .select('icao', 'name', 'alias', 'iata', 'callsign', 'country') \
            .dropna( subset=['icao']) \
            .dropDuplicates(['icao'])

    airlines_table.printSchema()
    print("Number of rows: ", airlines_table.count())
    quality_checks( airlines_table)

    print("Airlines table to S3...")
    airlines_table.write.parquet( output_data + 'airlines/', mode='overwrite')


def process_airports( spark, input_data, output_data):
    """
    Process airports data
    1. Download airports.csv
    2. Cleaning
    3. Select airports_code and write it to S3 as a staging table
    4. Quality checks airports_table
    5. Write airports_table to S3
    """
    airports = load_source_url('airports')
    print( airports)

    airports_schema = StructType([
                StructField( "id", IntegerType(), True),
                StructField( "ident", StringType(), True),
                StructField( "type", StringType(), True),
                StructField( "name", StringType(), True),
                StructField( "latitude_deg", DoubleType(), True),
                StructField( "longitude_deg", DoubleType(), True),
                StructField( "elevation_ft", DoubleType(), True),
                StructField( "continent", StringType(), True),
                StructField( "iso_country", StringType(), True),
                StructField( "iso_region", StringType(), True),
                StructField( "municipality", StringType(), True),
                StructField( "scheduled_service", StringType(), True),
                StructField( "gps_code", StringType(), True),
                StructField( "iata_code", StringType(), True),
                StructField( "local_code", StringType(), True),
                StructField( "home_link", StringType(), True),
                StructField( "wikipedia_link", StringType(), True),
                StructField( "keywords", StringType(), True)])

    airports_df = spark \
                    .read.format('csv') \
                    .options( header='true') \
                    .schema( airports_schema) \
                    .load( airports.path) \
                    .replace( '-', None) \
                    .dropna( subset=['ident'])

    airports_table = airports_df \
                        .withColumn( 'scheduled_service', when( airports_df['scheduled_service'] == 'yes', lit(True)) \
                            .otherwise( lit(False))) \
                        .dropDuplicates( ['ident'])
    
    airports_table = airports_table.select('ident', 'type', 'name', 'latitude_deg', 'longitude_deg', 'elevation_ft',
                            'iso_country', 'iso_region', 'municipality', 'scheduled_service', 'gps_code',
                            'iata_code', 'local_code')

    airports_code = airports_table.select('ident', 'iso_country')
    quality_checks( airports_code)
    print(f"Airports code table to S3...")
    airports_code.write.parquet( input_data + '/airports_code/', mode='overwrite')
    
    airports_table.printSchema()
    print( "Number of rows:", airports_table.count())
    quality_checks( airports_table)

    print(f"Airports table to S3...")
    airports_table.write.parquet( output_data + 'airports/', mode='overwrite')


def process_countries( spark, output_data):
    """
    Process countries
    1. Download countries.csv
    2. Download covid19.csv
    3. Join the two tables with the name of the country as the key
    4. Create a table with the join
    5. Quality checks
    6. Write countries_table to S3
    """

    countries = load_source_url('countries')
    print( countries)

    countries_schema = StructType([
                StructField( "id", IntegerType(), True),
                StructField( "code", StringType(), True),
                StructField( "name", StringType(), True),
                StructField( "continent", StringType(), True),
                StructField( "wikipedia_link", StringType(), True),
                StructField( "keywords", StringType(), True)])

    df =  spark.read.format('csv') \
                .options( header='true') \
                .schema( countries_schema) \
                .load( countries.path)

    df = df \
            .dropna( subset=['code']) \
            .dropDuplicates(['code']) \
            .withColumnRenamed('code', 'iso_code2') \
            .withColumnRenamed( 'continent', 'iso_continent') \
            .select('iso_code2', 'name', 'iso_continent') \
        
    covid19 = load_source_url('owid-covid-data')

    df2 =  spark.read.format('csv') \
                .options( header='true') \
                .load( covid19.path)

    udf_isalnum = udf( is_alnum, StringType())
    df2 = df2 \
            .select('iso_code', 'continent', 'location', 'population') \
            .dropDuplicates(['iso_code', 'continent', 'location']) \
            .withColumn( 'iso_code3', udf_isalnum('iso_code')) \
            .withColumn( 'population', col('population').cast( DoubleType())) \
            .dropna( how='any', subset=['iso_code3', 'location'])

    countries_table = df \
                        .join( broadcast(df2),
                                df.name == df2.location,
                                how='left') \
                        .drop( 'location')

    countries_table = countries_table.select('iso_code2', 'iso_code3', 'name', 'iso_continent', 'continent', 'population')
    countries_table.printSchema()
    print("Total number of rows: ", countries_table.count())
    quality_checks( countries_table)

    print(f"Countries table to S3...")
    countries_table.write.parquet( output_data + 'countries/', mode='overwrite')


def process_opensky( spark, input_data, output_data):
    """
    Process Opensky 
    1. Download and load all the flights data    
    2. Cleaning
    3. Join with airport_codes table from staging to add iso_code for countries of origin and destination
    4. Cleaning
    5. Quality checks
    6. Write flights_table to S3
    """
    flightlists = pd.read_csv('./data/source_url.csv', index_col='name')
    opensky = flightlists.loc[ flightlists.index.str.contains('flightlist')]
    for idx in range( len(opensky)):
        download_and_cache( opensky.iloc[idx].name + '.csv.gz', opensky.iloc[idx].url)

    schema = StructType([
                StructField( "callsign", StringType(), True),
                StructField( "number", StringType(), True),
                StructField( "icao24", StringType(), True),
                StructField( "registration", StringType(), True),
                StructField( "typecode", StringType(), True),
                StructField( "origin", StringType(), True),
                StructField( "destination", StringType(), True),
                StructField( "firstseen", TimestampType(), True),
                StructField( "lastseen", TimestampType(), True),
                StructField( "day", DateType(), True),
                StructField( "latitude_1", DoubleType(), True),
                StructField( "longitude_1", DoubleType(), True),
                StructField( "altitude_1", DoubleType(), True),
                StructField( "latitude_2", DoubleType(), True),
                StructField( "longitude_2", DoubleType(), True),
                StructField( "altitude_2", DoubleType(), True)])

    df = spark \
            .read.format('csv') \
            .options( header='true') \
            .schema( schema) \
            .load( './data/source/flightlist*')

    cols = ['callsign', 'number', 'icao24', 'registration', 'typecode', 
            'ident_origin', 'ident_destination', 'firstseen_at', 'lastseen_at', 'day',
            'latitude_deg1', 'longitude_deg1', 'altitude_ft1', 'latitude_deg2', 'longitude_deg2', 'altitude_ft2',
            'callsign_icao', 'year', 'month']

    df = df \
            .dropna( subset=['day']) \
            .dropna( how='all', subset=['origin', 'destination']) \
            .withColumn( 'day', to_date( 'day', 'yyyy-MM-dd HH:mm:ss+mm:mm')) \
            .withColumn( 'callsign_icao', df['callsign'].substr( 1, 3)) \
            .withColumn( 'year', year('day')) \
            .withColumn( 'month', month('day')) \
            .dropDuplicates() \
            .toDF( *cols)

    airports_countries = spark.read.parquet( input_data + '/airports_code')

    flightlist_table = df \
                        .join(
                            broadcast( airports_countries),
                            df.ident_origin == airports_countries.ident,
                            how='left') \
                        .drop('ident') \
                        .withColumnRenamed('iso_country', 'iso_country_origin') \
                        .join(
                            broadcast( airports_countries),
                            df.ident_destination == airports_countries.ident,
                            how='left') \
                        .drop('ident') \
                        .withColumnRenamed('iso_country', 'iso_country_destination')

    flightlist_table = flightlist_table.select( 'year', 'month', 'day', 'callsign', 'callsign_icao', 'icao24', 'registration', 'typecode', \
                            'ident_origin', 'iso_country_origin', 'ident_destination', 'iso_country_destination', \
                            'firstseen_at', 'lastseen_at', 'latitude_deg1', 'longitude_deg1', 'altitude_ft1', \
                            'latitude_deg2', 'longitude_deg2', 'altitude_ft2')

    flightlist_table.printSchema()
    print("Number of rows: ", flightlist_table.count())
    quality_checks( flightlist_table)

    print(f"Flightlist table to S3...")
    flightlist_table.write.parquet( output_data + 'flightlist/', mode='overwrite', partitionBy=['year','month'])


def process_covid( spark, output_data):
    """
    Process covid19
    1. Load covid19.csv
    2. Cleaning + selecting relevant columns
    3. Quality checks
    4. Write covid19_table to S3
    """

    covid19 = load_source_url('owid-covid-data')
    print( covid19)

    df2 =  spark.read.format('csv') \
                .options( header='true') \
                .load( covid19.path)


    udf_isalnum = udf( is_alnum, StringType())
    covid19_table = df2 \
                    .dropDuplicates(['iso_code', 'date']) \
                    .withColumn( 'iso_code3', udf_isalnum('iso_code')) \
                    .dropna( how='any', subset=['iso_code3', 'location'])

    covid19_table = covid19_table.select('iso_code3', 'date', 'total_cases', 'new_cases',
                                       'new_cases_smoothed', 'total_deaths', 'new_deaths',
                                       'new_deaths_smoothed')    

    covid19_table = covid19_table.withColumn( 'date', to_date( 'date'))
    double_cols = ['total_cases', 'new_cases',
               'new_cases_smoothed', 'total_deaths', 'new_deaths',
               'new_deaths_smoothed']

    for col_name in double_cols:
        covid19_table = covid19_table.withColumn( col_name, col(col_name).cast( DoubleType()))

    covid19_table.printSchema()
    print("Number of rows: ", covid19_table.count())
    quality_checks( covid19_table)

    print("Covid19 table to S3...")
    covid19_table.write.parquet( output_data + 'covid19/', mode='overwrite')


def main():
    """
    1. Create a Spark Session
    2. For each of the tables: 
        * Read the data
        * Process the data to create tables
        * Write to parquet files to S3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-capstoneproject/staging/"
    output_data = "s3a://udacity-capstoneproject/marts/"

    process_airlines(spark, output_data)
    process_airports( spark, input_data, output_data)
    process_countries( spark, output_data)
    process_opensky( spark, input_data, output_data)
    process_covid( spark, output_data)

if __name__ == "__main__":
    main()
