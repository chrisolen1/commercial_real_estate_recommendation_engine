# biz_dev_recommendation_engine
Hybrid content-based, collaborative filtering approach to generating recommendations for businesses looking to open/expand in an urban location, using data from the City of Chicago

This repo does not include all of the data necessary to run the recommendation on one's own. Missing, as indicated in the hybrid_recommendation.py script is the local business data, which was obtained from a private source, and the education data, which can be obtained from the City of Chicago's data portal via csv export or api call. 

Also, note that the zillow api script included in this repo is designed to run periodically, preferably via crontab, so as to generate a sufficient amount of real estate data over a period of a few weeks or months. 

# repo includes the following files:
1. address_book.csv - list of all addresses in cook county, il used to generate real estate data via zillow api.
2. grid_generation_and_api.py - scripts to generate latitudinal/longitudinal grid/zones for which we will be generating recommendations and call apis to get business, educational, transit, real estate, and demographic features of each zone.
3. content_filtering_etl.py - script to transform raw-ish data into proper format for doing content-based filtering.
4. hybrid_recommendation.py - script to run content-based and collaborative filtering to generate grid/zone recommendations for a particular type of business. 
5. oltp_tables.sql - generates tables in mysql to house raw data from api calls. also includes a trigger for when updates are made to property table.
6. create_lookup_table.sql - generates grid/zone lookup tables in mysql for each operational table.

