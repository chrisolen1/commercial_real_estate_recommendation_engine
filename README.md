# biz_dev_recommendation_engine
Hybrid content-based, collaborative filtering approach to generating recommendations for businesses looking to open/expand in an urban location, using data from the City of Chicago

This repo does not include all of the data necessary to run the recommendation on one's own. Missing, as indicated in the hybrid_recommendation.py script is the local business data, which was obtained from a private source, and the education data, which can be obtained from the City of Chicago's data portal via CSV export or API call. 

Also, note that the Zillow API script included in this repo is designed to run periodically, preferably via crontab, so as to generate sufficient amounts of real estate data over a few months or weeks. 

# repo includes the following files:
1. address_book.csv - list of all addresses in cook county, il used to generate real estate data via zillow api
2. 
3. content_filtering_etl.py 
