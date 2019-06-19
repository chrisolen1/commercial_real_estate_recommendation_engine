import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
from sklearn import preprocessing
from sklearn.metrics.pairwise import cosine_similarity

### COLLABORATIVE FILTERING ###

"""
Generating item-to-item matrix
"""

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "business_test_2"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

biz_query = "SELECT * FROM business_train_2;"
biz_test = "SELECT * FROM business_test_2;"
cf_business = pd.read_sql_query(biz_query, con)
business_test = pd.read_sql_query(biz_test, con)

#Taking only first four digits in SIC
def first_four(row):
    return row[0:4]
cf_business['sic_4'] = cf_business['sic'].apply(first_four)
business_test['sic_4'] = business_test['sic'].apply(first_four)

#Dropping rows we don't need:
cf_business.drop(['zone_sic','sic','median_revenue','sd_revenue','median_emp_total',
               'sd_emp_total','median_emp_here','sd_emp_here','median_age','sd_age',
               'median_sqft','sd_sqft','n_bus'], axis=1, inplace=True)
business_test.drop(['zone_sic','sic','median_revenue','sd_revenue','median_emp_total',
               'sd_emp_total','median_emp_here','sd_emp_here','median_age','sd_age',
                'median_sqft','sd_sqft','n_bus'], axis=1, inplace=True)

#Adding revenue per sqft and using as a metric to "rate" different type of businesses (i.e. sic_4 codes)
cf_business['rev_per_sqft'] = cf_business['avg_revenue']/cf_business['avg_sqft']

"""
Revenue per square foot is highly skewed so we standardized and then grouped the distribution into 20% segments to align the data to a 1 to 5 rating system.
"""

#Isolating relevant fields to turn into a matrix
bus_reduced = cf_business[['sic_4','zone','rev_per_sqft']]

#Pivot of reduced business dataframe
bus_pivot = bus_reduced.pivot_table(index='sic_4', columns='zone', values='rev_per_sqft')

#Converting from pandas Dataframe to a matrix for calculation purposes
bus_matrix= bus_pivot.values

#Standardizing the average revenue per square foot
stscaler = StandardScaler().fit(bus_matrix)
standardized_data = stscaler.transform(bus_matrix)

"""
Even with standardizing, we found collaborative filtering to have poor predictions on continuous variables so we added an 
additional layer of abstraction to simplify whether average revenue per sqft was low - high based on a 1-5 rating scale. 
Each value from 1 to 5 represents ~20% of the distribution of average revenue per sqft.
"""

#Function to group standardized values into 20% segments from low(1) to high(5)
def buckets(x):
    if x <= -0.84:
        x = 1
    elif  -0.84 < x <= -0.25:
        x = 2
    elif  -0.25 < x <= 0.26:
        x = 3
    elif  0.26 < x <= 0.85:
        x = 4
    elif  0.85 < x:
        x = 5
    return x

#Applying function to segment standardized data to rating scale from 1-5
ranked_data = pd.DataFrame(standardized_data, columns = bus_pivot.columns, index = bus_pivot.index).applymap(buckets)

#Replace nan values with zero
ranked_matrix = ranked_data.replace(np.nan, 0)

#Calculating the cosine similarity for items (i.e. business groups) 
item_sim = cosine_similarity(ranked_matrix)

#Function to predict rating for all business-zone pair and returns long dataframe of ratings
def predict(rated_df, similarity, sic_code,type='item'):
    if type == 'item':
        pred = similarity.dot(rated_df)/np.array([np.abs(similarity).sum(axis=1)]).T
        pred = pd.DataFrame(pred, columns= rated_df.columns, index = rated_df.index)
        #Unpivoting dataframe
        cos_ranked_pred = pd.melt(pred.reset_index(),id_vars=['sic_4'])
        #sorting predictions for highest rated businesses
        cos_ranked_pred = cos_ranked_pred.sort_values(by=['sic_4','value'],ascending=False)
        #creating unique business-zone combination
        cos_ranked_pred['sic_zone'] = cos_ranked_pred['sic_4'] + cos_ranked_pred['zone'].astype(str)
        #ranked predictions for all zones with sic code 5411 (grocery stores)
        cos_pred = cos_ranked_pred[cos_ranked_pred['sic_4'] ==sic_code]
        #ranking recommendations
        cos_pred['rank'] = cos_pred['value'].rank().astype(int)
    elif type == 'user':
        pred = similarity.dot(rated_df.T)/np.array([np.abs(similarity).sum(axis=1)]).T
        pred = pd.DataFrame(pred, columns= rated_df.index, index = rated_df.columns)
        #unpivoting dataframe
        cos_ranked_pred = pd.melt(pred.reset_index(),id_vars=['zone'])
        #sorting predictions for highest rated businesses
        cos_ranked_pred = cos_ranked_pred.sort_values(by=['sic_4','value'],ascending=False)
        #creating unique business-zone combination
        cos_ranked_pred['sic_zone'] = cos_ranked_pred['sic_4'] + cos_ranked_pred['zone'].astype(str)
        #ranked predictions for all zones with sic code 5411 (grocery stores)
        cos_pred = cos_ranked_pred[cos_ranked_pred['sic_4'] ==sic_code]
        #ranking recommendations
        cos_pred['rank'] = cos_pred['value'].rank().astype(int)
    return cos_pred

pd.options.mode.chained_assignment = None
#Ranked predictions for all zones with sic code 5411 (grocery stores)
cos_pred = predict(ranked_matrix,item_sim,'5411')

"""
Now let's exclude zones where that business type already exists...
"""

#Creating column of unique sic code to zone number
bus_reduced['sic_zone'] = bus_reduced['sic_4'] + bus_reduced['zone'].astype(str)

#List of existing business-zone combinations
bus_exists = bus_reduced['sic_zone'].unique()

#Ranked predictions for businesses that currently do not exist in a zone
cos_pred_new = cos_pred[~cos_pred['sic_zone'].isin(bus_exists)]

### CONTENT-BASED FILTERING ###

"""
First we define the functions that will help us carry out our user preferences survey
"""

#CRIME SURVEY

#Define a function that updates the customer vector based on stated crime preferences:
def crime_filtering(crime_response, customer_vect, content, crime_types):
    low_threat = crime_types[0:5]
    high_threat = crime_types[5:]
    if crime_response == 'A':
        #low_threat_tolerance = .25
        #high_threat_tolerance = .1
        for i in range(len(low_threat)):
            customer_vect[low_threat[i]] = content[low_threat[i]].min()
        for i in range(len(high_threat)):
            customer_vect[high_threat[i]] = content[high_threat[i]].min()
        return customer_vect
    elif crime_response == 'B':
        low_threat_tolerance = .5
        high_threat_tolerance = .25
        for i in range(len(low_threat)):
            customer_vect[low_threat[i]] = content[low_threat[i]].quantile(low_threat_tolerance)
        for i in range(len(high_threat)):
            customer_vect[high_threat[i]] = content[high_threat[i]].quantile(high_threat_tolerance)
        return customer_vect
    else:
        low_threat_tolerance = .75
        high_threat_tolerance = .5
        for i in range(len(low_threat)):
            customer_vect[low_threat[i]] = content[low_threat[i]].quantile(low_threat_tolerance)
        for i in range(len(high_threat)):
            customer_vect[high_threat[i]] = content[high_threat[i]].quantile(high_threat_tolerance)
        return customer_vect
    
    
#BUSINESS ENVIRONMENT SURVEY:

#Define a function that updates the customer vector based on stated business environment preferences:
def business_filtering(response, customer_vect, content, sic_codes):
    if response == 'YES':
        for i in range(len(sic_codes)):
            customer_vect[sic_codes[i]] = content[sic_codes[i]].max()
        return customer_vect
    else:
        for i in range(len(sic_codes)):
            customer_vect[sic_codes[i]] = 0
        return customer_vect
    
    
#PUBLIC TRANSPORTATION SURVEY:

#Define a function that updates the customer vector based on stated transit preferences:
def transit_filtering(transit_response, customer_vect, content, transit_types):
    if transit_response == 'A':
        customer_vect[transit_types[0]] = content[transit_types[0]].quantile(.99)
        customer_vect[transit_types[1]] = content[transit_types[1]].quantile(.9)
        return customer_vect
    elif transit_response == 'B':
        customer_vect[transit_types[0]] = content[transit_types[0]].quantile(.92)
        customer_vect[transit_types[1]] = content[transit_types[1]].quantile(.5)
        return customer_vect
    else:
        customer_vect[transit_types[0]] = content[transit_types[0]].quantile(.5)
        customer_vect[transit_types[1]] = content[transit_types[1]].quantile(.25)
        return customer_vect
    
#SCHOOLS SURVEY:

#Define a function that updates the customer vector based on stated school quantity preferences:
def school_quantity_filtering(school_quantity_response, customer_vect, content):
    if school_quantity_response == 'A':
        customer_vect['n_schools'] = content['n_schools'].quantile(.9)
        return customer_vect
    else:
        customer_vect['n_schools'] = content['n_schools'].quantile(.75)
        return customer_vect
    
#Define a function that updates the customer vector based on stated school quality preferences:
def school_quality_filtering(school_quality_response, customer_vect, content, quality_types):
    if school_quality_response == 'A':
        customer_vect[quality_types[0]] = content[quality_types[0]].quantile(.85)
        customer_vect[quality_types[1]] = content[quality_types[1]].quantile(.85)
        return customer_vect
    else:
        customer_vect[quality_types[0]] = content[quality_types[0]].quantile(.5)
        customer_vect[quality_types[1]] = content[quality_types[1]].quantile(.5)
        return customer_vect
    
    
#POPULATION DENSITY SURVEY:
    
#Define a function that updates the customer vector based on stated density preferences:
def density_filtering(density_response, customer_vect, content, density_types):
    if density_response == 'A':
        customer_vect[density_types[0]] = content[density_types[0]].max()
        customer_vect[density_types[1]] = content[density_types[1]].max()
        return customer_vect
    elif density_response == 'B':
        customer_vect[density_types[0]] = content[density_types[0]].max()
        customer_vect[density_types[1]] = content[density_types[1]].min()
        return customer_vect
    elif density_response == 'C':
        customer_vect[density_types[0]] = content[density_types[0]].min()
        customer_vect[density_types[1]] = content[density_types[1]].max()
        return customer_vect
    elif density_response == 'D':
        customer_vect[density_types[0]] = content[density_types[0]].min()
        customer_vect[density_types[1]] = content[density_types[1]].min()
        return customer_vect
    else:
        customer_vect[density_types[0]] = content[density_types[0]].quantile(.5)
        customer_vect[density_types[1]] = content[density_types[1]].quantile(.5)
        return customer_vect
    
#PROPERTY VALUE SURVEY:
    
#Define a function that updates the customer vector based on stated property value preferences:
def property_value_filtering(property_value_response, customer_vect, content):
    if property_value_response == 'A':
        customer_vect['sqftValue'] = content['sqftValue'].max()
        return customer_vect
    elif property_value_response == 'B':
        customer_vect['sqftValue'] = content['sqftValue'].quantile(.5)
        return customer_vect
    else:
        customer_vect['sqftValue'] = content['sqftValue'].min()
        return customer_vect
    
#PROPERTY SIZE SURVEY:
    
##Define a function that updates the customer vector based on stated property size preferences:
def property_size_filtering(property_size_response, customer_vect, content, size_types):
    if property_size_response == 'A':
        customer_vect[size_types[0]] = content[size_types[0]].max()
        customer_vect[size_types[1]] = content[size_types[1]].max()
        return customer_vect
    elif property_size_response == 'B':
        customer_vect[size_types[0]] = content[size_types[0]].quantile(.5)
        customer_vect[size_types[1]] = content[size_types[1]].quantile(.5)
        return customer_vect
    else:
        customer_vect[size_types[0]] = content[size_types[0]].min()
        customer_vect[size_types[1]] = content[size_types[1]].min()
        return customer_vect
    
    
#LOT SIZE SURVEY:
    
#Define a function that updates the customer vector based on stated property size preferences:
def lot_size_filtering(lot_size_response, customer_vect, content):
    if lot_size_response == 'A':
        customer_vect['lotSize'] = content['lotSize'].max()
        return customer_vect
    elif lot_size_response == 'B':
        customer_vect['lotSize'] = content['lotSize'].quantile(.5)
        return customer_vect
    else:
        customer_vect['lotSize'] = content['lotSize'].min()
        return customer_vect
    
    
#PROPERTY AGE SURVEY:
    
#Define a function that updates the customer vector based on stated house age preferences:
def house_age_filtering(house_age_response, customer_vect, content):
    if house_age_response == 'A':
        customer_vect['houseAge'] = content['houseAge'].max()
        return customer_vect
    elif house_age_response == 'B':
        customer_vect['houseAge'] = content['houseAge'].quantile(.5)
        return customer_vect
    else:
        customer_vect['houseAge'] = content['houseAge'].min()
        return customer_vect
    
#PROPERTY MARKET HOTNESS SURVEY:
    
#Define a function that updates the customer vector based on stated property market preferences:
def market_hot_filtering(market_hot_response, customer_vect, content):
    if market_hot_response == 'A':
        customer_vect['lastSold'] = content['lastSold'].min()
        return customer_vect
    elif market_hot_response == 'B':
        customer_vect['lastSold'] = content['lastSold'].quantile(.5)
        return customer_vect
    else:
        customer_vect['lastSold'] = content['lastSold'].max()
        return customer_vect

#AGE OF BUSINESSES SURVEY:
    
#Define a function that updates the customer vector based on stated business age preferences:    
def business_age_filtering(business_age_response, customer_vect, content):
    if business_age_response == 'A':
        customer_vect['avg_age'] = content['avg_age'].quantile(.9)
        return customer_vect
    elif business_age_response == 'B':
        customer_vect['avg_age'] = content['avg_age'].quantile(.5)
        return customer_vect
    else:
        customer_vect['avg_age'] = content['avg_age'].quantile(.1)
        return customer_vect
    
#SIZE OF BUSINESSES SURVEY:
    
#Define a function that updates the customer vector based on stated business size preferences:    
def business_size_filtering(business_size_response, customer_vect, content):
    if business_size_response == 'A':
        customer_vect['avg_emp_here'] = content['avg_emp_here'].max()
        return customer_vect
    elif business_size_response == 'B':
        customer_vect['avg_emp_here'] = content['avg_emp_here'].quantile(.5)
        return customer_vect
    else:
        customer_vect['avg_emp_here'] = content['avg_emp_here'].min()
        return customer_vect
    
#ETHNIC DEMOGRAPHICS SURVEY: 
    
#Define a function that updates the customer vector based on stated ethnic demographic preferences:
def ethnic_demographics_filtering(ethnic_demographics_response, customer_vect, content, ethnicity_types):
    if ethnic_demographics_response == 'A':
        customer_vect[ethnicity_types[0]] = content[ethnicity_types[0]].max()
        customer_vect[ethnicity_types[1]] = content[ethnicity_types[1]].min()
        customer_vect[ethnicity_types[2]] = content[ethnicity_types[2]].min()
        customer_vect[ethnicity_types[3]] = content[ethnicity_types[3]].min()
        customer_vect[ethnicity_types[4]] = content[ethnicity_types[4]].min()
        return customer_vect
    elif ethnic_demographics_response == 'B':
        customer_vect[ethnicity_types[0]] = content[ethnicity_types[0]].min()
        customer_vect[ethnicity_types[1]] = content[ethnicity_types[1]].max()
        customer_vect[ethnicity_types[2]] = content[ethnicity_types[2]].min()
        customer_vect[ethnicity_types[3]] = content[ethnicity_types[3]].min()
        customer_vect[ethnicity_types[4]] = content[ethnicity_types[4]].min()
        return customer_vect
    elif ethnic_demographics_response == 'C':
        customer_vect[ethnicity_types[0]] = content[ethnicity_types[0]].min()
        customer_vect[ethnicity_types[1]] = content[ethnicity_types[1]].min()
        customer_vect[ethnicity_types[2]] = content[ethnicity_types[2]].max()
        customer_vect[ethnicity_types[3]] = content[ethnicity_types[3]].min()
        customer_vect[ethnicity_types[4]] = content[ethnicity_types[4]].min()
        return customer_vect
    else:
        customer_vect[ethnicity_types[0]] = content[ethnicity_types[0]].min()
        customer_vect[ethnicity_types[1]] = content[ethnicity_types[1]].min()
        customer_vect[ethnicity_types[2]] = content[ethnicity_types[2]].min()
        customer_vect[ethnicity_types[3]] = content[ethnicity_types[3]].max()
        customer_vect[ethnicity_types[4]] = content[ethnicity_types[4]].max()
        return customer_vect
    
#MEDIAN INCOME SURVEY:
    
#Define a function that updates the customer vector based on stated local median income preferences:
def median_income_filtering(median_income_response, customer_vect, content):
    if median_income_response == 'A':
        customer_vect['median_total'] = content['median_total'].max()
        return customer_vect
    elif median_income_response == 'B':
        customer_vect['median_total'] = content['median_total'].quantile(.5)
        return customer_vect
    else:
        customer_vect['median_total'] = content['median_total'].min()
        return customer_vect
    
#HOUSEHOLD SIZE SURVEY:
    
#Define a function that updates the customer vector based on stated local household size preferences:
def household_size_filtering(household_size_response, customer_vect, content):
    if household_size_response == 'A':
        customer_vect['ave_household_size_total'] = content['ave_household_size_total'].max()
        return customer_vect
    else:
        customer_vect['ave_household_size_total'] = content['ave_household_size_total'].min()
        return customer_vect
    
#MEDIAN AGE SURVEY:
    
##Define a function that updates the customer vector based on stated age demographics preferences:
def median_age_filtering(median_age_response, customer_vect, content, age_types):
    if median_age_response == 'A':
        customer_vect[age_types[0]] = content[age_types[0]].max()
        customer_vect[age_types[1]] = content[age_types[1]].max()
        return customer_vect
    else:
        customer_vect[age_types[0]] = content[age_types[0]].min()
        customer_vect[age_types[1]] = content[age_types[1]].min()
        return customer_vect
    
#ETHNIC HOMOGENEITY SURVEY:
    
#Define a function that updates the customer vector based on stated ethnic homogeneity preferences:
def homogeneity_filtering(homogeneity_response, customer_vect, content):
    if homogeneity_response == 'A':
        customer_vect['monolithic'] = content['monolithic'].max()
        return customer_vect
    else:
        customer_vect['monolithic'] = content['monolithic'].min()
        return customer_vect

"""
THEN WE PULL IN THE CONTENT MATRIX GENERATED DURING ETL
"""

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "chicago_olap"
table = "zones_content_filtering"

#Establishing connection with analytical db:
engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

query = "SELECT * FROM zones_content_filtering;"
content = pd.read_sql_query(query, con)

#Creating empty customer vector:
customer_vect = pd.Series(np.repeat(np.NaN,len(content.columns)),index=content.columns)

#Dropping 'last_updated' timestamp:
content.drop(['updated_at'],axis=1, inplace=True)
customer_vect.drop(['updated_at'], inplace=True)

#Reindexing content matrix using zone numbers and droping zone column:
content.set_index('zone', inplace=True)

"""
AND AS THE USER ANSWERS A SERIES OF QUESTIONS SPECIFIC TO THEIR PREFERENCES, A CUSTOMER PREFERENCE VECTOR GETS CONTINUOUSLY UPDATED ALONG 
WITH THE CONTENT MATRIX
"""

#Crime survey question:
crime_response = input("What affect does crime have on the success of your business?\n \
      A - Almost ANY Crime poses a grave risk to the success of my business\n \
      B - Most crime poses a big risk to the success of my business\n \
      C - Crime has little effect on the success of my business\n \
      D - Crime has no effect on the success of my business\n \
      Answer A, B, C, or D here: ")

#Defining crime types for easier indexing:
crime_types = ['domestic','arrest','crime_type_DRUGS','crime_type_SEX_CRIME',
                'crime_type_OTHER','crime_type_PROPERTY_CRIME','crime_type_STEALING',
                'crime_type_VIOLENT','crime_type_HOMICIDE']

#Updating customer vector if survey response is A B, or C:
if crime_response != 'D':
    customer_vect = crime_filtering(crime_response, customer_vect, content, crime_types)
  
#If customer is crime agnostic, eliminating crime as a factor from both content matrix and customer vect:    
else:
    content.drop(crime_types, axis=1, inplace=True)
    customer_vect.drop(crime_types, inplace=True)

#QUESTION 1:
bus_res1 = input("Do you want your business to be in an area with AGRICULTURE, FOREST, AND FISHING companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res1_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_0')])

#Updating customer vector if response was YES or NO:
if bus_res1 != 'N/A':
    customer_vect = business_filtering(bus_res1, customer_vect, content, res1_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res1_sic, axis=1, inplace=True)
    customer_vect.drop(res1_sic, inplace=True)

#QUESTION 2:
bus_res2 = input("Do you want your business to be in an area with MINING, OIL, AND GAS companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res2_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_10')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_11')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_12')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_13')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_14')]) 

#Updating customer vector if response was YES or NO:
if bus_res2 != 'N/A':
    customer_vect = business_filtering(bus_res2, customer_vect, content, res2_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res2_sic, axis=1, inplace=True)
    customer_vect.drop(res2_sic, inplace=True)
    
#QUESTION 3:
bus_res3 = input("Do you want your business to be in an area with BUILDING CONTRACTOR, CONSTRUCTION, AND UTILITY companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res3_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_15')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_16')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_17')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_18')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_19')])

#Updating customer vector if response was YES or NO:
if bus_res3 != 'N/A':
    customer_vect = business_filtering(bus_res3, customer_vect, content, res3_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res3_sic, axis=1, inplace=True)
    customer_vect.drop(res3_sic, inplace=True)
    
#QUESTION 4:
bus_res4 = input("Do you want your business to be in an area with FOOD, BEVERAGE, AND TOBACCO PRODUCTION companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res4_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_20')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_21')])

#Updating customer vector if response was YES or NO:
if bus_res4 != 'N/A':
    customer_vect = business_filtering(bus_res4, customer_vect, content, res4_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res4_sic, axis=1, inplace=True)
    customer_vect.drop(res4_sic, inplace=True)
    
#QUESTION 5:
bus_res5 = input("Do you want your business to be in an area with TEXTILE, WOOD, PAPER, OR PRINTING MANUFACTURING companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res5_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_22')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_23')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_24')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_25')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_26')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_27')])

#Updating customer vector if response was YES or NO:
if bus_res5 != 'N/A':
    customer_vect = business_filtering(bus_res5, customer_vect, content, res5_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res5_sic, axis=1, inplace=True)
    customer_vect.drop(res5_sic, inplace=True)
    
#QUESTION 6:
bus_res6 = input("Do you want your business to be in an area with HEAVY DUTY (e.g. CHEMICAL, GLASS, RUBBER, METAL, MACHINERY, ELECTRONIC) MANUFACTURING companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res6_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_28')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_29')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_3')])

#Updating customer vector if response was YES or NO:
if bus_res6 != 'N/A':
    customer_vect = business_filtering(bus_res6, customer_vect, content, res6_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res6_sic, axis=1, inplace=True)
    customer_vect.drop(res6_sic, inplace=True)
    
#QUESTION 7:
bus_res7 = input("Do you want your business to be in an area with TRANSPORTATION AND LOGISTICS companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res7_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_4')])

#Updating customer vector if response was YES or NO:
if bus_res7 != 'N/A':
    customer_vect = business_filtering(bus_res7, customer_vect, content, res7_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res7_sic, axis=1, inplace=True)
    customer_vect.drop(res7_sic, inplace=True)

#QUESTION 8:
bus_res8 = input("Do you want your business to be in an area with WHOLESALE companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res8_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_50')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_51')])

#Updating customer vector if response was YES or NO:
if bus_res8 != 'N/A':
    customer_vect = business_filtering(bus_res8, customer_vect, content, res8_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res8_sic, axis=1, inplace=True)
    customer_vect.drop(res8_sic, inplace=True)
    
#QUESTION 9:
bus_res9 = input("Do you want your business to be in an area with GENERAL (e.g. APPAREL, ELECTRONIC, HOMEGOODS) RETAIL companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res9_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_52')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_53')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_56')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_57')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_59')])

#Updating customer vector if response was YES or NO:
if bus_res9 != 'N/A':
    customer_vect = business_filtering(bus_res9, customer_vect, content, res9_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res9_sic, axis=1, inplace=True)
    customer_vect.drop(res9_sic, inplace=True)
    
#QUESTION 10:
bus_res10 = input("Do you want your business to be in an area with GROCERY STORES?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res10_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_54')]) 

#Updating customer vector if response was YES or NO:
if bus_res10 != 'N/A':
    customer_vect = business_filtering(bus_res10, customer_vect, content, res10_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res10_sic, axis=1, inplace=True)
    customer_vect.drop(res10_sic, inplace=True)
    
#QUESTION 11:
bus_res11 = input("Do you want your business to be in an area with AUTO RETAIL AND REPAIR companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res11_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_55')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_75')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_76')])

#Updating customer vector if response was YES or NO:
if bus_res11 != 'N/A':
    customer_vect = business_filtering(bus_res11, customer_vect, content, res11_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res11_sic, axis=1, inplace=True)
    customer_vect.drop(res11_sic, inplace=True)
    
#QUESTION 12:
bus_res12 = input("Do you want your business to be in an area with RESTAURANTS AND BARS?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res12_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_58')])

#Updating customer vector if response was YES or NO:
if bus_res12 != 'N/A':
    customer_vect = business_filtering(bus_res12, customer_vect, content, res12_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res12_sic, axis=1, inplace=True)
    customer_vect.drop(res12_sic, inplace=True)
    
#QUESTION 13:
bus_res13 = input("Do you want your business to be in an area with COMMERCIAL BANKING AND LENDING companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res13_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_60')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_61')])

#Updating customer vector if response was YES or NO:
if bus_res13 != 'N/A':
    customer_vect = business_filtering(bus_res13, customer_vect, content, res13_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res13_sic, axis=1, inplace=True)
    customer_vect.drop(res13_sic, inplace=True)
    
#QUESTION 14:
bus_res14 = input("Do you want your business to be in an area with FINANCIAL AND INSURANCE SERVICES companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res14_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_62')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_63')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_64')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_67')]) 

#Updating customer vector if response was YES or NO:
if bus_res14 != 'N/A':
    customer_vect = business_filtering(bus_res14, customer_vect, content, res14_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res14_sic, axis=1, inplace=True)
    customer_vect.drop(res14_sic, inplace=True)
    
#QUESTION 15:
bus_res15 = input("Do you want your business to be in an area with REAL ESTATE SERVICES companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res15_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_65')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_66')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_68')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_69')])

#Updating customer vector if response was YES or NO:
if bus_res15 != 'N/A':
    customer_vect = business_filtering(bus_res15, customer_vect, content, res15_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res15_sic, axis=1, inplace=True)
    customer_vect.drop(res15_sic, inplace=True)
    
#QUESTION 16:
bus_res16 = input("Do you want your business to be in an area with HOSPITALITY (e.g. HOTEL AND MOTEL) companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res16_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_70')])

#Updating customer vector if response was YES or NO:
if bus_res16 != 'N/A':
    customer_vect = business_filtering(bus_res16, customer_vect, content, res16_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res16_sic, axis=1, inplace=True)
    customer_vect.drop(res16_sic, inplace=True)
    
#QUESTION 17:
bus_res17 = input("Do you want your business to be in an area with TECH AND ENGINEERING companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res17_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_737')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_870')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_871')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_872')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_873')])

#Updating customer vector if response was YES or NO:
if bus_res17 != 'N/A':
    customer_vect = business_filtering(bus_res17, customer_vect, content, res17_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res17_sic, axis=1, inplace=True)
    customer_vect.drop(res17_sic, inplace=True)
    
#QUESTION 18:
bus_res18 = input("Do you want your business to be in an area with MEDICAL SERVICES companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res18_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_8')])

#Updating customer vector if response was YES or NO:
if bus_res18 != 'N/A':
    customer_vect = business_filtering(bus_res18, customer_vect, content, res18_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res18_sic, axis=1, inplace=True)
    customer_vect.drop(res18_sic, inplace=True)
    
#QUESTION 19:
bus_res19 = input("Do you want your business to be in an area with OTHER SERVICES (e.g. ADVERTISING, LEGAL, EDU, SOCIAL SERVICES, MISC) companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res19_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_71')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_72')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_730')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_731')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_732')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_733')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_734')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_735')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_736')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_738')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_739')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_77')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_81')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_82')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_83')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_84')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_85')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_86')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_874')])
            
#Updating customer vector if response was YES or NO:
if bus_res19 != 'N/A':
    customer_vect = business_filtering(bus_res19, customer_vect, content, res19_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res19_sic, axis=1, inplace=True)
    customer_vect.drop(res19_sic, inplace=True)
    
#QUESTION 20: 
bus_res20 = input("Do you want your business to be in an area with AMUSEMENT SERVICES (e.g. SPORTS, THEATER, AMUSEMENT PARK, ARCADE) companies?\nAnswer YES, NO, or N/A here: ")

#Creating a list of relevant sic codes:
res20_sic = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_78')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_79')])
            
#Updating customer vector if response was YES or NO:
if bus_res20 != 'N/A':
    customer_vect = business_filtering(bus_res20, customer_vect, content, res20_sic)

#If customer is ambilvalent to these industries, eliminating them as a factor from both content matrix and customer vect:    
else:
    content.drop(res20_sic, axis=1, inplace=True)
    customer_vect.drop(res20_sic, inplace=True)

sics_to_drop = list(content.columns[pd.Series(content.columns).str.startswith('sic_4_88')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_89')]) + \
            list(content.columns[pd.Series(content.columns).str.startswith('sic_4_9')])
content.drop(sics_to_drop, axis = 1, inplace = True)
customer_vect.drop(sics_to_drop, inplace=True)

#Transit survey question:
transit_response = input("How important is proximity to public transportation?\n \
      A - Crucially important; we can't succeed with out it\n \
      B - Very important\n \
      C - A nice perk, but not very important\n \
      D - Not important\n \
      Answer A, B, C, or D here: ")

#Defining transit types for easier indexing:
transit_types = ['el','bus']

#Updating customer vector if survey response is A B, or C:
if transit_response != 'D':
    customer_vect = transit_filtering(transit_response, customer_vect, content, transit_types)
  
#If customer is transit agnostic, eliminating transit as a factor from both content matrix and customer vect:    
else:
    content.drop(transit_types, axis=1, inplace=True)
    customer_vect.drop(transit_types, inplace=True)

#Quantity of schools survey question:
school_quantity_response = input("How important is proximity to schools?\n \
      A - Very important\n \
      B - Somewhat important\n \
      C - Not important\n \
      Answer A, B, or C here: ")

#Change the customer vector if the user selects A or B:
if school_quantity_response != 'C':
    customer_vect = school_quantity_filtering(school_quantity_response, customer_vect, content)

#If customer is school quantity agnostic, eliminating n_schools as a factor from both content matrix and customer vect:    
else:
    content.drop(['n_schools'], axis=1, inplace=True)
    customer_vect.drop(['n_schools'], inplace=True)

#Quality of schools survey question:
school_quality_response = input("How important is the quality of schools near your business?\n \
      A - Very important\n \
      B - Somewhat important\n \
      C - Not important\n \
      Answer A, B, or C here: ")

#Create a list of school quality metrics:
quality_types = ['avg_rating', 'awards']

#Change the customer vector if the user selects A or B:
if school_quality_response != 'C':
    content.dropna(subset = quality_types, inplace=True)
    customer_vect = school_quality_filtering(school_quality_response, customer_vect, content, quality_types)

#If customer is school quality agnostic, eliminating quality metrics as a factor from both content matrix and customer vect:    
else:
    content.drop(quality_types, axis=1, inplace=True)
    customer_vect.drop(quality_types, inplace=True)

#Density survey question:
density_response = input("Which of the following describes the location you would like to be in?\n \
      A - Densely commercial and residential \n \
      B - Densely commerical, but sparsely residential \n \
      C - Densely residential, but sparsely commercial \n \
      D - Sparsely commercial and residential \n \
      E - Moderately dense as far as commercial and residential development \n \
      F - Density is not important \
      Answer A, B, C, D, E, or F here: ")

density_types = ['n_bus','n_properties']

#Updating customer vector if survey response is A,B,C,D; note that null rows are also removed from content matrix:
if density_response != 'F':
    content.dropna(subset = ['n_properties'], inplace=True)
    customer_vect = density_filtering(density_response, customer_vect, content, density_types)
  
#If customer is density agnostic, eliminating density as a factor from both content matrix and customer vect:    
else:
    content.drop(density_types, axis=1, inplace=True)
    customer_vect.drop(density_types, inplace=True)

#Property value survey question:
property_value_response = input("Regarding property values near your place of business, which of the following is true?:\n \
      A - I'm looking for property values on the high end  \n \
      B - I'm looking for near-median property values \n \
      C - I'm looking for property values on the low end \n \
      D - Property values are not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if property_value_response != 'D':
    content.dropna(subset = ['sqftValue'], inplace=True)
    customer_vect = property_value_filtering(property_value_response, customer_vect, content)
  
#If customer is property value agnostic, eliminating propery value as a factor from both content matrix and customer vect:    
else:
    content.drop(['sqftValue'], axis=1, inplace=True)
    customer_vect.drop(['sqftValue'], inplace=True)

#Property size survey question:
property_size_response = input("What are your preferences as far as property sizes near your place of business:\n \
      A - I want to be near larger properties  \n \
      B - I want to be near medium-sized properties \n \
      C - I want to be near small properties \n \
      D - Property size is not important to me \n \
      Answer A, B, C, or D here: ")

size_types = ['avg_sqft','houseSize']

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if property_size_response != 'D':
    content.dropna(subset = size_types, inplace=True)
    customer_vect = property_size_filtering(property_size_response, customer_vect, content, size_types)
  
#If customer is property size agnostic, eliminating propery size as a factor from both content matrix and customer vect:    
else:
    content.drop(size_types, axis=1, inplace=True)
    customer_vect.drop(size_types, inplace=True)

#Property size survey question:
lot_size_response = input("What are your preferences as far as lot sizes near your place of business:\n \
      A - I want to be near larger lots  \n \
      B - I want to be near medium-sized lots \n \
      C - I want to be near small lots \n \
      D - Lot size is not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if lot_size_response != 'D':
    content.dropna(subset = ['lotSize'], inplace=True)
    customer_vect = lot_size_filtering(lot_size_response, customer_vect, content)
  
#If customer is property size agnostic, eliminating propery size as a factor from both content matrix and customer vect:    
else:
    content.drop(['lotSize'], axis=1, inplace=True)
    customer_vect.drop(['lotSize'], inplace=True)

#House age survey question:
house_age_response = input("What are your preferences as far as the age of houses near your place of business:\n \
      A - I want to be near older homes  \n \
      B - I want to be near medium-aged homes \n \
      C - I want to be near newer homes \n \
      D - House age is not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if house_age_response != 'D':
    content.dropna(subset = ['houseAge'], inplace=True)
    customer_vect = house_age_filtering(house_age_response, customer_vect, content)
  
#If customer is house age agnostic, eliminating house age as a factor from both content matrix and customer vect:    
else:
    content.drop(['houseAge'], axis=1, inplace=True)
    customer_vect.drop(['houseAge'], inplace=True)

#Property market hotness survey question:
market_hot_response = input("How hot do you want the housing market to be in your area?:\n \
      A - I want a very dynamic housing market  \n \
      B - I want a moderately dynamic housing market \n \
      C - I want a relatively stagnate housing market \n \
      D - The housing market in my area of business is not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if market_hot_response != 'D':
    content.dropna(subset = ['lastSold'], inplace=True)
    customer_vect = market_hot_filtering(market_hot_response, customer_vect, content)
  
#If customer is property market agnostic, eliminating property market hotness as a factor from both content matrix and customer vect:    
else:
    content.drop(['lastSold'], axis=1, inplace=True)
    customer_vect.drop(['lastSold'], inplace=True)

#Business age survey question:
business_age_response = input("How established would you like the other businesses in your area of business?:\n \
      A - I want to be near well-established businesses  \n \
      B - I want to be near moderately-established businesses \n \
      C - I want to be in an area with relatively new businesses \n \
      D - The age of businessees in my area is not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if business_age_response != 'D':
    content.dropna(subset = ['avg_age'], inplace=True)
    customer_vect = business_age_filtering(business_age_response, customer_vect, content)
  
#If customer is property market agnostic, eliminating property market hotness as a factor from both content matrix and customer vect:    
else:
    content.drop(['avg_age'], axis=1, inplace=True)
    customer_vect.drop(['avg_age'], inplace=True)

#Business size survey question:
business_size_response = input("How large would you like the other businesses in your area of business?:\n \
      A - I want to be near large businesses  \n \
      B - I want to be near moderately-sized businesses \n \
      C - I want to be in an area with relatively small businesses \n \
      D - The size of businessees in my area is not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if business_size_response != 'D':
    content.dropna(subset = ['avg_emp_here'], inplace=True)
    customer_vect = business_size_filtering(business_size_response, customer_vect, content)
  
#If customer is business size agnostic, eliminating business size as a factor from both content matrix and customer vect:    
else:
    content.drop(['avg_emp_here'], axis=1, inplace=True)
    customer_vect.drop(['avg_emp_here'], inplace=True)

#Ethnic demographics survey question: 
ethnic_demographics_response = input("What is your target ethnic demographic?:\n \
      A - Caucasian/Latino  \n \
      B - African American \n \
      C - Asian \n \
      D - Native and/or Hawaiian \n \
      E - Ethnic demographics are not important to me \n \
      Answer A, B, C, D, E here: ")    
    
ethnicity_types = ['white_total','black_total','asian_total','hawaiian_total','native_total']

#Updating customer vector if survey response is A,B,C,D; note that null rows are also removed from content matrix:
if ethnic_demographics_response != 'E':
    content.dropna(subset = ethnicity_types, inplace=True)
    customer_vect = ethnic_demographics_filtering(ethnic_demographics_response, customer_vect, content, ethnicity_types)
  
#If customer is ethnic demographic agnostic, eliminating ethnic demographic as a factor from both content matrix and customer vect:    
else:
    content.drop(ethnicity_types, axis=1, inplace=True)
    customer_vect.drop(ethnicity_types, inplace=True)

#Median income survey question:
median_income_response = input("Regarding median incomes near your place of business, which of the following is true?:\n \
      A - I'm looking for median incomes on the high end  \n \
      B - I'm looking for near-median median incomes \n \
      C - I'm looking for median incomes on the low end \n \
      D - Median incomes are not important to me \n \
      Answer A, B, C, or D here: ")

#Updating customer vector if survey response is A,B,C; note that null rows are also removed from content matrix:
if median_income_response != 'D':
    content.dropna(subset = ['median_total'], inplace=True)
    customer_vect = median_income_filtering(median_income_response, customer_vect, content)
  
#If customer is median income agnostic, eliminating median income as a factor from both content matrix and customer vect:    
else:
    content.drop(['median_total'], axis=1, inplace=True)
    customer_vect.drop(['median_total'], inplace=True)

#Household size survey question:
household_size_response = input("Regarding household sizes near your place of business, which of the following is true?:\n \
      A - I'm looking to be closer to larger households \n \
      B - I'm looking to be closer to smaller households \n \
      C - Hosuehold size is not important to me \n \
      Answer A, B, or C here: ")

#Updating customer vector if survey response is A,B; note that null rows are also removed from content matrix:
if household_size_response != 'C':
    content.dropna(subset = ['ave_household_size_total'], inplace=True)
    customer_vect = household_size_filtering(household_size_response, customer_vect, content)
  
#If customer is household size agnostic, eliminating household size as a factor from both content matrix and customer vect:    
else:
    content.drop(['ave_household_size_total'], axis=1, inplace=True)
    customer_vect.drop(['ave_household_size_total'], inplace=True)

#Median age survey question:
median_age_response = input("What are your preferences as far as the median age near your place of business:\n \
      A - I want to be near an older demographic  \n \
      B - I want to be near a younger demographic \n \
      C - Age demographics are not important to me \n \
      Answer A, B, C here: ")

age_types = ['median_male_age','median_female_age']

#Updating customer vector if survey response is A,B; note that null rows are also removed from content matrix:
if median_age_response != 'C':
    content.dropna(subset = age_types, inplace=True)
    customer_vect = median_age_filtering(median_age_response, customer_vect, content, age_types)
  
#If customer is median age agnostic, eliminating median age as a factor from both content matrix and customer vect:    
else:
    content.drop(age_types, axis=1, inplace=True)
    customer_vect.drop(age_types, inplace=True)   

#Ethnic homogeneity survey question:
homogeneity_response = input("How ethnically diverse do you want your place of business to be?:\n \
      A - I'm looking to in a more ethnically homogeneous area \n \
      B - I'm looking to be in a more ethnically hetereogeneous area \n \
      C - Ethnic diversity is not important to me \n \
      Answer A, B, or C here: ")

#Updating customer vector if survey response is A,B; note that null rows are also removed from content matrix:
if homogeneity_response != 'C':
    content.dropna(subset = ['monolithic'], inplace=True)
    customer_vect = homogeneity_filtering(homogeneity_response, customer_vect, content)
  
#If customer is ethnic diversity agnostic, eliminating ethnic diversity as a factor from both content matrix and customer vect:    
else:
    content.drop(['monolithic'], axis=1, inplace=True)
    customer_vect.drop(['monolithic'], inplace=True) 

#Dropping 'zone' from customer_vect:
customer_vect.drop(['zone'], inplace=True)

print("Calculating similarity based on", content.shape[0], "zones and", content.shape[1], "features...")

#Similarity calculation:
nonscaled_content_filtering = {content.index[i]:cosine_similarity([list(customer_vect)], [list(content.iloc[i])])[0][0] for i in range(len(content))}
nonscaled_sorted = sorted(nonscaled_content_filtering.items(), key=lambda kv: kv[1], reverse=True)
content_recommendation = pd.DataFrame(nonscaled_sorted, columns = ['zone','similarity'])

#Give scores to each recommended zone:
content_recommendation['scores'] = list(range(1120, 1120 - len(content), -1))

### HYBRID FILTERING ###

#Modifying zone type in collaborative filtering from int to numpy int64
cos_pred_new['zone'] = cos_pred_new['zone'].astype(np.int64)

#Joining collaborative filtering and content based filtering results to get a composite score
hybrid = pd.merge(cos_pred_new,content_recommendation, on=['zone'], how='left')
hybrid.rename(columns={'rank':'collab_rank', 'scores':'content_rank'},inplace = True)
hybrid['composite_score'] = hybrid['collab_rank'] + hybrid['content_rank']
hybrid = hybrid.sort_values('composite_score',ascending=False)
hybrid.head()




