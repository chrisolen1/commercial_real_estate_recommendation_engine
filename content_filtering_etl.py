import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
from sklearn import preprocessing
from sklearn.metrics.pairwise import cosine_similarity

###ETL for Content-Based Filtering ###

"""
BELOW WE EXTRACT DATA FROM TABLES IN THE OPERATIONAL MYSQL DATABASE GENERATED IN THE "grid_generation_and_api.py" FILE. NOTE THAT THE EDUCATION TABLE WAS UPLOADED MANUALLY FROM
THE CITY OF CHICAGO'S DATA PORTAL (THOUGH AN API SCRIPT COULD HAVE BEEN WRITTEN). NOTE ALSO THAT THE BUSINESS TABLE WAS MANUALLY GENERATED FROM A PROPRIETY SOURCE AND
THE INDIVIDUAL IDENTITIES OF BUSINESSES WERE ANONYMIZED BY AGGREGATING THEIR FEATURES BY SIC CODE WITHIN EACH ZONE DUE TO SENSITIVITY ISSUES
"""

"""
WE START BY GENERATING A 'CONTENT MATRIX', WHICH CREATES A MATRIX OF PROPERTY, DEMOGRAPHIC, CRIME, EDUCATION, BUSINESS FEATURES PER ZONE
"""

#Business ETL

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "business_train_2"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

biz_query = "SELECT * FROM business_train_2;"
business = pd.read_sql_query(biz_query, con)

#Taking only first four digits in SIC
def first_four(row):
    return row[0:4]
business['sic_4'] = business['sic'].apply(first_four)

#Dropping rows we don't need:
business.drop(['zone_sic','sic','median_revenue','sd_revenue','median_emp_total',
               'sd_emp_total','median_emp_here','sd_emp_here','median_age','sd_age',
               'median_sqft','sd_sqft'], axis=1, inplace=True)
    
#Converting SIC's to dummy variables:    
business = pd.get_dummies(business, columns=['sic_4'])

#Aggregating non-SIC features using mean:
continuous = business.groupby(['zone'])[list(business.columns[5:8])].mean()

#Aggregating SIC features using sum:
discrete = business.groupby(['zone'])[list(business.columns[8:])].sum()

products_business = pd.concat([continuous, discrete], axis = 1)
products_business.shape

#Property ETL

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "property"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

property_query = "select * from property as p inner join property_zone_lkup as l on p.results_key = l.results_key;"
property = pd.read_sql_query(property_query, con)

#Dropping columns we don't need:
property.drop(['zpid','street', 'city', 'zipcode', 'latitude',
       'longitude','valueChange', 'low_estimate',
       'high_estimate', 'neighborhood', 'neighborhood_zindex_value',
       'hometype', 'assessmentYear', 'assessment','bathrooms', 'bedrooms','lastSoldPrice',
       'grid_num', 'updated', 'id', 'results_key', 'latitude', 'longitude'],axis=1,inplace=True)

#Finding age in years:
def year_diff_bought(row):
    return int(2019.0 - row)
    
replacing = property[property['yearBuilt'].notnull()]['yearBuilt'].apply(year_diff_bought)
property.iloc[replacing.index.values, list(property.columns).index('yearBuilt')] = replacing
property['houseAge'] = property['yearBuilt']
property.drop(['yearBuilt'],axis=1,inplace=True)

#Finding number of years ago sold in years: 
def year_diff_sold(row):
    now = datetime.datetime(2019,5,1,0,0).date()
    return (now.year - row.year)

replacing = property[property['lastSold'].notnull()]['lastSold'].apply(year_diff_sold)
property.iloc[replacing.index.values, list(property.columns).index('lastSold')] = replacing
property['lastSold'].replace({None:np.NaN},inplace=True)

#Imputing houseSize:
unique_zones = property['zone'].unique()
for i in unique_zones:
    if len(property[property['zone']== i]['houseSize'].isna().value_counts()) == 2:
        sub_mean = property[property['zone']== i]['houseSize'].mean()
        sub_range = property[property['zone']== i]
        replacing = [sub_mean] * len(property.iloc[sub_range[sub_range['houseSize'].isna()].index.values, list(property.columns).index('houseSize')])
        property.iloc[sub_range[sub_range['houseSize'].isna()].index.values, list(property.columns).index('houseSize')] = replacing
    else:
        pass

#Calculating sqftValue:
property['sqftValue'] = property['zestimate']/property['houseSize']        

#Aggregating on mean:
continuous = property.groupby(['zone'])[['lotSize','houseSize','lastSold','houseAge','sqftValue']].mean()
continuous.index = continuous.index.map(int)

#Aggregating on sum:
property['n_properties'] = np.repeat(1,len(property))
discrete = property.groupby('zone')['n_properties'].sum()
discrete.index = discrete.index.map(int)

#Concatenating continuous and discrete: 
products_property = pd.concat([continuous, discrete], axis = 1)
products_property.shape

#Crime ETL:

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "crime"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

crime_query = "select * from crime as c inner join crime_zone_lkup as l on c.results_key = l.results_key;"
crime = pd.read_sql_query(crime_query, con)

#Dropping columns we don't need:
crime.drop(['id','case_number','date','year','description','beat','block','community_area','ward','district',
            'latitude','longitude','fbi_code','iucr','entered_on','x_coordinate','y_coordinate','updated',
            'location_description','results_key'],
    axis=1,inplace=True)

#Combining like crime categories and creating dummmies:
crime.replace({'THEFT':'STEALING','BURGLARY':'STEALING','MOTOR VEHICLE THEFT':'STEALING',
               'ROBBERY':'STEALING','BATTERY':'VIOLENT','ASSAULT':'VIOLENT','WEAPONS VIOLATION':'VIOLENT',
               'CRIM SEXUAL ASSAULT':'VIOLENT','ARSON':'VIOLENT','CRIMINAL DAMAGE':'PROPERTY_CRIME',
               'CRIMINAL TRESPASS':'PROPERTY_CRIME','NARCOTICS':'DRUGS','SEX OFFENSE':'SEX_CRIME',
               'PROSTITUTION':'SEX_CRIME','OBSCENITY':'SEX_CRIME','DECEPTIVE PRACTICE':'FRAUD',
               'OTHER OFFENSE':'OTHER','OFFENSE INVOLVING CHILDREN':'OTHER','PUBLIC PEACE VIOLATION':'OTHER',
               'INTERFERENCE WITH PUBLIC OFFICER':'OTHER','LIQUOR LAW VIOLATION':'OTHER','GAMBLING':'OTHER',
               'STALKING':'OTHER','KIDNAPPING':'OTHER','INTIMIDATION':'OTHER','CONCEALED CARRY LICENSE VIOLATION':'OTHER',
               'NON-CRIMINAL':'OTHER','PUBLIC INDECENCY':'OTHER','HUMAN TRAFFICKING':'OTHER','NON-CRIMINAL (SUBJECT SPECIFIED)':'OTHER',
               'OTHER NARCOTIC VIOLATION':'OTHER'}, inplace=True)
crime.rename(columns={'primary_type':'crime_type'},inplace=True)
crime = pd.get_dummies(crime, columns=['crime_type'])

#Aggregating using sum, ditching white collar 'FRAUD' crimes:
products_crime = crime.groupby(['zone'])[['domestic', 'arrest','crime_type_DRUGS', 'crime_type_HOMICIDE',
       'crime_type_OTHER', 'crime_type_PROPERTY_CRIME','crime_type_SEX_CRIME',
       'crime_type_STEALING', 'crime_type_VIOLENT']].sum()
products_crime.index = products_crime.index.map(int)
products_crime.shape

#School ETL:

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "school"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

school_query = "select * from school as s inner join school_zone_lkup as l on s.School_ID = l.School_ID;"
school = pd.read_sql_query(school_query, con)

#Dropping columns I don't need:
school.drop(['School_ID','Long_Name','Primary_Category','Address','City','State','Zip',
            'Growth_Reading_Grades_Tested_Pct_ES','Growth_Math_Grades_Tested_Pct_ES',
            'Attainment_Reading_Pct_ES', 'Attainment_Math_Pct_ES',
       'Culture_Climate_Rating', 'Creative_School_Certification',
       'School_Survey_Involved_Families','School_Survey_Collaborative_Teachers', 'School_Survey_Safety',
       'Suspensions_Per_100_Students_Year_2_Pct',
       'Misconducts_To_Suspensions_Year_2_Pct','College_Enrollment_School_Pct_Year_2',
       'College_Persistence_School_Pct_Year_2', 'School_Latitude',
       'School_Longitude', 'id', 'latitude', 'longitude'], axis=1, inplace=True)

#Filling NAs for award categories:
school['Blue_Ribbon_Award_Year'] = school['Blue_Ribbon_Award_Year'].fillna(0)
school['Excelerate_Award_Gold_Year'] = school['Excelerate_Award_Gold_Year'].fillna(0)
school['Spot_Light_Award_Year'] = school['Spot_Light_Award_Year'].fillna(0)
school['Improvement_Award_Year'] = school['Improvement_Award_Year'].fillna(0)
school['Excellence_Award_Year'] = school['Excellence_Award_Year'].fillna(0)

#Converting years an award was won to count value:
def make_one(row):
    if row != 0:
        return row**0
    else:
        row = 0

school['Blue_Ribbon_Award_Year'] = school['Blue_Ribbon_Award_Year'].apply(make_one)
school['Excelerate_Award_Gold_Year'] = school['Excelerate_Award_Gold_Year'].apply(make_one)
school['Spot_Light_Award_Year'] = school['Spot_Light_Award_Year'].apply(make_one)
school['Improvement_Award_Year'] = school['Improvement_Award_Year'].apply(make_one)
school['Excellence_Award_Year'] = school['Excellence_Award_Year'].apply(make_one)

school['Blue_Ribbon_Award_Year'] = school['Blue_Ribbon_Award_Year'].fillna(0)
school['Excelerate_Award_Gold_Year'] = school['Excelerate_Award_Gold_Year'].fillna(0)
school['Spot_Light_Award_Year'] = school['Spot_Light_Award_Year'].fillna(0)
school['Improvement_Award_Year'] = school['Improvement_Award_Year'].fillna(0)
school['Excellence_Award_Year'] = school['Excellence_Award_Year'].fillna(0)

#Summing total awards per school:
school['awards'] = school['Blue_Ribbon_Award_Year'] + school['Excelerate_Award_Gold_Year'] + school['Spot_Light_Award_Year'] + school['Improvement_Award_Year'] + school['Excellence_Award_Year']
school.drop(['Blue_Ribbon_Award_Year','Excelerate_Award_Gold_Year','Spot_Light_Award_Year',
             'Improvement_Award_Year','Excellence_Award_Year'],axis=1,inplace=True)

#Converting school type to dummy:
school = pd.get_dummies(school, columns=['School_Type'])

#Aggregating by sum first:
discrete = school.groupby('zone')[['awards',
       'School_Type_Career academy', 'School_Type_Charter',
       'School_Type_Citywide-Option', 'School_Type_Classical',
       'School_Type_Contract', 'School_Type_Magnet',
       'School_Type_Military academy', 'School_Type_Neighborhood',
       'School_Type_Regional gifted center',
       'School_Type_Selective enrollment', 'School_Type_Small',
       'School_Type_Special Education']].sum()

discrete['n_schools'] = discrete['School_Type_Career academy'] + discrete['School_Type_Charter'] + \
       discrete['School_Type_Citywide-Option'] + discrete['School_Type_Classical'] + \
       discrete['School_Type_Contract'] + discrete['School_Type_Magnet'] + \
       discrete['School_Type_Military academy'] + discrete['School_Type_Neighborhood'] + \
       discrete['School_Type_Regional gifted center'] + \
       discrete['School_Type_Selective enrollment'] + discrete['School_Type_Small'] + \
       discrete['School_Type_Special Education']  
  

#Dropping individual school types:
discrete.drop(['School_Type_Career academy',
       'School_Type_Charter', 'School_Type_Citywide-Option',
       'School_Type_Classical', 'School_Type_Contract', 'School_Type_Magnet',
       'School_Type_Military academy', 'School_Type_Neighborhood',
       'School_Type_Regional gifted center',
       'School_Type_Selective enrollment', 'School_Type_Small',
       'School_Type_Special Education'], axis = 1, inplace = True)


#Converting to ordinal:
school['Student_Growth_Rating'].replace({'FAR BELOW AVERAGE':0,'NO DATA AVAILABLE': np.NaN,
      'BELOW AVERAGE':1,'AVERAGE':2,'ABOVE AVERAGE':3,'FAR ABOVE AVERAGE':4,'': np.NaN}, inplace=True)

school['Student_Attainment_Rating'].replace({'FAR BELOW AVERAGE':0,'NO DATA AVAILABLE': np.NaN,
      'BELOW AVERAGE':1,'AVERAGE':2,'ABOVE AVERAGE':3,'FAR ABOVE AVERAGE':4,
      'FAR BELOW EXPECTATIONS':0,'BELOW EXPECTATIONS':1,'MET EXPECTATIONS':2,
      'ABOVE EXPECTATIONS':3,'FAR ABOVE EXPECTATIONS':4,'': np.NaN}, inplace=True)

school['School_Survey_Supportive_Environment'].replace({'VERY WEAK':0,'NOT ENOUGH DATA': np.NaN,
      'WEAK':1,'NEUTRAL':2,'STRONG':3,'VERY STRONG':4,'': np.NaN}, inplace=True)

school['School_Survey_Ambitious_Instruction'].replace({'VERY WEAK':0,'NOT ENOUGH DATA': np.NaN,
      'WEAK':1,'NEUTRAL':2,'STRONG':3,'VERY STRONG':4,'': np.NaN}, inplace=True)

school['School_Survey_Effective_Leaders'].replace({'VERY WEAK':0,'NOT ENOUGH DATA': np.NaN,
      'WEAK':1,'NEUTRAL':2,'STRONG':3,'VERY STRONG':4,'': np.NaN}, inplace=True)
  
min_max_scaler = preprocessing.MinMaxScaler()
school['attendance_scaled'] = min_max_scaler.fit_transform(np.array(school['Student_Attendance_Year_2_Pct']).reshape(-1,1))*5

school['avg_rating'] = school[['Student_Growth_Rating','Student_Attainment_Rating','School_Survey_Supportive_Environment',
      'School_Survey_Ambitious_Instruction','School_Survey_Effective_Leaders','attendance_scaled']].mean(axis=1)

#Aggregating by mean:
continuous = school.groupby('zone')['avg_rating'].mean()

#Concatenating discrete and continuous together:
products_school = pd.concat([continuous, discrete], axis = 1)

#Transit ETL:

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "transit"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

transit_query = "select * from transit as t inner join transit_zone_lkup as l on t.results_key = l.results_key;"
transit = pd.read_sql_query(transit_query, con)

#Dropping columns we don't need:
transit.drop(['results_key','stop_id','direction_id','stop_name','latitude','longitude','updated','id'],axis=1,inplace=True)

#Creating single 'el' variable:
transit['el'] = transit['blue'] + transit['brn'] + transit['g'] + transit['o'] + transit['p'] + transit['pexp'] + transit['pnk'] + transit['red'] + transit['y']

#Aggregating by sum:
products_transit = transit.groupby('zone')[['el','bus']].sum()
products_transit.index = products_transit.index.map(int)
products_transit.shape

#Demographics ETL:

passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "zillow_oltp"
table = "grid_census_final_2"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()

demographics_query = "select * from grid_census_final_2;"
demographics = pd.read_sql_query(demographics_query, con)

#Calculating score for how monolithic a zone is:     
demographics['monolithic'] = [demographics[['white_total',
       'black_total', 'native_total', 'asian_total', 'hawaiian_total',
       'other_alone_total', 'two_or_more_total']].iloc[i].max() for i in range(len(demographics[['white_total',
       'black_total', 'native_total', 'asian_total', 'hawaiian_total',
       'other_alone_total', 'two_or_more_total']]))]

demographics.drop(['n_bound','s_bound','w_bound','e_bound','NW ZIP', 'NE ZIP', 'SW ZIP',
                   'SW ZIP', 'SE ZIP','other_alone_total', 'two_or_more_total'], axis=1, inplace=True)

#Aggregating by mean so that table has same form as other tables:
products_demographics = demographics.groupby('zone')[['median_total', 'ave_household_size_total', 'white_total',
       'black_total', 'native_total', 'asian_total', 'hawaiian_total','monolithic','median_male_age','median_female_age']].mean()

demographics_map = products_demographics

products_demographics.shape

#Merge dataframes on index:

#First the non-business data:
minus_business = products_property.join(products_crime,how='outer').join(products_school,how='outer').join(products_transit,how='outer').join(products_demographics,how='outer')
#Then left joining on the business table so as to eliminate any zones without business activity:
products = products_business.join(minus_business,how='left')

#Filling school-related NaN's with zero:
products['n_schools'].fillna(0,inplace=True)

#Filling transit-related NaN's with zero:
for i in range(list(products.columns).index('el'),list(products.columns).index('bus')+1):
    products[products.columns[i]].fillna(0,inplace=True)

#Filling crime-related NaN's with zero:
for i in range(list(products.columns).index('domestic'),list(products.columns).index('crime_type_VIOLENT')+1):
    products[products.columns[i]].fillna(0,inplace=True)

#Resulting shape:
products.shape

#Create a mapping of df types to mysql data types:
def dtype_mapping():
    return {'object' : 'VARCHAR',
        'int64' : 'INT',
        'float64' : 'FLOAT',
        'datetime64' : 'DATETIME',
        'bool' : 'TINYINT',
        'category' : 'INT',
        'uint8': 'INT'}
    
#Function to create sqlalchemy engine:
def mysql_engine(user = 'root', password = "DB_PASSWORD", host = "DB_HOST", 
                 database = 'chicago_olap'):
    engine = create_engine("mysql+pymysql://{0}:{1}@{2}/{3}".format(user, password, host, database))
    return engine

#Function to create mysql connection via the engine:
def mysql_con(engine):
    con = engine.connect()
    return con

#Function to create sql input for table names and types:
def gen_tbl_cols_sql(df):
    dmap = dtype_mapping()
    sql = "`zone` INT(10) NOT NULL"
    headers = products.dtypes.index
    headers_list = [(hdr, str(products[hdr].dtype)) for hdr in headers]
    for i, hl in enumerate(headers_list):
        sql += ", `{0}` {1}".format(hl[0], dmap[hl[1]])
    sql += ", updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY (`zone`)"    
    return sql

#Drop if exists:
def drop_if_exists(con, tbl_name):
    sql = "DROP TABLE IF EXISTS `{0}`;".format(tbl_name)
    con.execute(sql)
    con.close()
drop_if_exists(mysql_con(mysql_engine()), tbl_name='zones_content_filtering')

#Function to create a mysql table from a df:
def create_mysql_tbl_schema(df, con, tbl_name):
    tbl_cols_sql = gen_tbl_cols_sql(df)
    sql = "CREATE TABLE `{0}` ({1}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;".format(tbl_name, tbl_cols_sql)
    con.execute(sql)
    con.close()

#Create the table:
create_mysql_tbl_schema(products, mysql_con(mysql_engine()), tbl_name='zones_content_filtering')

#Insert transformed data into the table:
passwd = "DB_PASSWORD"
host = "DB_HOST"
database = "chicago_olap"
table = "zones_content_filtering"

engine = create_engine("mysql+pymysql://root:%s@%s/%s" % (passwd, host, database))
con = engine.connect()
products.to_sql(con=con, name=table, if_exists='append')
