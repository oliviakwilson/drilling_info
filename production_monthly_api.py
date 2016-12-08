#! usr/bin/python

# Created: 7 November, 2016
# Analyst: Olivia Wilson
# Purpose: Generate Production Monthly Data Set from Production Header Data Set  - Drilling Info
# Take distinct entity ids from the production header data set and loop through each to generate production monthly details

import json 																			# Handles JSON
import MySQLdb 																			# Handles MySQL Database
from urllib2 import Request, urlopen, URLError											# Handles interactions with HTTP
import urllib
import time

# Start of the ETL
start_time = time.time()
print ("Program started at %s ") % (start_time)

# Drilling Info API Information:

api_key = ''											# Key Provided by Customer Service DI Access
header_api_key = {'X-API-KEY':api_key}													# Header to pass in API KEY in API Call
base_url = 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities-details'
# test entity_id = 129590982

# Complete List: state_province = ( 'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'FL', 'FO GULF', 'FO PACIFIC', 'KS', 'KY', 'LA', 'MD', 'MI', 'MS', 'MO', 'MT', 'NE', 'NV', 'NM', 'NY', 'ND', 'OH', 'OK', 'OR', 'PA', 'SD', 'TN', 'TX', 'UT', 'VA', 'WV', 'WY')
# Test with small state records: -- excluded NY for now
state_province = ('AR', 'ND', 'MT')

api_call_dates = range(2006, 2017)

# Database Connection Details
db = MySQLdb.connect(	host="",		# Host
						user="",		# Username
						passwd="",		# Password
						db="" 			# Name of Database
						 )

# Create cursor object to execute all queries
try:
   cur = db.cursor()
   print ('Successfully connected to database')
except:
   print ('Connection unsuccessful')

# SQL to extract all distinct entity ids per state province in the production header data set
# By ordering by entity_id desc, if the MySQL Connection fails in the middle of a state, can go back and add where entity_id < (last entity_id to be inserted)
entity_list_sql = """
	select distinct
		entity_id
	from 
		warehouse.di_production_headers
	where
		state_province = '%s'
        and last_prod_date >= '2006-01-01'
        and case
				when state_province = 'AR' then entity_id < 124283917
			end
	order by
		entity_id desc
"""
# SQL string to insert production monthly data for each entity id
sql_str = """
	replace into warehouse.di_production_monthly (id, entity_id, prod_datetime, liq, gas, wtr, wcnt, days)
	values
	(%s, %s, %s, %s, %s, %s, %s, %s)
"""

# For each state in list of states, execute SQL to extract each entity id
for state_name in state_province:
	cur.execute(entity_list_sql % state_name)
	entities = cur.fetchall()
	print entities
	y = 0
	for entity_id in entities:
		for x_yr in api_call_dates:
			min_prod_datetime = ('%s-01-01') % x_yr
			try:
				params = {'entity_id': entity_id[0],
				'min_prod_datetime':min_prod_datetime,
				'format':'json',
				'pagesize':'50000'}
				params= urllib.urlencode(params)
				url = '{base_url}?{params}'.format(base_url=base_url,params=params)
				print url
				req = Request(url, headers=header_api_key)								
				response = urlopen(req)
				production_headers = response.read()
				json_data = json.loads(production_headers)							
				# Insert each of the api records into the database
				x = 0
				for i in json_data:
					new_record = (i['id'], i['entity_id'], i['prod_datetime'], i['liq'], i['gas'], i['wtr'], i['wcnt'], i['days'])
					print new_record
					cur.execute(sql_str, new_record)
					db.commit()
					print "Just Loaded: "
					print new_record
					x += 1
				print "Loaded %s new records for entity_id %s " % (x, entity_id)
			except URLError, e:
				print 'Error', e
		y += 1
	print "Finished loading %s distinct entities for state %s " % (y, state_name)

print "finished loading all new records"


# Close the connection to the database
cur.close()
print ('Closed the connection')

print('This ETL is now complete')
print('The program took %s seconds to execute') % (time.time()-start_time)
print ("This program took %s minutes to execute") % ((time.time()-start_time)/60)