#!/usr/bin/python

# Created 24 October, 2016
# Analyst: Olivia Wilson
# Purpose: Extracts production header data from Drilling Info DI Access API and inserts it into MySQL Database

import json 																			# Deals with JSON
import MySQLdb 																			# Deals with MySQL Database
from urllib2 import Request, urlopen, URLError											# Handles interactions with HTTP
import urllib
import time

# Start of the ETL
start_time = time.time()
print ("Program started at %s ") % (start_time)

# Drilling Info API Information:
api_key = ''											# Key Provided by Customer Service DI Access
header_api_key = {'X-API-KEY':api_key}													# Header to pass in API KEY in API Call


# State/Province to Pass into the API Call (Skip Alabama and Texas because already input those separately during testing)
# ( AL', 'AK', 'AZ', AR', 'CA', 'CO', 'FL', 'KS', 'KY', 'LA', 'MD', 'MI', 'MS', 'MO', 'MT', 'NE', 'NV', 'NM', 'NY', 'ND', 'OH', 'OK', 'OR', 'PA', 'SD', 'TN', 'UT', 'VA', 'WV', 'WY')
state_province = {'state_province': 'NY',
					'state_province': 'FO GULF',
					'state_province': 'FO PACIFIC'} #('CA', 'MI', 'OK', 'TX', 'NY', 'FO GULF', 'FO PACIFIC') #("TX", 'NY', 'FO GULF', 'FO PACIFIC') #, No records at all ('NY', 'FO GULF','FO PACIFIC') # Pre 1990 (MI, OK)

# Parameters to pass into the URL to not exceed the API max extract limitation
api_call_dates = range(1902,2016)


# Database Connection Details
db = MySQLdb.connect(	host="",			# Host
						user="",		    # Username
						passwd="",			# Password
						db="" 				# Name of Database
						 )

# Create cursor object to execute all queries
try:
   cur = db.cursor()
   print ('Successfully connected to database')
except:
   print ('Connection unsuccessful')

sql_str = "REPLACE INTO warehouse.drilling_info_production_headers_api_raw_data_dump  (id, entity_id, district, entity_type, prod_type, county_parish, county_state, state_province, country, lease_name, api_uwi, initial_completion_date, field, reservoir, regulatory_number, well_number, current_operator, oil_gatherer, gas_gatherer, current_producing_status, wellbore_orientation, total_depth, lower_perf, upper_perf, liq_grav, gas_grav, daily_oil, daily_gas, cum_gas, cum_oil, cum_water, entity_latitude, entity_longitude, first_prod_date, last_prod_date, latest_well_count, gor_last_12, basin, peak_gas, peak_oil, api_awi_list, max_active_wells, months_produced, master_current_operator, alloc_plus, oil_gravity, first_month_oil, first_month_gas, first_month_water, first_12_oil, first_12_gas, first_12_water, last_12_oil, last_12_gas, last_12_water, second_month_gor, latest_gor, last_12_yield, second_month_yield, latest_yield, spud_year, peak_gas_month, peak_oil_month, peak_boe, peak_boe_month, peak_mmcfge, peak_mmcfge_month, latest_test_year, latest_flow_pressure, latest_whsip, first_6_oil, first_6_gas, first_6_water, first_24_oil, first_24_gas, first_24_water, first_60_oil, first_60_gas, first_60_water, first_6_boe, first_12_boe, first_24_boe, first_60_boe, first_12_mmcfge, first_24_mmcfge, first_60_mmcfge, prac_ip_oil_daily, prac_ip_gas_daily, prac_ip_cfged, prac_ip_boe, cum_mmcfge, cum_bcfge, prior12_oil, prior12_gas, prior12_wtr) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

# Fetch the data from the Drilling Info API
try:
	for state_name in state_province:
		params= urllib.urlencode(state_name)
		base_url = 'https://di-api.drillinginfo.com/v1/direct-access/producing-entities'
		url1 = '{base_url}?{params}'.format(base_url=base_url,params=params)
		for x_yr in api_call_dates:
			min_first_prod_date = ('%s-01-01') % x_yr
			max_first_prod_date = ('%s-12-31') % x_yr
			url2 = ('&min_first_prod_date=%s&max_first_prod_date=%s&format=json&pagesize=50000') % (min_first_prod_date, max_first_prod_date)
			url3 = url1 + url2
			print url3
			req = Request(url3, headers=header_api_key)								
			response = urlopen(req)
			production_headers = response.read()
			json_data = json.loads(production_headers)						
			# Insert each of the api records into the database
			for i in json_data:
				new_record = (i['id'], i['entity_id'], i['district'], i['entity_type'], i['prod_type'], i['county_parish'], i['county_state'], i['state_province'], i['country'], i['lease_name'], i['api_uwi'], i['initial_completion_date'], i['field'], i['reservoir'], i['regulatory_number'], i['well_number'], i['current_operator'], i['oil_gatherer'], i['gas_gatherer'], i['current_producing_status'], i['wellbore_orientation'], i['total_depth'], i['lower_perf'], i['upper_perf'], i['liq_grav'], i['gas_grav'], i['daily_oil'], i['daily_gas'], i['cum_gas'], i['cum_oil'], i['cum_water'], i['entity_latitude'], i['entity_longitude'], i['first_prod_date'], i['last_prod_date'], i['latest_well_count'], i['gor_last_12'], i['basin'], i['peak_gas'], i['peak_oil'], i['api_awi_list'], i['max_active_wells'], i['months_produced'], i['master_current_operator'], i['alloc_plus'], i['oil_gravity'], i['first_month_oil'], i['first_month_gas'], i['first_month_water'], i['first_12_oil'], i['first_12_gas'], i['first_12_water'], i['last_12_oil'], i['last_12_gas'], i['last_12_water'], i['second_month_gor'], i['latest_gor'], i['last_12_yield'], i['second_month_yield'], i['latest_yield'], i['spud_year'], i['peak_gas_month'], i['peak_oil_month'], i['peak_boe'], i['peak_boe_month'], i['peak_mmcfge'], i['peak_mmcfge_month'], i['latest_test_year'], i['latest_flow_pressure'], i['latest_whsip'], i['first_6_oil'], i['first_6_gas'], i['first_6_water'], i['first_24_oil'], i['first_24_gas'], i['first_24_water'], i['first_60_oil'], i['first_60_gas'], i['first_60_water'], i['first_6_boe'], i['first_12_boe'], i['first_24_boe'], i['first_60_boe'], i['first_12_mmcfge'], i['first_24_mmcfge'], i['first_60_mmcfge'], i['prac_ip_oil_daily'], i['prac_ip_gas_daily'], i['prac_ip_cfged'], i['prac_ip_boe'], i['cum_mmcfge'], i['cum_bcfge'], i['prior12_oil'], i['prior12_gas'], i['prior12_wtr'])
				cur.execute(sql_str, new_record)
				db.commit()
				print "Just Loaded: "
				print new_record
except URLError, e:
	print 'Error', e


print "finished loading all new records"


# Close the connection to the database
cur.close()
print ('Closed the connection')

print('This ETL is now complete')
print('The program took %s seconds to execute') % (time.time()-start_time)
print ("This program took %s minutes to execute") % ((time.time()-start_time)/60)