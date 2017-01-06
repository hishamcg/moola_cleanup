import re
import sys
import requests
import time
import math
from progress import printProgress
import pickle
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

reload(sys)
sys.setdefaultencoding("utf-8")
error_log = []
category_count = 4 #len(str(reg_no))
company_class = {"FLC": "Financial Lease Company",
				"FTC": "Subsidiary of a Foreign Company",
				"GAP": "General Association Public",
				"GAT": "General Association Private",
				"GOI": "Companies owned by Government Of India",
				"NPL": "Not For Profits Company",
				"PLC": "Public Limited Company",
				"PTC": "Private Limited Company",
				"SGC": "Companies owned by State Government",
				"ULL": "Public Limited Company with Unlimited Liability",
				"ULT": "Private Limited Company with Unlimited Liability",
				"OPC": "One person Company"}

def parse_nic(path,not_bg_mode):
	array = []
	section_array = []
	section_tmp_string = ''
	last_value = 0
	count = 0
	num_lines = sum(1 for line in open(path, "r"))
	key_val = '01000'
	
	if not_bg_mode: printProgress(count, num_lines, prefix = path+' ', suffix = '')
	with open(path, "r") as ins:
		#this is a very ugly methode
		# Initial call to print 0% progress
		for line in ins:
			count+=1
			if count%100 == 0:
				if not_bg_mode: printProgress(count, num_lines, prefix = path+' ', suffix = '')

			split_arr = filter(None,line.strip().split("  "))
			#if (len(split_arr) == 1 and 'division' in split_arr[0].lower() and len(re.findall(r'\d+',split_arr[0])) == 1): #filter(None,split_arr[0].split(" "))[1].isdigit()):
			#remove empty line
			if len(split_arr) > 0:
				#get line with divion
				if split_arr[0].strip().lower().startswith('division') and len(re.findall(r'\d+',line)) == 1:
					key_val = append_zero(re.findall(r'\d+',line)[0])
					int_val = int(key_val)
					if last_value < int_val:
						last_value = int_val
						#i thot having int as key was good. but i guess string was better
						arr = [key_val, line.split(":")[-1].strip()]
						array.append(arr)
						section_array.append([key_val,section_tmp_string])

					#this condition helps to remove buggy entry
					elif last_value != int_val and int_val > int(array[-2][0]):
						last_value = int_val
						array[-1] = [key_val, line.split(":")[-1].strip()]
						#section_array.append([key_val,section_tmp_string])

					#checking if section value is available

				#get with section
				elif split_arr[0].strip().lower().startswith('section'):
					section_tmp_string = line.split(":")[-1].strip()
				else:
					arr,int_val = check_digit(split_arr,array,last_value)
					if int_val == -1:
						#appended arr to previous data
						array[-1][1] = array[-1][1] + " " + arr
					elif int_val == last_value:
						#appended arr to previous data
						array[-1][1] = array[-1][1] + " " + arr[0][1]
					elif arr and int_val:
						last_value = int_val
						array += arr
		if not_bg_mode: printProgress(count, num_lines, prefix = path+' ', suffix = '')
	return dict(array),dict(section_array)

def check_digit(split_arr,array,last_value):
	#here i am doing lot of stupid assumptoins. 
	#1. that numbers are always seperated by two space. eg 12  121  1211  then data
	#2. hence data part will always give False on isdigit check.
	digits_arr = []
	test_num = False
	pop_array = False
	int_val = 0

	if(len(split_arr)>1):
		for a in split_arr:
			a = a.strip()
			if a.isdigit():
				val = append_zero(a)
				int_val = int(val)
				if last_value <= int_val:
					last_value = int_val
					digits_arr.append(val)
				elif last_value > int_val and int_val > int(array[-2][0]):
					last_value = int_val
					pop_array = True
					digits_arr.append(val)

				if test_num:
					print "RED ALERT! ==> ERROR ==> oki boss your logic is wrong"
			else:
				test_num = True
	else:
		a = split_arr[0].strip()
		if len(re.findall(r'\d+',a)) == 0 or not a.startswith((re.findall(r'\d+',a))[0]):
			return a,-1

	#this is the last string so check if there is any number in it
	for r_digi in re.findall(r'\d+',a):
		#we consider this as a genuine code if the string starts with it
		if a.startswith(r_digi):
			val = append_zero(r_digi)
			int_val = int(val)
			if last_value <= int_val:
				last_value = int_val
				digits_arr.append(val)
				a = a[len(r_digi):].strip()
			elif last_value > int_val and int_val > int(array[-2][0]):
				last_value = int_val
				digits_arr.append(val)
				a = a[len(r_digi):].strip()
				pop_array = True
		else:
		 	break

	if not a:
		return None,None
	elif pop_array:
		#print '\n'+array[-1][0]+ ' ===> ' +str(int_val)
		array.pop()
	elif not digits_arr:
		return a,-1

	#make a sub array out of all the code and data. here we just repeat the data
	sub_arr = map(list, zip(digits_arr,[a]*len(digits_arr)))
	return sub_arr,last_value

def append_zero(s):
	#append 0 to the key
	for i in range(len(s),category_count+1):
		s+='0'
	return s

def update_mongo(nic_list,section_list,DB_NAME,not_bg_mode):
	conn = eval("MongoClient('localhost', 27017)."+DB_NAME)
	coll = conn.registrations
	bulk = coll.initialize_ordered_bulk_op()

	cursor = coll.find({"_type":"CorporateIdentificationNumber"})
	total_doc_count = cursor.count()
	if not_bg_mode: print "Starting Bulk index of {} documents".format(cursor.count())

	actions = []

	if not_bg_mode: print '\n#######starting elasticsearch update###########\n'
	if not_bg_mode: printProgress(1, total_doc_count, prefix = 'update : ', suffix = '')


	bulk_data = []
	is_bulk_executable = False
	for n, doc in enumerate(cursor):
		if len(doc["ref"]) == 21:
			cat_string,section_string = check_update_nic_list(doc,nic_list,section_list)
			#if cat_string and cat_string is not None:
			bulk.find({'_id':doc['_id']}).update({"$set": {'industry_tags': cat_string,
														'section_code': section_string,
														'company_class': set_company_class(doc["ref"]),
														'listed_or_unlisted': set_listed_or_unlisted(doc["ref"])}})
			is_bulk_executable = True
			#print doc["ref"] + "\n" + cat_string + "\n" + section_string + "\n" + set_company_class(doc["ref"]) + "\n" + set_listed_or_unlisted(doc["ref"])
			
		if n%10000 and is_bulk_executable:
			is_bulk_executable = False
			bulk.execute()
			bulk = coll.initialize_ordered_bulk_op()
			if not_bg_mode: printProgress(n, total_doc_count, prefix = 'update : ', suffix = '')

	if is_bulk_executable:
		bulk.execute()
	if not_bg_mode: printProgress(n, total_doc_count, prefix = 'update : ', suffix = '')
	

	#outfile = open('err_log.txt', 'w')
	#pickle.dump(error_log, outfile)

def set_listed_or_unlisted(ref):
	foo = ref[0:1]
	return "listed" if foo == "L" else "unlisted"

def set_company_class(ref):
	global company_class
	foo = ref[12:15]
	return company_class[foo] if company_class.has_key(foo) else "Unknown"

def get_cat_array(reg_no):
	#append 0 to the key
	cat_arr = []
	#this creates an array with four element. eg: input = 12345, output = [12345,12340,12300,12000]
	#this wer hardcoded for speed
	for i in range(3,0,-1):
		cat_arr.append(reg_no[:5 - i]+'0'*i)
	cat_arr.append(reg_no)
	return cat_arr


def check_update_nic_list(doc,nic_list,section_list):
	reg_no =  doc["ref"][1:6]
	if reg_no.isdigit() and len(str(int(reg_no))) > 3:
		list_choice = 1 #default
		try:
			year_of_registration = int(doc["ref"][8:12])
			#this returns possible nic year [0,1,2] => [2004,2008,2014]
			if year_of_registration < 2009:
				list_choice = 0
		except Exception as e:
			pass
		
		#here we try to find probable year of nic based on a simple hit scoring

		#make a list of all categories with reg_no
		cat_arr = get_cat_array(reg_no)

		#array has size of len(nic_list)
		#values = [[0]*category_count]*len(nic_list)
		values = []
		# for li in range(list_choice,len(nic_list)):
		for li in range(len(nic_list)):
			value = []
			if li < list_choice: 
				value = [False]*len(cat_arr)
			else:
				for reg in cat_arr:
					#remove j char from the end of the string and check has_key
					value.append(nic_list[li].has_key(reg))
			#values[li] = value
			values.append(value)
		#max(values) == 0 means we got not hit at all
		val_sum = [sum(x) for x in values]
		index = val_sum.index(max(val_sum))
		max_val = max(val_sum)

		#grab all possible hits
		if max_val > 0:
			nic_update_arr = []
			temp_v = 0
			#nic_update_arr.append(section_list[index][cat_arr[0]])
			for ind in range(category_count):
				if(values[index][ind]):
					nic_update_arr.append(nic_list[index][cat_arr[ind]])

			#error check
			if len(nic_update_arr) > len(filter(None,nic_update_arr)):
				print 'ERROR: there are empty value =>'+str(nic_update_arr) + ' ==> ' + doc["ref"]

			return ", ".join(list(set(nic_update_arr))),section_list[index][cat_arr[0]]
		else:
			error_log.append("## no data found for reg => "+ doc["ref"])
			#print "## no data found for reg => "+ doc["_source"]["ref"]
			return 'Uncategorized','Uncategorized'
	else:
		return 'Uncategorized','Uncategorized'

def test(ref,nic_list,section_list):
	reg_no =  ref[1:6]
	year_of_registration = int(ref[8:12])
	list_choice = 1 #default 
	if year_of_registration < 2008:
		list_choice = 0
	cat_arr = get_cat_array(reg_no)

	values = []
	# for li in range(list_choice,len(nic_list)):
	for li in range(len(nic_list)):
		value = []
		if li < list_choice: 
			value = [False]*len(cat_arr)
		else:
			for reg in cat_arr:
				#remove j char from the end of the string and check has_key
				value.append(nic_list[li].has_key(reg))
		#values[li] = value
		values.append(value)
	#max(values) == 0 means we got not hit at all
	val_sum = [sum(x) for x in values]
	index = val_sum.index(max(val_sum))
	max_val = max(val_sum)

	#grab all possible hits
	if max_val > 0:
		nic_update_arr = []
		temp_v = 0
		#nic_update_arr.append(section_list[index][cat_arr[0]])
		for ind in range(category_count):
			if(values[index][ind]):
				nic_update_arr.append(nic_list[index][cat_arr[ind]])

		#error check
		if len(nic_update_arr) > len(filter(None,nic_update_arr)):
			print 'ERROR: there are empty value =>'+str(nic_update_arr) + ' ==> ' + resp["_source"]["ref"]
	print list(set(nic_update_arr))

if __name__ == '__main__':
	DB_NAME = sys.argv[1]
	not_bg_mode = False if sys.argv[2].lower() == "y" else True
	nic_list_04, nic_sec_04 = parse_nic('nic_2004.txt',not_bg_mode)
	nic_list_08, nic_sec_08 = parse_nic('nic_2008.txt',not_bg_mode)
	update_mongo([nic_list_04,nic_list_08],[nic_sec_04,nic_sec_08],DB_NAME,not_bg_mode)

	#print nic_list_08['73200']
	#test('U63113KA2013PTC067859',[nic_list_04,nic_list_08],[nic_sec_04,nic_sec_08])	
	
