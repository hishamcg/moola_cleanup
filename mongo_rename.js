var bool_mode = false
if (mode == "deep"){
	bool_mode = true
}else{
	bool_mode = false
}
db= new Mongo("127.0.0.1:27017").getDB(db_name)

print ("*****  connected to "+db_name)

var bool_rename = false,bool_nic_delete = false

print ("\n*****  checking email normalization")
if (bool_mode){
	print ("*****  started deep scan...")
	if (db.parties.findOne( { email_id: { $exists: true } } )){bool_rename = true }
}else{
	if (db.parties.findOne({"_type":"PrivateCompany"}).email_id){bool_rename = true}
}

if (bool_rename){
	if (set_base_value == "y"){
		print ("*****  changing \"email_id\" => \"email\" and increase base \"conversion\" value by 100")
		print ("*****  this will take sometime...")
		db.parties.updateMany( {}, { $rename: { "email_id": "email"},$inc: { "conversion": 100 }} ,{
			allowDiskUse:true,
			cursor:{}
			}
		)
	}else{
		print ("*****  changing \"email_id\" => \"email\"")
		print ("*****  this will take sometime...")
		db.parties.updateMany( {}, { $rename: { "email_id": "email"}} ,{
			allowDiskUse:true,
			cursor:{}
			}
		)
	}
	print ("*****  completed")
}else if (set_base_value == "y"){
	print ("*****  No email normalization required")
	print ("*****  increase base \"conversion\" value by 100")
	print ("*****  this will take sometime...")
	db.parties.updateMany( {}, {$inc: { "conversion": 100 }} ,{
		allowDiskUse:true,
		cursor:{}
		}
	)
}else {
	print ("*****  No email normalization required")
}

print ("\n*****  checking presence of \"nic_string\" field")
if (bool_mode){
	print ("*****  started deep scan...")
	if (db.registrations.findOne({"_type":"CorporateIdentificationNumber", nic_string: {$exists: true }})){bool_nic_delete = true }
}else{
	if (db.registrations.findOne({"_type":"CorporateIdentificationNumber"}).nic_string){bool_nic_delete = true}
}

if (bool_nic_delete){
	print ("*****  removing \"nic_string\" field")
	print ("*****  this will take sometime...")
	db.registrations.updateMany( {}, { $unset: { "nic_string": 1 } } ,{
		allowDiskUse:true,
		cursor:{}
		}
	)
	print ("*****  completed")
}else{
	print ("*****  No need to remove \"nic_string\". it doesnt exist")
}

db.close