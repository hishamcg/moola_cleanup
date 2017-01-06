#!/bin/bash
RED='\033[0;31m'
NC='\033[0m'
White='\033[0;37m'
Green='\033[1;32m'
source ~/.bash_profile

echo "*****************************************"
echo "*****                               *****"
echo "*****        []                     *****"
echo "*****      []                       *****"
echo "*****    []                         *****"
echo "*****  []          []          []   *****"
echo "*****    []      []  []      []     *****"
echo "*****      []  []      []  []       *****"
echo "*****        []          []         *****"
echo "*****                               *****"
echo "*****************************************"
echo "*****                               *****"
echo "*****        MONGO ES UPDATE        *****"
echo "*****                               *****"
echo "*****************************************"
echo ""

function start(){
	_DB_NAME="$1"
	mode="$2"
	if [ -n "$_DB_NAME" ]; then
		echo -e "*****  using ${Green}$_DB_NAME ${NC}DB"
	elif [ "$MOOLA_DB_NAME" ]; then
		_DB_NAME=$MOOLA_DB_NAME
		echo -e "*****  using ${Green}$_DB_NAME ${NC}DB"
	else
		echo -e "      ${RED}ERROR: DB NAME NOT FOUND"
		echo ""
		echo -e "      ${NC}please pass mongo db name along with the bash file."
		echo -e "      eg: ${Green}./run.sh moola_development ${NC}"
		echo -e "              ${White}------ OR -----${NC}" 
		echo -e "      add environment variable ${Green}\"MOOLA_DB_NAME=your_db_name\" ${NC}to .bash_profile"
		echo -e "              ${White}------ OR -----${NC}" 
		echo -e "      use ${Green}-h ${NC} for help"
		echo ""
	fi

	if [ "$_DB_NAME" ]; then
		echo "*****  Run rename? y/n (this will rename email_id -> email and pin -> pincode)"
		read rename
		echo "*****  Increase the base value of \"conversion\" field? y/n"
		read inp
		echo "*****  Run nic update? y/n"
		read do_nic
		if [ "$do_nic" = "y" ]; then
			echo "*****  In Background mode? y/n"
			read bg_mode
		fi
		
		mongo --quiet --eval "var db_name='$_DB_NAME',mode='$mode',set_base_value='$inp',rename='$rename'" mongo_rename.js
		
		# if [ "$inp" == "y" ]; then
		# 	mongo --quiet --eval "var db_name='$_DB_NAME'" mongo_set_rank_base_value.js
		# fi

		if [ "$do_nic" == "y" ]; then 
			echo "*****  Updating industry code and other fields from nic04 and nic 08"
			if [ "$bg_mode" == "y" ]; then
				python script/nic_mongo.py "$_DB_NAME" "$bg_mode" &
				disown
			else
				python script/nic_mongo.py "$_DB_NAME" "$bg_mode"
			fi
		fi
	fi
}

if [ "$1" = "-h" ]; then
	echo "this script help you to clean your data and standardize it"
	echo ""
	echo "-- use below conditions"
	echo ""
	echo "       -h  : print this message"
	echo "       -b  : base scan (default). use if you are running"
	echo "             the cleanup first time. FAST"
	echo "       -d  : deep scan. use in case this update failed" 
	echo "             somewhere. SLOW"
else
	if [ "$1" = "-d" ]; then
		start "$2" "deep"
	elif [ "$1" = "-b" ]; then
		start "$2" "basic"
	else
		start "$1" "basic"
	fi
fi