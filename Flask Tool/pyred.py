############ REQUIREMENTS ####################
# sudo apt-get install python-pip 
# sudo apt-get install libpq-dev
# sudo pip install psycopg2
# sudo pip install sqlalchemy
# sudo pip install sqlalchemy-redshift
##############################################
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


def querySHOW(num_return,sell_name,add_name):
	#print num_return
	cols = ['id','property_address','buyer','seller','transaction_date','property_id','property_type','transaction_amount','loan_amount','lender','sqft','year_built']
	DATABASE = "mydb"
	USER = "sjandhy"
	PASSWORD = "Jandhyala4"
	HOST = "sjandhy.ctfge7lkct5r.us-west-1.redshift.amazonaws.com"
	PORT = "5439"
	SCHEMA = "public"      #default is "public" 

####### connection and session creation ############## 
	connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % (USER,PASSWORD,HOST,str(PORT),DATABASE)
	engine = sa.create_engine(connection_string)
	session = sessionmaker()
	session.configure(bind=engine)
	s = session()
	SetPath = "SET search_path TO %s" % SCHEMA
	s.execute(SetPath)
###### All Set Session created using provided schema  #######

#def querySHOW(num_return):
	#CONNECTRED()
	#query = "select * from LH where (buyer like '%' + @num_return + '%'); "
	#querys = "select count(*) from (select distinct(buyer) from LH where (buyer like '%' + @num_return + '%') group by buyer); "
	buy_like = "%" + num_return + "%"
	sell_like = "%" + sell_name + "%"
	add_like = "%" + add_name + "%"
	
	query_b = "select count(*) from (select distinct(buyer) from LH where (buyer like :vall) group by buyer); "
	query_s = "select count(*) from (select distinct(seller) from LH where (seller like :vall) group by seller); "
	query_a = "select count(*) from (select distinct(property_address) from LH where (property_address like :vall) group by property_address); "
	
	query_b1 = "select count(*) from (select distinct(buyer) from LH where (buyer = :vall) group by buyer); "
	query_s1 = "select count(*) from (select distinct(seller) from LH where (seller = :vall) group by seller); "
	query_a1 = "select count(*) from (select distinct(property_address) from LH where (property_address = :vall) group by property_address); "
	
	#bpass = 0
	#spass = 0
	#apass = 0
	
	err_message = ""
	if  len(num_return) != 0:
		no_b = 1
		rcheck_b = s.execute(query_b1,{'vall': num_return})
		b_result = rcheck_b.fetchall()
		b_results = b_result[0][0]
		if  b_results == 1:
			bpass = 1
			#pass
		else:
			#b_results = 0
			bpass = 0
			rcheck_b = s.execute(query_b,{'vall': buy_like})
			b_result = rcheck_b.fetchall()
			b_results = b_result[0][0]
			if b_results > 1:
				err_message += 'Buyer not specific : ' + num_return
			elif b_results == 0:
				err_message += 'No Such Buyer'
	else:
		no_b= 0
	
	if len(sell_name) !=0:
		no_s = 1
		rcheck_s = s.execute(query_s1,{'vall': sell_name})
		s_result = rcheck_s.fetchall()
		s_results = s_result[0][0]
		if s_results == 1:
			spass = 1
			#pass
		else:
			#s_results = 0
			spass = 0
			rcheck_s = s.execute(query_s,{'vall': sell_like})
			s_result = rcheck_s.fetchall()
			s_results = s_result[0][0]
			if s_results > 1:
				err_message += 'Seller not specific : ' + sell_name
			elif s_results== 0:
				err_message += 'No such Seller : ' + str(s_results)
	else:
		no_s= 0
	
	if len(add_name) != 0:
		no_a = 1
		rcheck_a = s.execute(query_a1,{'vall': add_name})
		a_result = rcheck_a.fetchall()
		a_results = a_result[0][0]
		if a_results == 1:
			apass = 1
			#pass
		else:
			#a_results = 0
			apass = 0
			rcheck_a = s.execute(query_a,{'vall': add_like})
			a_result = rcheck_a.fetchall()
			a_results = a_result[0][0]
			if a_results > 1:
				err_message += 'Address not specific : ' + add_name
			elif a_results == 0:
				err_message += 'No Such Address ' + str(a_results)
	else:
		no_a= 0
	
	
	
	
	if  no_b == 0 and no_s ==0 and no_a == 0:
		str_result = 'Please enter some data'
		return str_result
	elif no_b != 0 and no_s ==0 and no_a == 0:
		if len(err_message) == 0:
			if bpass != 1:
				query = "select * from LH where buyer like :val; "
			else:
				query = "select * from LH where buyer = :val; "
				buy_like = num_return
			rr = s.execute(query,{'val': buy_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	elif no_b == 0 and no_s !=0 and no_a == 0:
		if len(err_message) == 0:
			if spass != 1:
				query = "select * from LH where seller like :val; "
			else:
				query = "select * from LH where seller = :val; "
				sell_like = sell_name
			rr = s.execute(query,{'val': sell_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	elif no_b == 0 and no_s ==0 and no_a != 0:
		if len(err_message) == 0:
			if apass != 1:
				query = "select * from LH where property_address like :val; "
			else:
				query = "select * from LH where property_address = :val; "
				add_like = add_name
			rr = s.execute(query,{'val': add_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	elif no_b != 0 and no_s !=0 and no_a == 0:
		if len(err_message) == 0:
			if bpass != 1 and spass!= 1:
				query = "select * from LH where seller like :val1 and buyer like :val2; "
			elif bpass !=1 and spass == 1:
				query = "select * from LH where seller = :val1 and buyer like :val2; "
				sell_like = sell_name
			elif bpass == 1 and spass != 1:
				query = "select * from LH where seller like :val1 and buyer = :val2; "
				buy_like = num_return
			else:
				query = "select * from LH where seller = :val1 and buyer = :val2; "
				sell_like = sell_name
				buy_like = num_return
			rr = s.execute(query,{'val1': sell_like, 'val2': buy_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	elif no_b != 0 and no_s ==0 and no_a != 0:
		if len(err_message) == 0:
			if bpass != 1 and apass!= 1:
				query = "select * from LH where property_address like :val1 and buyer like :val2; "
			elif bpass !=1 and apass == 1:
				query = "select * from LH where property_address = :val1 and buyer like :val2; "
				add_like = add_name
			elif bpass == 1 and apass != 1:
				query = "select * from LH where property_address like :val1 and buyer = :val2; "
				buy_like = num_return
			else:
				query = "select * from LH where property_address = :val1 and buyer = :val2; "
				add_like = add_name
				buy_like = num_return			
			rr = s.execute(query,{'val1': add_like, 'val2': buy_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	elif no_b == 0 and no_s !=0 and no_a != 0:
		if len(err_message) == 0:
			if spass != 1 and apass!= 1:
				query = "select * from LH where seller like :val1 and property_address like :val2; "
			elif spass !=1 and apass == 1:
				query = "select * from LH where property_address = :val1 and seller like :val2; "
				add_like = add_name
			elif spass == 1 and apass != 1:
				query = "select * from LH where seller = :val1 and property_address like :val2; "
				sell_like = sell_name
			else:
				query = "select * from LH where seller = :val1 and property_address = :val2; "
				add_like = add_name
				sell_like = sell_name
			rr = s.execute(query,{'val1': sell_like, 'val2': add_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	elif no_b != 0 and no_s !=0 and no_a != 0:
		if len(err_message) == 0 :
			if bpass != 1:
				if spass != 1 and apass!= 1:
					query = "select * from LH where seller like :val1 and buyer like :val2 and property_address like :val3; "
				elif spass !=1 and apass == 1:
					query = "select * from LH where seller like :val1 and buyer like :val2 and property_address = :val3; "
					add_like = add_name
				elif spass == 1 and apass != 1:
					query = "select * from LH where seller = :val1 and property_address like :val3 and buyer like :val2; "
					sell_like = sell_name
				else:
					query = "select * from LH where seller = :val1 and property_address = :val3 and buyer like :val2; "
					add_like = add_name
					sell_like = sell_name
					
			if bpass == 1:
				buy_like = num_return
				if spass != 1 and apass!= 1:
					query = "select * from LH where seller like :val1 and buyer = :val2 and property_address like :val3; "
				elif spass !=1 and apass == 1:
					query = "select * from LH where seller like :val1 and buyer = :val2 and property_address = :val3; "
					add_like = add_name
				elif spass == 1 and apass != 1:
					query = "select * from LH where seller = :val1 and property_address like :val3 and buyer = :val2; "
					sell_like = sell_name
				else:
					query = "select * from LH where seller = :val1 and property_address = :val3 and buyer = :val2; "
					add_like = add_name
					sell_like = sell_name
				
			rr = s.execute(query,{'val1': sell_like, 'val2': buy_like, 'val3': add_like})
			all_results =  rr.fetchall()
			x = pd.DataFrame(all_results,columns= cols,index=None)
			return x
		else:
			return err_message
	
	
	"""
	query = "select * from LH where buyer like :val; "
	query1 = "select count(*) from (select distinct(buyer) from LH where (buyer like :vall) group by buyer); "
	rcheck1 = s.execute(query1,{'vall': buy_like})
	r_result = rcheck1.fetchall()
	r_results = r_result[0][0]
	if r_results == 0:
		str_result = 'No Such Borrower on records'
		exp = 10
		return (str_result)
	elif r_results == 1:
		rr = s.execute(query,{'val': buy_like})
		all_results =  rr.fetchall()
		exp = 11
		x = pd.DataFrame(all_results,columns= cols,index=None)
		return x
	else:
		str_result = 'There are multiple such buyers. Please be specific'
		exp = 12
		return (str_result)
	"""
	s.close()
'''
#def pretty(all_results):
    for row in all_results :
        print "row start >>>>>>>>>>>>>>>>>>>>"
        for r in row :
            print " ----" , r
        print "row end >>>>>>>>>>>>>>>>>>>>>>"


#pretty(all_results)


########## close session in the end ###############
	s.close()
	'''
#querySHOW('FEDERAL NATIONAL TRUST 2004-W6')
