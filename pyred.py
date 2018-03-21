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
import matplotlib.pyplot as plt
import numpy as np

cols = ['id','property_address','buyer','seller','transaction_date','property_id','property_type','transaction_amount','loan_amount','lender','sqft','year_built']
cols1 = ['buyer','seller','avg_transaction_date','sum_transaction_amount','sum_loan_amount','avg_sqft','avg_year_built']

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

query = "select * from LH ORDER BY BUYER , SELLER, transaction_date,transaction_amount,loan_amount; "
#querys = "select count(*) from (select distinct(buyer) from LH where (buyer like '%' + @num_return + '%') group by buyer); "
'''
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

'''


#query = "select buyer, seller, avg(extract(year from transaction_date)) as avg_transaction_date, sum(transaction_amount) as sum_transaction_amount, sum(loan_amount) as sum_loan_amount, avg(sqft) as avg_sqft,avg(year_built) as avg_year_built from LH where property_address like '% CA%' and property_address != ' ' group by buyer,seller  order by buyer desc, seller desc;"

#query = "select buyer, seller, avg(extract(year from transaction_date)), sum(transaction_amount), sum(loan_amount), avg(sqft),avg(year_built) from LH where property_address != ' ' group by buyer,seller  order by buyer desc, seller desc;"

query = "select * from LH_act;"

print query
rr = s.execute(query)
print 'YOOOO'
all_results =  rr.fetchall()
#x = pd.DataFrame(all_results,columns= cols1,index=None)

x = pd.DataFrame(all_results,columns= cols,index=None)

print x.head()
print x.describe()
print len(x)
print x.ix[:,'sqft'].min(), x.ix[:,'sqft'].max()

fig = plt.figure()
'''
plt.subplot(2,2,1)
plt.scatter(x.index.tolist(),x['sum_transaction_amount'].values)

plt.subplot(2,2,2)
plt.scatter(x.index.tolist(),x['sum_loan_amount'].values)

plt.subplot(2,2,3)
plt.scatter(x.index.tolist(),x['avg_sqft'].values)

plt.subplot(2,2,4)
plt.scatter(x.index.tolist(),x['avg_transaction_date'].values)
'''
s.close()

def plotting(name,i):
	path = 'C:\Users\jsowm\Documents\LendingHome\Data Mining\\'
	filename = path + name
	plt.subplot(4,3,6*i+1)
	plt.scatter(x.index.tolist(), x['transaction_amount'].values)

	plt.subplot(4,3,6*i+2)
	plt.scatter(x.index.tolist(), x['loan_amount'].values)

	plt.subplot(4,3,6*i+3)
	plt.scatter(x['sqft'].values, x['transaction_amount'].values)

	plt.subplot(4,3,6*i+4)
	plt.scatter(x['sqft'].values, x['loan_amount'].values)

	plt.subplot(4,3,6*i+5)
	plt.scatter(x['transaction_date'].values, x['transaction_amount'].values)
	
	plt.subplot(4,3,6*i+6)
	plt.scatter(x['transaction_date'].values, x['loan_amount'].values)
	
	#x.plot.scatter(x.index.tolist(),x['sum_transaction_amount'])
	fig.savefig('{}.png'.format(filename), dpi=fig.dpi)
	

	
x = x.sort_values('sqft')	
x= x.reset_index()
l = len(x)
'''
print l, (2*l//3)
#a = x.ix[:(l//3),'sqft']
a = x.ix[:(3*l//4),'sqft']
b = x.ix[(3*l//4):,'sqft']	
a = a.reset_index()
b = b.reset_index()
#print a.head()
print a.head()
print b.head()
'''

dmin = x.ix[:,'sqft'].min()
dmax = x.ix[:,'sqft'].max()

dmedian = x['sqft'].median()
dmedian = 10000
print dmedian

a = x[x.sqft < dmedian]
b = x[x.sqft >= dmedian]

c = b[b.sqft < b.sqft.mean()]
d = b[b.sqft >= b.sqft.mean()]

e = d[d.sqft < d.sqft.mean()]
f = d[d.sqft >= d.sqft.mean()]

g = f[f.sqft < f.sqft.mean()]
h = f[f.sqft >= f.sqft.mean()]
 
i = h[h.sqft < h.sqft.mean()]
j = h[h.sqft >= h.sqft.mean()]

print a.head()
print b.head()

path = 'C:\Users\jsowm\Documents\LendingHome\Data Mining\\'

plt.suptitle('Density distribution of size of properties', fontsize=20)

plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)
	


plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,2,1)
plt.xlim(xmin=a.ix[:,'sqft'].min(), xmax = a.ix[:,'sqft'].max())
plt.hist(a['sqft'].values, bins = 20)
plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)

plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,2,2)
plt.xlim(xmin=c.ix[:,'sqft'].min(), xmax = c.ix[:,'sqft'].max())
plt.hist(c['sqft'].values, bins = 20)
plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)

'''
plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,1,3)
plt.xlim(xmin=d.ix[:,'sqft'].min(), xmax = d.ix[:,'sqft'].max())
plt.hist(d['sqft'].values, bins = 15)
'''

plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,2,3)
plt.xlim(xmin=e.ix[:,'sqft'].min(), xmax = e.ix[:,'sqft'].max())
plt.hist(e['sqft'].values, bins = 20)
plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)


plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,2,4)
plt.xlim(xmin=g.ix[:,'sqft'].min(), xmax = g.ix[:,'sqft'].max())
plt.hist(g['sqft'].values, bins = 20)
plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)


plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,2,5)
plt.xlim(xmin=i.ix[:,'sqft'].min(), xmax = i.ix[:,'sqft'].max())
plt.hist(i['sqft'].values, bins = 20)
plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)


plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)

plt.subplot(3,2,6)
plt.xlim(xmin=j.ix[:,'sqft'].min(), xmax = j.ix[:,'sqft'].max())
plt.hist(j['sqft'].values, bins = 20)
plt.xlabel('sqft', fontsize=10)
plt.ylabel('Count', fontsize=10)


#x.hist(column = 'sqft', bins =10)
fig.savefig(path+'sqft_bins8.png', dpi=fig.dpi)
plt.show()

'''
#plotting()

#x = x.sort_values('transaction_date')
#plotting('transaction_date_sorted2',0)

#x = x.sort_values('sqft')
#plotting('sqft_sorted2',1)path = 'C:\Users\jsowm\Documents\LendingHome\Data Mining\\'
path = 'C:\Users\jsowm\Documents\LendingHome\Data Mining\\'

plt.gca().set_autoscale_on(False)
plt.ticklabel_format(useOffset=False)
#plt.xticks(np.arange(x.ix[:,'sqft'].min(), x.ix[:,'sqft'].max(),100))


plt.xlim(xmin=x.ix[:,'sqft'].min(), xmax = x.ix[:,'sqft'].max())

plt.hist(x['sqft'].values, bins = 10)

#x.hist(column = 'sqft', bins =10)
fig.savefig(path+'sqft_bins.png', dpi=fig.dpi)
plt.show()
'''