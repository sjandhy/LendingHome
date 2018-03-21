import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

path = 'C:\Users\jsowm\Documents\LendingHome\Data Mining\\'
#filename = path + 'year-wise-cummulative.csv'
#filename = path + 'best_buyer_amts.csv'
"""
data = pd.read_csv(filename,index_col = False)

print data.head()



plt.suptitle('Buyer with Max Loan amount per year and their summary hence', fontsize=20)


def plotyear(i,a,b):
	print data.ix[b-1,'l_buyer'],data.ix[b-1,'base_year']
	print data.ix[a:b]
	plt.subplot(3,2,i)
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'lnet_transaction_amount'],'g-', label='Net transaction amount per year')
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'lnet_loan_amount'],'r-', label='Net loan amount per year')
	plt.ticklabel_format(useOffset=False)
	plt.xticks(np.arange(2012,2018,1))
	plt.xlabel('years', fontsize=10)
	plt.ylabel('Dollars', fontsize=10)
	plt.gca().set_title('{} for year {}'.format(data.ix[b-1,'l_buyer'],data.ix[b-1,'base_year']),fontsize=12)
	


plotyear(1,0,5)
plotyear(2,6,11)
plt.legend(loc='best')
plotyear(3,12,17)
plotyear(4,18,23)
plotyear(5,24,29)
plotyear(6,30,35)

plt.show()
"""



'''
filename = path + 'best_seller_amts.csv'
#filename = path + 'best_lender_amts.csv'

data = pd.read_csv(filename,index_col = False)

plt.suptitle('Buyer with Most Transactions per year and their transactions hence', fontsize=20)


#temp = "32" + str(i)
	#ax = plt.subplot(temp)
	#ax.set_title('{} for year {}'.format(data.ix[a,'buyer'],data.ix[a,'base_year']))
	#




def plotyear(i,a,b):
	print data.ix[b-1,'buyer'],data.ix[b-1,'base_year']
	print data.ix[a:b]
	plt.subplot(3,2,i)
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'net_transaction_amount'],'g-', label='Net transaction amount per year')
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'net_loan_amount'],'r-', label='Net loan amount per year')
	plt.ticklabel_format(useOffset=False)
	plt.xticks(np.arange(2012,2018,1))
	plt.xlabel('years', fontsize=10)
	plt.ylabel('Dollars', fontsize=10)
	plt.gca().set_title('{} for year {}'.format(data.ix[b-1,'buyer'],data.ix[b-1,'base_year']),fontsize=13)
	




	#plt.plot(data['year'],data['count_of_transactions'],'b-', label='Count of transactions per year')
	
#plt.xlabel('years', fontsize=18)
#plt.ylabel('Count', fontsize=16)

plotyear(1,0,5)
plotyear(2,6,11)
plt.legend(loc='best')
plotyear(3,12,17)
plotyear(4,18,23)
plotyear(5,24,29)
plotyear(6,30,35)

#fig = plt.figure()

#plt.ticklabel_format(useOffset=False)
#plt.xticks(np.arange(2012,2018,1))

plt.show()
'''
"""
filename = path + 'best_seller_amts.csv'
#filename = path + 'best_lender_amts.csv'

data = pd.read_csv(filename,index_col = False)

plt.suptitle('Seller with Max Loan amount per year and their summary hence', fontsize=20)


def plotyear(i,a,b):
	print data.ix[b-1,'l_seller'],data.ix[b-1,'base_year']
	print data.ix[a:b]
	plt.subplot(3,2,i)
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'lnet_transaction_amount'],'g-', label='Net transaction amount per year')
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'lnet_loan_amount'],'r-', label='Net loan amount per year')
	plt.ticklabel_format(useOffset=False)
	plt.xticks(np.arange(2012,2018,1))
	plt.xlabel('years', fontsize=10)
	plt.ylabel('Dollars', fontsize=10)
	plt.gca().set_title('{} for year {}'.format(data.ix[b-1,'l_seller'],data.ix[b-1,'base_year']),fontsize=12)

	
plotyear(1,0,5)
#plt.gca().set_title('{} for year {}-2015'.format(data.ix[0,'seller'],data.ix[0,'base_year']),fontsize=13)
plotyear(2,6,11)
plt.legend(loc='best')
plotyear(3,12,17)
plotyear(4,18,23)
plotyear(5,24,29)
#plt.gca().set_title('{} for year {}'.format(data.ix[24,'seller'],data.ix[24,'base_year']),fontsize=13)

plotyear(6,30,35)
#plt.gca().set_title('{} for year {}'.format(data.ix[30,'seller'],data.ix[30,'base_year']),fontsize=13)


#fig = plt.figure()

#plt.ticklabel_format(useOffset=False)
#plt.xticks(np.arange(2012,2018,1))

plt.show()
"""
'''
filename = path + 'best_lender_amts.csv'

data = pd.read_csv(filename,index_col = False)

plt.suptitle('Lender with Most Loan amount per year and their summary hence', fontsize=20)


def plotyear(i,a,b):
	print data.ix[b-1,'l_lender'],data.ix[b-1,'base_year']
	print data.ix[a:b]
	plt.subplot(3,2,i)
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'lnet_transaction_amount'],'g-', label='Net transaction amount per year')
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'lnet_loan_amount'],'r-', label='Net loan amount per year')
	plt.ticklabel_format(useOffset=False)
	plt.xticks(np.arange(2012,2018,1))
	plt.xlabel('years', fontsize=10)
	plt.ylabel('Dollars', fontsize=10)
	plt.gca().set_title('{} for year {}'.format(data.ix[b-1,'l_lender'],data.ix[b-1,'base_year']),fontsize=13)

	
plotyear(1,0,5)
#plt.gca().set_title('{} for year {}-2014'.format(data.ix[0,'lender'],data.ix[0,'base_year']),fontsize=13)

plotyear(2,6,11)
plt.legend(loc='best')
plotyear(3,12,17)
plotyear(4,18,23)
#plt.gca().set_title('{} for year {}'.format(data.ix[18,'lender'],data.ix[18,'base_year']),fontsize=13)

plotyear(5,24,29)
#plt.gca().set_title('{} for year {}-2017'.format(data.ix[24,'lender'],data.ix[24,'base_year']),fontsize=13)

plotyear(6,30,35)
#plt.gca().set_title('{} for year {}'.format(data.ix[30,'lender'],data.ix[30,'base_year']),fontsize=13)


#fig = plt.figure()

#plt.ticklabel_format(useOffset=False)
#plt.xticks(np.arange(2012,2018,1))

plt.show()
'''

filename = path + 'property_type.csv'

data = pd.read_csv(filename,index_col = False)

print data
plt.suptitle('Density distribution for Property type ', fontsize=20)

Px = [1,2,3,4,5,6,7,8,9,10,11,12]
Pvalues = data['count'].values
labels = data['property_type'].values

plt.bar(Px,Pvalues,align='center')
plt.xticks(Px,labels)
plt.show()