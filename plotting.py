import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

path = 'C:\Users\jsowm\Documents\LendingHome\Data Mining\\'
#filename = path + 'year-wise-cummulative.csv'
filename = path + 'count.csv'
filename = path + 'what_happened_to_best_buyers_per_year.csv'
filename = path + 'best_seller.csv'
filename = path + 'best_lender.csv'

data = pd.read_csv(filename,index_col = False)

#data = data.sort_values('Year')
print data.head()

fig = plt.figure()
ax = fig.add_subplot(111)
'''
plt.plot(data['Transaction year'],data['Net Transaction amount'],'g-', label='Net Transaction amount per year')
plt.plot(data['Transaction year'],data['Net Loan amount'],'b-', label='Net Loan amount per year')

#ax.get_xaxis().get_major_formatter().set_scientific(False)
plt.ticklabel_format(useOffset=False)
plt.xticks(np.arange(2012,2018,1))
plt.yticks(np.arange(min(data.iloc[:,1:].min()),max(data.iloc[:,1:].max()),100000000000))

#ax.set_xlim(xmin = 2012,xmax = 2016,1)
#plt.ylim(100000000000,)

fig.savefig('year-wise-cummulative', dpi=fig.dpi)

fig.suptitle('Year-wise-cummulative', fontsize=20)
plt.xlabel('years', fontsize=18)
plt.ylabel('Dollars', fontsize=16)

plt.legend()
plt.show()
'''

"""
plt.plot(data['Year'],data['buyer_count'],'g-', label='Net Buyer count per year')
plt.plot(data['Year'],data['seller_count'],'r-', label='Net Seller count per year')
plt.plot(data['Year'],data['lender_count'],'b-', label='Net lender count per year')
plt.plot(data['Year'],data['property_count'],'v-', label='Net property count per year')
plt.plot(data['Year'],data['transaction_count'],'p-', label='Net transaction count per year')

#ax.get_xaxis().get_major_formatter().set_scientific(False)
plt.ticklabel_format(useOffset=False)
plt.xticks(np.arange(2012,2018,1))
plt.yticks(np.arange(min(data.iloc[:,1:].min()),max(data.iloc[:,1:].max())+10000,10000))

#ax.set_xlim(xmin = 2012,xmax = 2016,1)
#plt.ylim(100000000000,)

fig.savefig('year-wise-cummulative', dpi=fig.dpi)

fig.suptitle('Year-wise-cummulative', fontsize=20)
plt.xlabel('years', fontsize=18)
plt.ylabel('Count', fontsize=16)

plt.legend(loc='best')
plt.show()
"""
'''
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
plt.suptitle('Seller with Most Transactions per year and their transactions hence', fontsize=20)


def plotyear(i,a,b):
	print data.ix[b-1,'seller'],data.ix[b-1,'base_year']
	print data.ix[a:b]
	plt.subplot(3,1,i)
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'net_transaction_amount'],'g-', label='Net transaction amount per year')
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'net_loan_amount'],'r-', label='Net loan amount per year')
	plt.ticklabel_format(useOffset=False)
	plt.xticks(np.arange(2012,2018,1))
	plt.xlabel('years', fontsize=10)
	plt.ylabel('Dollars', fontsize=10)
	
plotyear(1,0,5)
plt.gca().set_title('{} for year {}-2015'.format(data.ix[0,'seller'],data.ix[0,'base_year']),fontsize=13)

#plotyear(2,6,11)
plt.legend(loc='best')
#plotyear(3,12,17)
#plotyear(4,18,23)
plotyear(2,24,29)
plt.gca().set_title('{} for year {}'.format(data.ix[24,'seller'],data.ix[24,'base_year']),fontsize=13)

plotyear(3,30,35)
plt.gca().set_title('{} for year {}'.format(data.ix[30,'seller'],data.ix[30,'base_year']),fontsize=13)


#fig = plt.figure()

#plt.ticklabel_format(useOffset=False)
#plt.xticks(np.arange(2012,2018,1))

plt.show()
"""
plt.suptitle('Lender with Most Transactions per year and their transactions hence', fontsize=20)


def plotyear(i,a,b):
	print data.ix[b-1,'lender'],data.ix[b-1,'base_year']
	print data.ix[a:b]
	plt.subplot(3,1,i)
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'net_transaction_amount'],'g-', label='Net transaction amount per year')
	plt.plot(data.ix[a:b,'year'],data.ix[a:b,'net_loan_amount'],'r-', label='Net loan amount per year')
	plt.ticklabel_format(useOffset=False)
	plt.xticks(np.arange(2012,2018,1))
	plt.xlabel('years', fontsize=10)
	plt.ylabel('Dollars', fontsize=10)
	
plotyear(1,0,5)
plt.gca().set_title('{} for year {}-2014'.format(data.ix[0,'lender'],data.ix[0,'base_year']),fontsize=13)

#plotyear(2,6,11)
plt.legend(loc='best')
#plotyear(3,12,17)
plotyear(2,18,23)
plt.gca().set_title('{} for year {}'.format(data.ix[18,'lender'],data.ix[18,'base_year']),fontsize=13)

plotyear(3,24,29)
plt.gca().set_title('{} for year {}-2017'.format(data.ix[24,'lender'],data.ix[24,'base_year']),fontsize=13)

#plotyear(3,30,35)
#plt.gca().set_title('{} for year {}'.format(data.ix[30,'lender'],data.ix[30,'base_year']),fontsize=13)


#fig = plt.figure()

#plt.ticklabel_format(useOffset=False)
#plt.xticks(np.arange(2012,2018,1))

plt.show()
