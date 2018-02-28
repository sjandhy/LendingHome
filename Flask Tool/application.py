'''
Simple Flask application to test deployment to Amazon Web Services
Uses Elastic Beanstalk and RDS

Author: Scott Rodkey - rodkeyscott@gmail.com

Step-by-step tutorial: https://medium.com/@rodkey/deploying-a-flask-application-on-aws-a72daba6bb80
'''

from flask import Flask, render_template, request
from application import db
#from application.models import Data
from application.forms import EnterDBInfo, RetrieveDBInfo
import pyred




# Elastic Beanstalk initalization
application = Flask(__name__)
application.debug=True
# change this to your own value
application.secret_key = 'cC1YCIWOj9GgWspgNEo2'   

@application.route('/', methods=['GET', 'POST'])
@application.route('/index', methods=['GET', 'POST'])
def index():
    form1 = EnterDBInfo(request.form) 
    form2 = RetrieveDBInfo(request.form) 
    if request.method == 'POST' and form2.validate():
		#num_return = str(form2.numRetrieve.data)
		#num_return=request.form['numRetrieve_field']
		#sell_name= request.form['SellerRetrieve_field']
		#add_name=request.form['AddressRetrieve_field']
		num_return = request.form.get('numRetrieve', "")
		sell_name = request.form.get('SellerRetrieve', "")
		add_name = request.form.get('AddressRetrieve', "")
		
            #print num_return
            #query_db = Data.query.order_by(Data.id.desc()).limit(num_return)
		query_db = pyred.querySHOW(num_return,sell_name,add_name)
            #print query_db
            #for q in query_db:
             #   print(q.notes)
		db.session.close()
		try:
			return render_template('results.html', results=query_db, num_return=num_return)
		except:
			return render_template('results1.html', results=query_db, num_return=num_return)                
    
    return render_template('index.html', form1=form1, form2=form2)

if __name__ == '__main__':
    application.run(host='0.0.0.0')