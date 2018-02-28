import pandas as pd
import ftplib
import urlparse
import os
from zipfile import ZipFile
import boto
from boto.s3.key import Key
import psycopg2
import s3fs
import posixpath





### FTP configuration
## Please change the ftp configurations like url, user and password to connect to appropriate server
## local_root can be treated as the templocation to hold the transfered files

url = 'ftp://10.0.0.253/LendingHome/'
url = urlparse.urlparse(url)
user = ''
password = ''

local_root = os.path.expanduser("~/Documents\LendingHome\Data\\") # change this to wherever you want to download to

def download(ftp, ftp_path, filename, check_cwd=True):
    basename = posixpath.basename(ftp_path)
    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    if check_cwd:
        ftp_dirname = posixpath.dirname(ftp_path)
        if ftp_dirname != ftp.pwd():
            ftp.cwd(ftp_dirname)

    with open(filename, 'wb') as fobj:
        ftp.retrbinary('RETR %s' % basename, fobj.write)
	return filename

def ftp_dir(ftp):
    # use a callback to grab the ftp.dir() output in a list
    dir_listing = []
    ftp.dir(lambda x: dir_listing.append(x))
    return [(line[0].upper() == 'D', line.rsplit()[-1]) for line in dir_listing]

# connect to ftp
ftp = ftplib.FTP(url.netloc)
ftp.login(user,password)

# recursively walk through the directory and download each file, depth first
stack = [url.path]
while stack:
	path = stack.pop()
	ftp.cwd(path)

    # add all directories to the queue
	children = ftp_dir(ftp)
	dirs = [posixpath.join(path, child[1]) for child in children if child[0]]
	files = [posixpath.join(path, child[1]) for child in children if not child[0]] 
	stack.extend(dirs[::-1]) # add dirs reversed so they are popped out in order

    # download all files in the directory
	for filepath in files:
		filename = download(ftp, filepath, os.path.join(local_root, filepath.split(url.path,1)[-1]), check_cwd=False)

# logout
ftp.quit()
print filename



def CorrectionProgram(x,fn,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY):
	cols = ['id','property_address','buyer','seller','transaction_date','property_id','property_type','transaction_amount','loan_amount','lender','sqft','year_built']
	fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID,secret=AWS_SECRET_ACCESS_KEY)
	if fn <> x.ix[0,'filename'].strip():
		print (fn == x.ix[0,'filename'])
		print (fn != x.ix[0,'filename'])
		print (fn is x.ix[0,'filename'])
		print 'Error not in stl_load_errors'
		return 0
	else:
		rowid = x.ix[0,'line_number'] - 2
		col_name = x.ix[0,'colname'].strip()
		with fs.open(fn,'rb') as f:
			data = pd.read_csv(f)
		chg1 = data.ix[rowid, col_name]
		if chg1[0] != "'" :
			print 'Check errors in file'
			return 0
		elif chg1[:-1] != "'":
			chg2 = '"' + chg1[1:] + '"'
			chg2 = chg2.strip()
			print chg2
			data.loc[rowid, col_name] = chg2
			print data.ix[rowid, col_name]
			#print len(data)
			data['transaction_date'] = pd.to_datetime(data.transaction_date)
			btw = data.to_csv(columns=cols, date_format = '%Y-%m-%d',index=False).encode()
			with fs.open(fn,'wb') as f:
				f.write(btw)
	return 2

def S3FileUploadFun(filename,END_POINT,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,BUCKET_NAME):
	s3 = boto.s3.connect_to_region(region_name =END_POINT, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

	bucket = s3.get_bucket(BUCKET_NAME)
	k = Key(bucket)

	with ZipFile(filename, 'r') as zip:
		lname = zip.namelist()
		zip.printdir()
		print 'Extracting the files now...................'
		for i in range(1,len(lname)):
			k.key = lname[i]
			k.set_contents_from_filename(zip.extract(lname[i]))
		print 'Done!!!!!!!!!!'
		print lname
		return lname


### load datato table from S3

def SQLUpload(con,sql,error_sql,fn,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY):
	try:
		cur.execute(sql)
		print 'All done with this file'
	except psycopg2.DatabaseError as e:
		err = str(e)
		print err
		print e
		if err.find(tbl) is not -1:
			con.rollback()
			con = psycopg2.connect(conn_string);
			cur1= con.cursor()
			cur1.execute(error_sql)
			all_results =  cur1.fetchall()
			x = pd.DataFrame(all_results,index=None,columns = stl_cols)
			print x
			exp = CorrectionProgram(x,fn,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
			if exp != 0:
				SQLUpload(con,sql,error_sql,fn,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
			else:
				pass
		else:
			print e





### Get the zip files and ### Upload individual csvs to S3
#### All the following details need to be entered appropriately
AWS_ACCESS_KEY_ID = ''             
AWS_SECRET_ACCESS_KEY = '' 
END_POINT = 'us-west-1'                          
S3_HOST = 's3.us-west-1.amazonaws.com'                            
BUCKET_NAME = 'lendinghometest'       
#path = 'C:\Users\jsowm\Documents\LendingHome\\' 
#FILENAME = path + 'upload.txt'                
#UPLOADED_FILENAME = 'dumps/upload.txt'
# include folders in file path. If it doesn't exist, it will be created


cols = ['id','property_address','buyer','seller','transaction_date','property_id','property_type','transaction_amount','loan_amount','lender','sqft','year_built']
DATABASE = "mydb"
USER = "sjandhy"
PASSWORD = ""
HOST = "sjandhy.ctfge7lkct5r.us-west-1.redshift.amazonaws.com"
PORT = "5439"
SCHEMA = "public"      #default is "public" 



#### TABLE SQL Upload
to_table = 'LH_temp'       ## Change this based on which table you want to load this data into
AWS_IAM_ROLE = ''
delim = ','
s3_path = 's3://lendinghometest/'  ##Bucket name on S3
tbl = "'stl_load_errors'"   ## Error table name


lname = S3FileUploadFun(filename,END_POINT,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,BUCKET_NAME)

conn_string = "dbname=%s user=%s password=%s host=%s port='5439'"%(DATABASE,USER,PASSWORD,HOST)  

error_sql = "select * from stl_load_errors order by starttime desc limit 1;"
stl_cols = ['userid','slice','tbl','starttime','session','query','filename','line_number','colname','type','col_length','position','raw_line','raw_field_value','err_code','err_reason']

print 'Table load begins'


for i in range(1,len(lname)):
	con = psycopg2.connect(conn_string);
	cur = con.cursor()
	fn = s3_path + lname[i]
	sql="""COPY %s FROM '%s' credentials 'aws_iam_role=%s'
	delimiter '%s' IGNOREHEADER 1 removequotes ; commit;""" % (to_table, fn, AWS_IAM_ROLE,delim)
	SQLUpload(con,sql,error_sql,fn,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
	con.commit()
	con.close() 

print 'Thank you'