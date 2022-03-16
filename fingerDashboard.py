import pandas as pd
import sqlalchemy as db
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from datetime import date

engine = db.create_engine("mysql+mysqldb://tebahsoft:"+"finger.ai2021"+"@app.finger.solutions:3306/fingerdb", encoding='utf-8')
conn = engine.connect()

user_data = pd.read_sql_query('SELECT * FROM auth_user', conn)
interview_data = pd.read_sql_query('SELECT * FROM interviews_interviews', conn)

# amount (쿠폰종류) , expire_at (종료일) , used_count (사용한갯수) , user_count (사용가능 개수)
coupon_data = pd.read_sql_query('SELECT * FROM interviews_purchase', conn)

# 오늘 접속한 사람 수 (Daily Active Users)
dau = pd.read_sql_query('SELECT count(*) FROM auth_user where day(last_login)=day(Now()) and month(last_login)=month(Now()) and year(last_login)=year(Now())', conn)
dauVal = int(dau['count(*)'][0])

# 최근1주일 접속한 사람 수 (Daily Active Users)
wau = pd.read_sql_query('SELECT count(*) FROM auth_user where DATE(last_login) > (CURDATE()- INTERVAL 7 DAY)', conn)
wauVal = int(wau['count(*)'][0])

# 최근1달 접속한 사람 수 (Daily Active Users)
mau = pd.read_sql_query('SELECT count(*) FROM auth_user where DATE(last_login) > (CURDATE()- INTERVAL 1 MONTH)', conn)
mauVal = int(mau['count(*)'][0])

# 오늘 가입한 사람 수 (Daily Joined Users)
dju = pd.read_sql_query('SELECT count(*) FROM auth_user where day(date_joined)=day(Now()) and month(date_joined)=month(Now()) and year(date_joined)=year(Now())', conn)
djuVal = int(dju['count(*)'][0])

# 오늘 등록된 인터뷰 수 (Daily Uploaded Items)
dui = pd.read_sql_query('SELECT count(*) FROM interviews_interviews where DATE(expire_at) = CURDATE() and share_flag = false and delete_flag = false', conn)
duiVal = int(dui['count(*)'][0])

# 오늘 등록된 인터뷰 시간 (Daily Uploaded Time)
dut = pd.read_sql_query('SELECT sum(duration) FROM interviews_interviews where DATE(expire_at) = CURDATE() and share_flag = false and delete_flag = false', conn)
if dut['sum(duration)'][0] is None:
    dutVal = 0
else:
    dutVal = int(dut['sum(duration)'][0])

# 오늘 발급된 쿠폰 개수 (Daily Purchased Coupons)
dpc = pd.read_sql_query('SELECT count(*) FROM interviews_purchase where DATE(expire_at) = (CURDATE()+ INTERVAL 5 YEAR)', conn)
dpcVal = int(dpc['count(*)'][0])

# 오늘 발급된 쿠폰 시간 총합 (Daily Purchased Time)
dpt = pd.read_sql_query('SELECT * FROM interviews_purchase where DATE(expire_at) = (CURDATE()+ INTERVAL 5 YEAR)', conn)
dptVal = 0
for row in dpt.values:
    dptVal = dptVal + int(row[2])*int(row[6])

# 오늘 발급된 실고객 쿠폰 개수 (Daily Purchased Coupons)
cdpc = pd.read_sql_query('SELECT count(*) FROM interviews_purchase where DATE(expire_at) = (CURDATE()+ INTERVAL 5 YEAR) AND owner_id > 3 AND owner_id <> 26 AND owner_id <> 25', conn)
cdpcVal = int(cdpc['count(*)'][0])

# 오늘 발급된 실고객 쿠폰 시간 총합 (Daily Purchased Time)
cdpt = pd.read_sql_query('SELECT * FROM interviews_purchase where DATE(expire_at) = (CURDATE()+ INTERVAL 5 YEAR) AND owner_id > 3 AND owner_id <> 26 AND owner_id <> 25', conn)
cdptVal = 0
for row in cdpt.values:
    cdptVal = cdptVal + int(row[2])*int(row[6])

# 오늘 발급된 쿠폰 금액 (Daily Purchased Fee)
# dpf = pd.read_sql_query('SELECT sum(duration) FROM coupon_data where DATE(expire_at) = (CURDATE()+ INTERVAL 5 YEAR)', conn)

print ("DATE", "DAU", "DJU", "DUI", "DUT", "DPC", "DPT", "DPF")
print (date.today(), dauVal, djuVal, duiVal, dutVal, dpcVal)

table = db.Table('dashboard', db.MetaData(), autoload=True, autoload_with=engine)
query = db.insert(table)
values_list = [{'date': date.today(), 'dau': dauVal, 'wau': wauVal, 'mau': mauVal, 'dju': djuVal, 'dui': duiVal, 'dut': dutVal, 'dpc': dpcVal, 'dpt': dptVal, 'cdpc': cdpcVal, 'cdpt': cdptVal}]
result_proxy = conn.execute(query, values_list)
result_proxy.close()


conn.close()