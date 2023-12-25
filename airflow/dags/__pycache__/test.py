datestring  = '2020-06-12'
from datetime import datetime

dt = datetime.strptime(datestring, '%Y-%m-%d')
print(dt.year)