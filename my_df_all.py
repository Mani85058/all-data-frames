from pyspark.sql import *
from pyspark.sql.functions import *
import random

number_of_records = 10000
spark = SparkSession.builder.getOrCreate()
state_names = "Andhra Pradesh, Arunachal Pradesh, Assam, Bihar, ' \
        'Chhattisgarh, Goa, Gujarat, Haryana, Himachal Pradesh, ' \
        'Jharkhand, Karnataka, Kerala, Madhya Pradesh, Maharashtra, ' \
        'Manipur, Meghalaya, Mizoram, Nagaland, Odisha, Punjab, ' \
        'Rajasthan, Sikkim, Tamil Nadu, Telangana, Tripura, Uttar Pradesh"
fruits = "Apple	Banana	Apricot	Atemoya Avocados	Blueberry	Blackcurrant" \
    "	Ackee Cranberry	Cantaloupe	Cherry	Black sapote/Chocolate pudding fruit Dragonfruit	Dates	Cherimoya	Buddha’s " \
    "hand fruit Finger lime	Fig	Coconut	Cape gooseberry/Inca berry/Physalis Grapefruit	Gooseberries	Custard " \
    " apple/Sugar apple/Sweetsop	Chempedak Hazelnut	Honeyberries	Dragon fruit	Durian Horned melon	Hog plum	" \
    "Egg fruit	Feijoa/Pineapple guava/Guavasteen Indian fig	Ice apple	Guava	Fuyu Persimmon Jackfruit	Jujube	" \
    "Honeydew melon	Jenipapo Kiwi	Kabosu	Kiwano	Kaffir lime/Makrut lime Lime	Lychee	Longan	Langsat Mango	" \
    "Mulberry	Pear	Lucuma Muskmelon	Naranjilla	Passion fruit	Mangosteen Nectarine	Nance	Quince	Medlar frui"
name = "KASIM HARIKA VENKANNA KASIM BHASKAR VENKANNA CHALAPATHI BHASKAR ' \
       'VIGNESWARA CHALAPATHI RAMA VIGNESWARA TIRUMALA RAMA Durga TIRUMALA ' \
       'DHANA Durga DHANA THAMBI SAMBA THAMBI JAHEER SAMBA SANTHI JAHEER ' \
        'SUDHAKAR SANTHI Sunitha SUDHAKAR SUBBA Sunitha SIVA SUBBA SIVASHANKAR ' \
       'SIVA SUGNATHA SIVASHANKAR SOBANA SUGNATHA SOBANA SIVAYYA RANGA SIVAYYA ' \
       'Rambabu RANGA Rambabu RAMANUJAM RAJU RAMANUJAM MUTTAIAH RAJU PAPARAO MUTTAIAH ' \
       'NARAYANA PAPARAO NAGARAJU NARAYANA LAKSHMI NAGARAJU NAGESWARAO LAKSHMI NAGESWARAO ' \
       'KRISHNA KOTESWARA KRISHNA KOTESWARA JAGADISH SIVA JAGADISH VENKATA SIVA SRINIVASA"
state_names = state_names.replace('  ', '').replace("'", '')
states = [i.strip() for i in state_names.split(", ")]
fruits = fruits.replace('/', ' ').replace("'", '')
list_fruits = [i for i in fruits.split()]
name = name.replace('  ', ' ').replace("'", '')
names = [i for i in name.split()]
def cust_name():
    nm = (random.choice(names)).title()
    return nm
def door_number():
    a = random.randint(1, 30)
    b = random.randint(11, 30)
    c = random.randint(111, 99990)
    dn = str(a)+"-"+str(b)+"-"+str(c)
    return dn
def area():
    ar = ''
    for _ in range(8):
        add_ar = (chr(64+(random.randint(1, 26))))
        ar += add_ar
    return ar.title()
def state():
    st = random.choice(states)
    return st
def pin_number():
    pn = random.randint(510000, 560000)
    return pn
def address():
    dr = door_number()
    ar = area()
    st = state()
    ads = dr+','+ar+','+st
    return ads
def phone_number():
    pn = random.choice(range(6600000000, 9999999999))
    return pn
def price():
    p = random.randrange(2000)
    return p
def quantity():
    q = random.randrange(30)
    return q
def item():
    i = random.choice(list_fruits)
    return i
def invoice_id():
    t_id = random.choice(range(10000000, 99999999))
    return 'invoice_ID '+str(t_id)
def salary():
    sl = (random.randint(1, 100)) * 1000
    return sl
def age():
    ag = random.randint(22, 56)
    return ag
transaction_data = [tuple((("cust_"+str(f"{i:05}")), invoice_id(), item(), price(), quantity())) for i in range(1, number_of_records)]
customer_data = [tuple((("cust_"+str(f"{i:05}")), cust_name(), phone_number(), address(), pin_number())) for i in range(1, number_of_records)]
employee_data = [tuple((("emp_"+str(f"{i:03}")), cust_name(), salary(), age(), phone_number(), address(), pin_number())) for i in range(1, number_of_records)]
transaction_scheema = ['customer_id', 'invoice_ID', 'item_name', 'each_price', 'quantity']
customer_scheema = ['customer_id', 'customer_name', 'phone_number', 'address', 'pin_number']
employee_scheema = ['employee_id', 'employee_name', 'salary', 'age', 'phone_number', 'address', 'pin_number']

transaction_df = spark.createDataFrame(transaction_data, transaction_scheema)
customer_df = spark.createDataFrame(customer_data, customer_scheema)
employee_df = spark.createDataFrame(employee_data, employee_scheema)

customer_df.show()
employee_df.show()
transaction_df.show()

