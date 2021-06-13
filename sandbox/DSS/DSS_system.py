import sys
import datetime
from collections import defaultdict
from datetime import date
import re
from Configs import BASELINE, CROPS
import json
import mysql.connector

current_date = date.today()
query_date = current_date.strftime("%d/%m/%Y")
query = 'select gn.fieldID,humidity,temp,rain,ndvi,msavi,`soil_m_6.9` from gisin gn inner join gisout go on gn.fieldID=go.fieldID limit 1000'


def dss(field_params, baseline_params):

    # for every field check the parameters against baseline:

    for field_id, field_values in field_params.items():

        for crop, crop_values in baseline_params.items():
            crop_selector(baseline_values=crop_values,
                          values=field_values, key=field_id, i=crop)


def crop_selector(baseline_values, values, key, i):
    # test every crop  against the field values
    [test_baseline_parameters(baseline_values, values, key, x)
     for x in CROPS if x == i]
    return


def extract(data):
    data_type = type(data)
    if(data_type == "string"):
        return convert_tuples(data)
    elif(data_type == "dict"):
        return convert_list(data)


def convert_tuples(txt):
    # extract values from string
    list_data = [int(s) for s in re.findall(r'\d+', txt)]
    return tuple(list_data)


def convert_list(items):
    # handle string like dictionary
    temp_items = json.loads(str(items))

    l = []
    for k, v in temp_items.items():
        l.append(v)
    return tuple(l)


def compare_baseline_field_params(l1, l2):

    x = [any(y[0] <= x <= y[1] for y in l2) for x in l1]
    return x


def test_field_suitability(field_id, current_status, crop, parameter, items):
    #val=(field_id, crop, current_status, parameter, current_date)
    db_conn = mysql.connector.connect(
        host="localhost", user="lisa", password="BsiKpt_y78ga", database="dssout")
    db_cursor = db_conn.cursor()
    sql = "INSERT INTO observatory(fieldID, crop,baseline,parameter,comments,createdAt) VALUES (%s,%s,%s,%s,%s,%s)"

    if any(x == True for x in current_status):
        sa = "not suitable"
        query = (field_id, crop, items, str(parameter), "Suitable", query_date)

        db_cursor.execute(sql, query)
        db_conn.commit()
        print("record inserted")
    elif all(x == False for x in current_status):
        query_state = (field_id, crop, items, str(
            parameter), str("Not Suitable"), query_date)

        db_cursor.execute(sql, query_state)
        db_conn.commit()

    return (field_id, "current ", items, parameter, 'not suitable for ', crop)


def insert_dss_out(query):
    dss_db = mysql.connector.connect(
        host="localhost",
        user="lisa",
        password="BsiKpt_y78ga",
        database="dssout"
    )
    cursor = dss_db.cursor()
    try:
        cursor.execute(query)
        dss_db.commit()
        print("record inserted", cursor.lastrowid)
    except:
        dss_db.rollback()
    dss_db.close()


def execute(sqlstatement):
    """ query data from the gisin and gisout table  using table join
        build a dictionary using the data.
    """

    try:

        conn = mysql.connector.connect(
            host="localhost",
            user="zaka",
            password="FreeLance1Z",
            database="gis"
        )
        cursor = conn.cursor()

        cursor.execute(sqlstatement)

        headers = ["fieldID", "humidity", "temp", "rain",
                   "ndvi", "msavi", "soilmoisture"]
        # initialize dict
        data_dict = dict()
        for rows in cursor:

            data_dict.setdefault(rows[0], {}).update(
                {headers[1]: rows[1], headers[2]: rows[2],
                 headers[3]: rows[3], headers[4]: rows[4], headers[5]: rows[5],
                 headers[6]: rows[6]})

        return data_dict
    except (Exception) as error:
        print(error)


def test_baseline_parameters(baseline_values, values, key, i):
    for sa, va in baseline_values.items():
        if(sa == 'soilM'):
            temp_soil = []
            temp_soil.append(convert_tuples(
                values["soilmoisture"]))
            suit1 = compare_baseline_field_params(
                va, temp_soil)
            test_field_suitability(
                field_id=key, current_status=suit1, items="soil",
                crop=i, parameter=temp_soil)

        elif(sa == 'temp'):
            temp_tempreature = []
            temp_tempreature.append(convert_list(
                values["temp"]))
            suit_temp = compare_baseline_field_params(
                va, temp_tempreature)
            test_field_suitability(
                field_id=key, current_status=suit_temp, items="temp",
                crop=i, parameter=temp_tempreature)


def execute_job():
    print("running job")
    # main
    dss(execute(query), BASELINE)
