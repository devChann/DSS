import sys
import datetime
import pyodbc
from collections import defaultdict
from datetime import date
import re
from Configs import Baseline
import json

import mysql.connector
# To implement later:
current_date = date.today()
query_date = current_date.strftime("%d/%m/%Y")
query = 'select gn.fieldID,humidity,temp,rain,ndvi,msavi,`soil_m_6.9` from gisin gn inner join gisout go on gn.fieldID=go.fieldID limit 1000'


def insert_dss_out(*argv):
    dss_db = mysql.connector.connect(
        host="localhost", user="lisa", password="BsiKpt_y78ga")
    cursor = dss_db.cursor()
    try:
        cursor.execute(*argv)
        dss_db.commit()
    except:
        dss_db.rollback()
    dss_db.close()


def execute(sqlstatement):
    """ query data from the gisin and gisout table  using table"""

    try:
        # conn = pyodbc.connect('Driver={SQL Server};'
        #                       'Server=LAPTOP-039SL7RG\SQLEXPRESS;'
        #                       'Database=PMSDB;'
        #                       'Trusted_Connection=yes;')
        conn = mysql.connector.connect(
            host="localhost", user="zaka", password="FreeLance1Z", database="gis")
        cursor = conn.cursor()

        cursor.execute(sqlstatement)
        # conn.commit()
        # headers = [i[0] for i in cursor.description]
        # initialize dict
        data_dict = cursor.fetchall()
        # for rows in cursor:

        #     data_dict.setdefault(rows[0], {}).update(
        #         {headers[1]: rows[1], headers[2]: rows[2],
        #          headers[3]: rows[3], headers[4]: rows[4], headers[5]: rows[5],
        #          headers[6]: rows[6], headers[7]: rows[7]})

        return data_dict
    except (Exception) as error:
        print(error)


def dss(field_params, baseline_params):
    for dic in field_params:
        for key, values in dic.items():
            for i, v in baseline_params.items():
                if i == 'Canola':
                    for param_key, params in v.items():
                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))
                            suit1 = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit1, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)

                elif(i == 'Peas'):
                   # print(i, '->', v)
                    for param_key, params in v.items():

                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))

                            suit_soil = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit_soil, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)
                        # elif(param_key == 'ph'):
                        #     temp_ph = []
                        #     temp_ph.append(convert_tuples(
                        #         values["soilmoisture"]))
                        #     suit_ph = compare_baseline_field_params(
                        #         params, temp_ph)

                        #     print("write db")
                elif(i == 'Lentils'):
                   # print(i, '->', v)
                    for param_key, params in v.items():

                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))
                            suit1 = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit1, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)

                            # elif(param_key == 'ph'):
                            #     temp_ph = []
                            #     temp_ph.append(convert_tuples(
                            #         values["soilmoisture"]))
                            #     suit_ph = compare_baseline_field_params(
                            #         params, temp_ph)
                            #     print("write db")
                elif(i == 'Wheat'):

                    for param_key, params in v.items():

                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))

                            suit1 = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit1, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)

                            # elif(param_key == 'ph'):
                            #     temp_ph = []
                            #     temp_ph.append(convert_tuples(
                            #         values["soilmoisture"]))
                            #     suit_ph = compare_baseline_field_params(
                            #         params, temp_ph)
                            #     print("write db")
                elif(i == 'Flax'):

                    for param_key, params in v.items():

                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))

                            suit1 = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit1, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)

                        # elif(param_key == 'ph'):
                        #     temp_ph = []
                        #     temp_ph.append(convert_tuples(
                        #         values["soilmoisture"]))
                        #     suit_ph = compare_baseline_field_params(
                        #         params, temp_ph)
                        #     print("write db")
                elif(i == 'SoyBeans'):

                    for param_key, params in v.items():

                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))

                            suit1 = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit1, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)

                            # elif(param_key == 'ph'):
                            #     temp_ph = []
                            #     temp_ph.append(convert_tuples(
                            #         values["soilmoisture"]))
                            #     suit_ph = compare_baseline_field_params(
                            #         params, temp_ph)
                            #     print("write db")

                elif(i == 'Fabas_Beans'):

                    for param_key, params in v.items():

                        if(param_key == 'soilM'):
                            temp_soil = []
                            temp_soil.append(convert_tuples(
                                values["soilmoisture"]))

                            suit1 = compare_baseline_field_params(
                                params, temp_soil)
                            test_field_suitability(
                                field_id=key, current_status=suit1, items="soil",
                                crop=i, parameter=temp_soil)
                        elif(param_key == 'temp'):
                            temp_tempreature = []
                            temp_tempreature.append(convert_list(
                                values["temp"]))
                            suit_temp = compare_baseline_field_params(
                                params, temp_tempreature)
                            test_field_suitability(
                                field_id=key, current_status=suit_temp, items="temp",
                                crop=i, parameter=temp_tempreature)

                    # elif(param_key == 'ph'):
                    #     temp_ph = []
                    #     temp_ph.append(convert_tuples(
                    #         values["soilmoisture"]))
                    #     suit_ph = compare_baseline_field_params(
                    #         params, temp_ph)
                    #     print("write db")


def extract(data):
    data_type = type(data)
    if(data_type == "string"):
        return convert_tuples(data)
    elif(data_type == "dict"):
        return convert_list(data)


def convert_tuples(txt):
    list_data = [int(s) for s in re.findall(r'\d+', txt)]
    return tuple(list_data)


def convert_list(items):
    # handle string like dictionary
    temp_items = json.loads(str(items))
    print(temp_items)
    l = []
    for k, v in temp_items.items():
        l.append(v)
    return tuple(l)


def compare_baseline_field_params(l1, l2):
    x = [any(y[0] <= x <= y[1] for y in l2) for x in l1]
    return x


def test_field_suitability(field_id, current_status, crop, parameter, items):
    if any(x == True for x in current_status):
        print(field_id, "current temp ", parameter, "are suitable")
    elif all(x == False for x in current_status):
        val = (field_id, crop, current_status, parameter, current_date)
        insert_dss_out("""INSERT INTO EMPLOYEE(fieldID,crop, comments, parameter, createdAt)
            VALUES (%s, %s, %s,%s, %s)""", val)
        # execute('insert into observatory values(field_id,crop,parameter)')
        print(field_id, "current", " ", parameter, 'not suitable ', crop)
    return (field_id, "current ", items, parameter, 'not suitable for ', crop)


# Implement class later:


dssx = dss(execute(query), Baseline)

# elif(param_key == 'ph'):
#     temp_ph = []
#     temp_ph.append(convert_tuples(
#         values["soilmoisture"]))
#     suit_ph = compare_baseline_field_params(
#         params, temp_ph)

#     print("write db")
