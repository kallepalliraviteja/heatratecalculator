import json
import base64
import requests
from lxml import html
import datetime
import mysql.connector
from iapws import IAPWS95
from threading import Timer


with open('config.json') as configfile:
    configuration=json.load(configfile)
mydb = mysql.connector.connect(
  host=configuration["DB_Host"],
  user=configuration["DB_UserName"],
  password=configuration["DB_Password"],
  database=configuration["DB_Schema"]
)
def kgpercms_2_MPa(pressure):
    return float(pressure)*0.0980665
def degcentigrate_2_kelvin(temp):
    return float(temp)+273.15
def kjperkg_2_kcalperkg(heatrate):
    return 0.239006*float(heatrate)
def insertToDB(unit,actualRecords,calculatedRecords):
    #print(actualRecords.values())
    cursor=mydb.cursor()
    placeholders = ', '.join(['%s'] * len(actualRecords))
    columns = ', '.join(actualRecords.keys())
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (unit+"_actuals", columns, placeholders)
    cursor.execute(sql, list(actualRecords.values()))
    mydb.commit()
    print(cursor.lastrowid)
    calculatedRecords["actual_id"]=str(cursor.lastrowid)
    placeholders = ', '.join(['%s'] * len(calculatedRecords))
    columns = ', '.join(calculatedRecords.keys())
    sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (unit+"_enthalphy_hr", columns, placeholders)
    cursor.execute(sql, list(calculatedRecords.values()))
    mydb.commit()

def extractHeatRate():
    timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Fetching data from recorder:"+timestamp)
    for recorder in configuration['Recorders']:
    #Requests library provides handy tool to do a basic authentication using auth variable as shown below
    #Here the username and password are encoded to base64 and are provided to the server.
        page=requests.get(recorder['Schema']+recorder['RecorderIp']+recorder['RecorderWebURL'],auth=(recorder['UserName'],recorder['Password']))
        data=html.fromstring(page.content)
        datanodes=data.xpath('//table//tr//td//table//tr//td[3]/text()')
        recorderParams=["MAIN_STEAM_FLOW","LOAD_MW","MAIN_STM_PR","MAIN_STEAM_TEMP","FW_PR_AT_ECOIL","FW_TMP_AT_ECOIL","HRH_PRESSURE","HRH_TEMPERATURE","CRH_PRESSURE","CRH_TEMPERATURE","REHEAT_SPRAY","FEED_WTR_FLOW","EXT_PR_HPH6_IL","EXT_TEMP_HPH6_IL","HPH6_DROP_TEMP","BFP_DISCH_HDRPR","FW_TMP_HPH6_IL","FW_TMP_HPH6_OL","Per_DM_MAKEUP","SH_SPRAY","ID_TIMESTAMP"]
        calculateParams=["MS_ENTHALPHY","FeedWater_ENTHALPHY","HRH_ENTHALPHY","CRH_ENTHALPHY","ExtractionSteam_ENTHALPHY","HPH6_Drain_ENTHALPHY","HPH6_INLET_ENTHALPHY","HPH6_OUTLET_ENTHALPHY","Flow_of_Extraction_Steam_To_HPH6","HRH_Steam_Flow","Heat_Input_To_Turbine_Cycle","Turbine_Heat_Rate","actual_id"]
        dbActualRow={}
        dbCalculatedRow={}
        i=0
        for data in datanodes:
            dbActualRow[recorderParams[i]]=str(data)
            i=i+1
            if(i>19):
                break
        dbActualRow[recorderParams[20]]=timestamp
        #Enthalphy of Mainsteam to Turbine
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["MAIN_STM_PR"]), T=degcentigrate_2_kelvin(dbActualRow["MAIN_STEAM_TEMP"]))
        dbCalculatedRow[calculateParams[0]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of Feed Water of Economiser Inlet
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["FW_PR_AT_ECOIL"]), T=degcentigrate_2_kelvin(dbActualRow["FW_TMP_AT_ECOIL"]))
        dbCalculatedRow[calculateParams[1]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of HRH Steam 
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["HRH_PRESSURE"]), T=degcentigrate_2_kelvin(dbActualRow["HRH_TEMPERATURE"]))
        dbCalculatedRow[calculateParams[2]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of CRH Steam 
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["CRH_PRESSURE"]), T=degcentigrate_2_kelvin(dbActualRow["CRH_TEMPERATURE"]))
        dbCalculatedRow[calculateParams[3]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of Extraction Steam to HPH6
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["EXT_PR_HPH6_IL"]), T=degcentigrate_2_kelvin(dbActualRow["EXT_TEMP_HPH6_IL"]))
        dbCalculatedRow[calculateParams[4]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of HPH6 Drain
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["EXT_PR_HPH6_IL"]), T=degcentigrate_2_kelvin(dbActualRow["HPH6_DROP_TEMP"]))
        dbCalculatedRow[calculateParams[5]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of Feed Water to HPH6 Inlet
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["BFP_DISCH_HDRPR"]), T=degcentigrate_2_kelvin(dbActualRow["FW_TMP_HPH6_IL"]))
        dbCalculatedRow[calculateParams[6]]=kjperkg_2_kcalperkg(steam.h)
        #Enthalpy of Feed Water to HPH6 Outlet
        steam=IAPWS95(P=kgpercms_2_MPa(dbActualRow["BFP_DISCH_HDRPR"]), T=degcentigrate_2_kelvin(dbActualRow["FW_TMP_HPH6_OL"]))
        dbCalculatedRow[calculateParams[7]]=kjperkg_2_kcalperkg(steam.h)
        #Flow of Extraction Steam To HPH6
        Flow_of_Extraction_Steam_To_HPH6=(float(dbActualRow["FEED_WTR_FLOW"])*(float(dbCalculatedRow["HPH6_OUTLET_ENTHALPHY"])-float(dbCalculatedRow["HPH6_INLET_ENTHALPHY"]))/(float(dbCalculatedRow["ExtractionSteam_ENTHALPHY"])-float(dbCalculatedRow["HPH6_Drain_ENTHALPHY"])))
        #HRH Steam Flow
        HRH_Steam_Flow=(float(dbActualRow["MAIN_STEAM_FLOW"])-Flow_of_Extraction_Steam_To_HPH6+float(dbActualRow["REHEAT_SPRAY"]))
        #Heat Input To Turbine Cycle
        Heat_Input_To_Turbine_Cycle=(float(dbActualRow["MAIN_STEAM_FLOW"])*(float(dbCalculatedRow["MS_ENTHALPHY"])-float(dbCalculatedRow["FeedWater_ENTHALPHY"]))*1000)+(HRH_Steam_Flow*(float(dbCalculatedRow["HRH_ENTHALPHY"])-float(dbCalculatedRow["CRH_ENTHALPHY"]))*1000)
        #Turbine Heat Rate
        Turbine_Heat_Rate=(Heat_Input_To_Turbine_Cycle/(float(dbActualRow["LOAD_MW"])*1000))
        dbCalculatedRow[calculateParams[8]]=Flow_of_Extraction_Steam_To_HPH6
        dbCalculatedRow[calculateParams[9]]=HRH_Steam_Flow
        dbCalculatedRow[calculateParams[10]]=Heat_Input_To_Turbine_Cycle
        dbCalculatedRow[calculateParams[11]]=Turbine_Heat_Rate
        dbCalculatedRow["actual_id"]=str(0)
        insertToDB(recorder["Name"],dbActualRow,dbCalculatedRow)
        #print(dbActualRow)
        #print(dbCalculatedRow)
t = Timer(180.0, extractHeatRate)
t.start()