import asyncio
import create_table as ct
import csv
from datetime import datetime
import json
import numpy as np
import signal
import socket
import time

from async_timeout import timeout
from interval import Interval
from aiosched import scheduler

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import metpy.calc as mpcalc
from metpy.units import units

### Setup status variable
### 0 = Initialize
### 1 = Normal
### 2 = Database Connection Error
### 3 = Database Error
### 11 = Device Connection Error
### 12 = Device Data Error
### 21 = Sensor Node Error
### 22 = Sensor Node Error
status = 0
DN = 4

Sample_Count = 30
Sample_Period = 30
Sampling_Flag = True
Query_Flag = False
Query_Period = 300 # 5 min
Do_Flag = True
FLAG = True
FIRST = True
device_port = 8899

device_WritingStr = ''
device_WritingStr_Soil = []

job1 = object()
job2 = object()

# Query_Data_Flag = [0,0,0,0,0]
Query_Data_Flag = [0,0,0,0]

device_info = {'dev1':{'IP':'IP1'},
               'dev2':{'IP':'IP2'},
               'dev3':{'IP':'IP3'},
               'dev4':{'IP':'IP4'},
               'dev5':{'IP':'IP5'}}

device_status = {'dev1':{'status':'','description':''},
                 'dev2':{'status':'','description':''},
                 'dev3':{'status':'','description':''},
                 'dev4':{'status':'','description':''},
                 'dev5':{'status':'','description':''}}

device_data = {'dev1':{'Temperature':0,'Humidity':0},
               'dev2':{'Temperature':0,'Humidity':0},
               'dev3':{'Temperature':0,'Humidity':0},
               'dev4':{'Temperature':0,'Humidity':0},
               'dev5':{'Temperature':0,'Humidity':0}}

device_data_raw = {'dev1':{'Temperature':0,'Humidity':0},
                   'dev2':{'Temperature':0,'Humidity':0},
                   'dev3':{'Temperature':0,'Humidity':0},
                   'dev4':{'Temperature':0,'Humidity':0},
                   'dev5':{'Temperature':0,'Humidity':0}}

#device_data_offset = {'dev1':{'Temperature':0.2,'Humidity':13.3},
#                      'dev2':{'Temperature':-2.7,'Humidity':8.0},
#                      'dev3':{'Temperature':-2.7,'Humidity':8.3},
#                      'dev4':{'Temperature':-2.7,'Humidity':8.5},
#                      'dev5':{'Temperature':-2.7,'Humidity':7.3}}

device_data_offset = {'dev1':{'Temperature':3.0,'Humidity':-5.0},
                      'dev2':{'Temperature':0.0,'Humidity':0.0},
                      'dev3':{'Temperature':0.0,'Humidity':0.0},
                      'dev4':{'Temperature':0.0,'Humidity':0.0},
                      'dev5':{'Temperature':0.0,'Humidity':0.0}}


device_info_soil = {'dev6':{'IP':'IP6'}}

device_status_soil = {'sdev1':{'status':'','description':''},
                      'sdev2':{'status':'','description':''}}

device_data_soil = {'sdev1':{'Temperature':0, 'Humidity':0,
                            'PH':0, 'EC':0},
                    'sdev2':{'Temperature':0, 'Humidity':0,
                            'PH':0, 'EC':0}}

device_data_soil_raw = {'sdev1':{'Temperature':0, 'Humidity':0,
                                'PH':0, 'EC':0},
                        'sdev2':{'Temperature':0, 'Humidity':0,
                                'PH':0, 'EC':0}}

device_data_soil_havg = {'sdev1':{'PH':0, 'EC':0},
                         'sdev2':{'PH':0, 'EC':0}}

device_data_soil_temp = [[[],[]],[[],[]]]
device_data_soil_avg = [[0,0],[0,0]]
device_data_soil_past = [[[0,0],[0,0]],[[0,0],[0,0]]]

###### CRC16 Calculation Function 
def crc16(data):
    regCRC = 0xFFFF
    data = list(data)
    for i in range(0, int(len(data) / 2)):
        buff = int(data[2 * i], 16) << 4
        buff |= int(data[2 * i + 1], 16)
        regCRC = regCRC ^ buff
        for j in range(0, 8):
            if regCRC & 0x01:
                regCRC = (regCRC >> 1) ^ 0xA001
            else:
                regCRC = regCRC >> 1

    crc_int = ((regCRC & 0xFF00) >> 8) | ((regCRC & 0x0FF) << 8)
    crc_str = bytes([int(crc_int / 256)]) + bytes([crc_int % 256])
    return crc_str

### ### Initialize Device Sending String Function
def WritingStr():
    global device_WritingStr
    global device_WritingStr_Soil
    id_hex_TH = bytes([1])
    read_TH = id_hex_TH + b'\x03\x00\x00\x00\x02'
    read_TH += crc16(read_TH.hex())
    device_WritingStr = read_TH

    for i in range(1,3):
        id_hex_SL = bytes([i+1])
        read_SL = id_hex_SL + b'\x03\x00\x00\x00\x08'
        read_SL += crc16(read_SL.hex())
        device_WritingStr_Soil.append(read_SL)
    print(device_WritingStr_Soil)

###### TH Getting Device Data Function
async def Get_Device_Data(dev_num, host):
    global device_WritingStr
    global device_status
    global Query_Data_Flag
    dev_id = str(dev_num)[3]
    try:
        reader, writer = await asyncio.open_connection(host, 8899)
        
        writer.write(device_WritingStr)
        await writer.drain()

        async with timeout(10) as cm:
            try:
                data = await reader.read(1024)
            except Exception as e:
                print(e)
        
        if cm.expired:
            device_status[dev_num]['status']=22
            device_status[dev_num]['description']=str('Sensor Node Loss!!')
            Save_Err_Log(dev_num,22)
            await Put_Zero(dev_num)
            writer.close()
            await writer.wait_closed()
            Query_Data_Flag[int(dev_id)-1] = 1
        else:

            ret = bytearray(data)

            if len(ret)>=9:
                    
                TP_value = int.from_bytes(bytes([ret[5]]) + bytes([ret[6]]), 'big')
                TP_value = float(TP_value / 10)

                HD_value = int.from_bytes(bytes([ret[3]]) + bytes([ret[4]]), 'big')
                HD_value = float(HD_value / 10)

                await Put_Data(dev_num, TP_value, HD_value)
                            
                # print('Now Temperature=',str(TP_value),'°C')
                # print('Now Humidity',str(HD_value),'%')

                # writer.close()
                # await writer.wait_closed()
                device_status[dev_num]['status']=1
                device_status[dev_num]['description']=str('Connection Success !!')
                
                Query_Data_Flag[int(dev_id)-1] = 1
            
            else:
                device_status[dev_num]['status'] = 12
                device_status[dev_num]['description']=str('Data Error')
                Save_Err_Log(dev_num,12)
                await Put_Zero(dev_num)
                Query_Data_Flag[int(dev_id)-1] = 1
        
        writer.close()
        await writer.wait_closed()

    except OSError as err: 
        device_status[dev_num]['status'] = 11
        device_status[dev_num]['description']=str(err)
        Save_Err_Log(dev_num,11)
        await Put_Zero(dev_num)
        Query_Data_Flag[int(dev_id)-1] = 1
        writer.close()
        await writer.wait_closed()

async def Put_Zero(dev_id):
    global device_data, device_data_raw
    device_data[dev_id]['Temperature'] = 0
    device_data[dev_id]['Humidity'] = 0
    device_data_raw[dev_id]['Temperature'] = 0
    device_data_raw[dev_id]['Humidity'] = 0


###### TH Put Data Function 
async def Put_Data(dev_id, T, H):
    global device_data, device_data_raw, device_data_offset
    global Query_Data_Flag
    global DN
    device_data_raw[dev_id]['Temperature'] = T
    device_data_raw[dev_id]['Humidity'] = H
    await Alignment_Data(dev_id, T, H)
    if all(Query_Data_Flag):
        Save_as_CSV('TH')
        time.sleep(0.5)
        Save_as_SQL('TH')
        Query_Data_Flag = [0]*DN
        print('#'*25)
        print(time.ctime(),'Query TH Data')
        print(device_data)
        print(device_data_raw)
    else: 
        pass


###### TH Data Alignment Function 
async def Alignment_Data(dev_id, T, H):
    global device_data, device_data_raw, device_data_offset
    device_data[dev_id]['Temperature'] =round(T + device_data_offset[dev_id]['Temperature'], 2)
    device_data[dev_id]['Humidity'] = round(H + device_data_offset[dev_id]['Humidity'], 2)


###### Soil Getting Device Data Function
async def Get_Device_Data_Soil(**kwargs):
    global device_WritingStr_Soil
    global device_status_soil
    global device_status_soil
    for k, v in kwargs.items():
        if k == 'ip':
            host = v
        elif k == 'tp':
            types = v
    temp_data = []
    try:
        reader, writer = await asyncio.open_connection(host, 8899)

        for i in range(0,2):
            writer.write(device_WritingStr_Soil[i])
            await writer.drain()

            async with timeout(8) as cm:
                try:
                    data = await reader.read(1024)
                except Exception as e:
                    print(e)
            
            if cm.expired:
                device_status_soil[f'sdev{i+1}']['status']=22
                device_status_soil[f'sdev{i+1}']['description']=str('Sensor Node Loss!!')
                temp_data.append([0, 0, 0, 0])
                Save_Err_Log(f'sdev{i+1}',22)
                writer.close()
                await writer.wait_closed()
            else:

                rets = bytearray(data)

                if len(rets)>=21:        
                    ST_value = int.from_bytes(bytes([rets[7]]) + bytes([rets[8]]), 'big')
                    ST_value = float(ST_value / 10)

                    SH_value = int.from_bytes(bytes([rets[9]]) + bytes([rets[10]]), 'big')
                    SH_value = float(SH_value / 10)

                    SP_value = int.from_bytes(bytes([rets[15]]) + bytes([rets[16]]), 'big')
                    SP_value = float(SP_value / 10)

                    SE_value = int.from_bytes(bytes([rets[17]]) + bytes([rets[18]]), 'big')
                    SE_value = float(SE_value / 10)

                    temp_data.append([ST_value, SH_value, SP_value, SE_value])

                    device_status_soil[f'sdev{i+1}']['status']=1
                    device_status_soil[f'sdev{i+1}']['description']=str('Connection Success !!')
                
                else:
                    
                    device_status_soil[f'sdev{i}']['status']=22
                    device_status_soil[f'sdev{i}']['description']=str(f'sdev{i} Data Error')
                    Save_Err_Log(f'sdev{ij}',22)
        
                    temp_data.append([0, 0, 0, 0])

        
        writer.close()
        await writer.wait_closed()
        print(temp_data)

        # # for data average use
        # if types == 'A':
        #     # print('SAMPLING')
        #     await Sample_Average(temp_data)
        # # for sampling data and alignment
        # elif types == 'S':
        #     # print('QUERY')
        #     Put_Data_Soil(temp_data)
        
    except OSError as err: 
        for j in range(1,3):
            device_status_soil[f'sdev{j}']['status']=21
            device_status_soil[f'sdev{j}']['description']=str(err)
            Save_Err_Log(f'sdev{j}',21)
        
        temp_data.append([0, 0, 0, 0])

    # for data average use
    if types == 'A':
        # print('SAMPLING')
        await Sample_Average(temp_data)
    # for sampling data and alignment
    elif types == 'S':
        # print('QUERY')
        Put_Data_Soil(temp_data)

###### Soil Data Initial Function
async def Sample_Average(soil_data):
    global device_data_soil_temp
    global device_data_soil_avg
    global device_info_soil
    global Sample_Count 
    global Sampling_Flag
    global Query_Flag
    global Query_Period
    global job1, job2

    if Sample_Count == 0:
        for i in range(len(soil_data)):
            device_data_soil_avg[i][0] = round(np.mean(np.array(device_data_soil_temp[i][0])),2)
            device_data_soil_avg[i][1] = round(np.mean(np.array(device_data_soil_temp[i][1])),2)
        
        Sampling_Flag = False
        Query_Flag = True
        job1.cancel()
        print('job1 Cancel')
        job2 = await scheduler.create(target=Get_Device_Data_Soil, kwargs={'ip':device_info_soil['dev6']['IP'], 'tp':'S'}, interval=Query_Period)
        print(device_data_soil_avg)
    else:
        for j in range(len(soil_data)):
            device_data_soil_temp[j][0].append(soil_data[j][2])
            device_data_soil_temp[j][1].append(soil_data[j][3])
        Sample_Count-=1
    
    print('#'*25)
    print(time.ctime(),'Sampling Soil Data')
    print('Sample_Count_S:',Sample_Count)
    print(device_data_soil_temp)
    
    

###### Soil Put Data Function
def Put_Data_Soil(soil_data):
    global device_data_soil
    global device_data_soil_raw
    global device_data_soil_past
    for i in range(len(soil_data)):
        device_data_soil_raw[f'sdev{i+1}']['Temperature'] = soil_data[i][0]
        device_data_soil_raw[f'sdev{i+1}']['Humidity'] = soil_data[i][1]
        device_data_soil_raw[f'sdev{i+1}']['PH'] = soil_data[i][2]
        device_data_soil_raw[f'sdev{i+1}']['EC'] = soil_data[i][3]
    
    print(soil_data)
    ali_data = Alignment_Data_Soil(soil_data)

    for i in range(len(soil_data)):
        device_data_soil[f'sdev{i+1}']['Temperature'] = ali_data[i][0]
        device_data_soil[f'sdev{i+1}']['Humidity'] = ali_data[i][1]
        device_data_soil[f'sdev{i+1}']['PH'] = ali_data[i][2]
        device_data_soil[f'sdev{i+1}']['EC'] = ali_data[i][3]
    print(ali_data)

    # save to csv and SQL
    Hour_Average(soil_data)

###### Soil Data Alignment Function 
def Alignment_Data_Soil(soil_data):
    global device_data_soil
    global device_data_soil_past
    global device_data_soil_avg
    global Sample_Count
    
    # device_data_soil_past
    # =>[[[PH_RAW,PH_ALI],[EC_RAW,EC_ALI]],[[PH_RAW,PH_ALI],[EC_RAW,EC_ALI]]]
    A1 = Interval(1.1, 1.2, upper_closed=False)
    A2 = Interval(0.8, 0.9, lower_closed=False)

    B1= Interval(1.2, 1.3, upper_closed=False)
    B2= Interval(0.7, 0.8, lower_closed=False)

    C1= Interval(1.3, 1.4, upper_closed=False)
    C2= Interval(0.6, 0.7, lower_closed=False)

    D1= Interval(1.4, 1.5, upper_closed=False)
    D2= Interval(0.5, 0.6, lower_closed=False)
    ALI = []
    ali = 0

    for j in range(len(soil_data)):
        ALI.append(soil_data[j][0:2])
        for k in range(0,2):
            FG = True

            raw = soil_data[j][k+2]
            avg = device_data_soil_avg[j][k]
            
            if Sample_Count == 0:
                if k == 0 and raw > 10:
                    ali = avg
                    device_data_soil_past[j][k][0] = raw
                    device_data_soil_past[j][k][1] = ali
                    FG = False
                elif k == 1 and raw > 12:
                    ali = avg
                    device_data_soil_past[j][k][0] = raw
                    device_data_soil_past[j][k][1] = ali
                    FG = False
                if FG:
                    if raw / avg >= 1.5 or raw / avg < 0.5:
                        ali = avg*0.9 + raw*0.1
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('C1')
                    elif raw / avg in D1 or raw / avg in D2:
                        ali = avg*0.8 + raw*0.2
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('D1')
                    elif raw / avg in C1 or raw / avg in C2:
                        ali = avg*0.7 + raw*0.3
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('E1')
                    elif raw / avg in B1 or raw / avg in B2:
                        ali = avg*0.6 + raw*0.4
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('F1')
                    elif raw / avg in A1 or raw / avg in A2:
                        ali = avg*0.5 + raw*0.5
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('G1')
                    else:
                        ali = raw
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('H1')

                ALI[j].append(round(ali,2))

            else:
                # past1 => past alignment data
                # past2 => past raw data
                past1 = device_data_soil_past[j][k][1]
                past2 = device_data_soil_past[j][k][0]
                past3 = avg

                if k == 0 and raw not in Interval(3, 10):
                    ali = avg
                    device_data_soil_past[j][k][0] = raw
                    device_data_soil_past[j][k][1] = avg
                    FG = False
                    # print('A2')
                    
                elif k == 0 and raw in Interval(3, 10) and raw in Interval(past2-0.8, past2+0.8):
                    ali = raw
                    device_data_soil_past[j][k][0] = raw
                    device_data_soil_past[j][k][1] = ali
                    FG = False
                    # print('B2')

                elif k == 1 and raw not in Interval(0.2, 12):
                    ali = avg
                    device_data_soil_past[j][k][0] = raw
                    device_data_soil_past[j][k][1] = ali
                    FG = False
                    # print('C2')

                elif k == 1 and raw in Interval(0.2, 12) and raw in Interval(past2-0.8, past2+0.8):
                    ali = raw
                    device_data_soil_past[j][k][0] = raw
                    device_data_soil_past[j][k][1] = ali
                    FG = False
                    # print('D2')      

                if FG:
                    if raw / past1 >= 1.5 or raw / past1 < 0.5:
                        ali = past3*0.9 + raw*0.1
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('E2')
                    elif raw / past1 in D1 or raw / past1 in D2:
                        ali = past3*0.8 + raw*0.2
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('F2')
                    elif raw / past1 in C1 or raw / past1 in C2:
                        ali = past3*0.7 + raw*0.3
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('G2')
                    elif raw / past1 in B1 or raw / past1 in B2:
                        ali = past3*0.6 + raw*0.4
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('H2')
                    elif raw / past1 in A1 or raw / past1 in A2:
                        ali = past3*0.5 + raw*0.5
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = round(ali,2)
                        # print('I2')
                    else:
                        ali = raw
                        device_data_soil_past[j][k][0] = raw
                        device_data_soil_past[j][k][1] = ali
                        # print('J2')

                ALI[j].append(round(ali,2))

    return ALI

###### Soil Data Hour Average Function 
def Hour_Average(soil_data):
    global device_data_soil_temp
    global device_data_soil_avg
    global device_data_soil_havg
    global Sample_Count 
    
    for k in range(len(soil_data)):
        device_data_soil_temp[k][0].append(soil_data[k][2])
        device_data_soil_temp[k][1].append(soil_data[k][3])
        if Sample_Count < 12 :       
            device_data_soil_avg[k][0] = round(np.mean(np.array(device_data_soil_temp[k][0])),2)
            device_data_soil_avg[k][1] = round(np.mean(np.array(device_data_soil_temp[k][1])),2)
            device_data_soil_havg[f'sdev{k+1}']['PH'] = device_data_soil_avg[k][0]
            device_data_soil_havg[f'sdev{k+1}']['EC'] = device_data_soil_avg[k][1]

        elif Sample_Count == 12:
            device_data_soil_avg[k][0] = round(np.mean(np.array(device_data_soil_temp[k][0])),2)
            device_data_soil_avg[k][1] = round(np.mean(np.array(device_data_soil_temp[k][1])),2)
            del device_data_soil_temp[k][0][:len(device_data_soil_temp[k][0])-12]
            del device_data_soil_temp[k][1][:len(device_data_soil_temp[k][1])-12]
            device_data_soil_havg[f'sdev{k+1}']['PH'] = device_data_soil_avg[k][0]
            device_data_soil_havg[f'sdev{k+1}']['EC'] = device_data_soil_avg[k][1]

        elif Sample_Count > 12:
            device_data_soil_temp[k][0].pop(0)
            device_data_soil_temp[k][1].pop(0)
            device_data_soil_avg[k][0] = round(np.mean(np.array(device_data_soil_temp[k][0])),2)
            device_data_soil_havg[f'sdev{k+1}']['PH'] = device_data_soil_avg[k][0]
            device_data_soil_avg[k][1] = round(np.mean(np.array(device_data_soil_temp[k][1])),2)
            device_data_soil_havg[f'sdev{k+1}']['EC'] = device_data_soil_avg[k][1]
        if k == 1  and Sample_Count <= 12:
            Sample_Count += 1
        else:
            pass
    
    # save to csv and SQL
    print('#'*25)
    print(time.ctime(),'Soil Hour AVG ')
    print('DEV Hour AVG:',device_data_soil_havg)
    Save_as_CSV('Soil')
    time.sleep(0.5)
    Save_as_SQL('Soil')


###### Data To SQL Function 
def Save_as_SQL(datatype):
    global device_data, device_data_raw, device_status
    global device_data_soil, device_data_soil_raw, device_status_soil
    global device_data_soil_havg
    Base = declarative_base()
    DB_IP_Info = ['IP_A','IP_B','IP_C']
    DB_Port_Info = ['Port_A','Port_B','Port_C']
    user = ['User_A','User_B','User_C']
    pwd = ['PWD_A','PWD_B','PWD_C']
    for i in range(3):
        ci = ct.connection_info(user[i],pwd[i],DB_IP_Info[i],DB_Port_Info[i],'Database_Name',Base)
        eg =ci.createengin()
        try:
            Session = sessionmaker(bind=eg)
            session = Session()
        except Exception as e:
            Save_Err_Log(Target='SQL', Status=2,)
            print(e)
        
        if datatype == 'TH':
            objA_TH = ct.THDATA(status=json.dumps(device_status), info = json.dumps(device_data))
            objR_TH = ct.THDATA_RAW(info = json.dumps(device_data_raw))
            session.add(objA_TH)
            session.add(objR_TH)
        elif datatype == 'Soil':
            objA_Soil = ct.SDDATA(status=json.dumps(device_status_soil), info = json.dumps(device_data_soil))
            objR_Soil = ct.SDDATA_RAW(info = json.dumps(device_data_soil_raw))
            objV_Soil = ct.SDDATA_AVG(info = json.dumps(device_data_soil_havg))
            session.add(objA_Soil)
            session.add(objR_Soil)
            session.add(objV_Soil)
        
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            Save_Err_Log(Target='SQL', Status=3,)
            print(e)

### ### Save Data as CSV File function 
def Save_as_CSV(datatype):
    global device_data, device_data_raw
    global device_data_soil, device_data_soil_raw
    global device_data_soil_havg
    filename =  str(datetime.now().strftime("%Y_%m_%d"))
    now = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    if datatype == 'TH':
        with open(f'./Data/TH_{filename}.csv', mode='a') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([device_data,now])
        
        with open(f'./Data/TH_RAW_{filename}.csv', mode='a') as csv_file_raw:
            writer = csv.writer(csv_file_raw,)
            writer.writerow([device_data_raw,now])

    elif datatype == 'Soil':

        with open(f'./Data/Soil_{filename}.csv', mode='a') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([device_data_soil,now])
            
        with open(f'./Data/Soil_RAW_{filename}.csv', mode='a') as csv_file_raw:
            writer = csv.writer(csv_file_raw)
            writer.writerow([device_data_soil,now])

        with open(f'./Data/Soil_RAW_{filename}.csv', mode='a') as csv_file_avg:
            writer = csv.writer(csv_file_avg)
            writer.writerow([device_data_soil_havg,now])

###### Save Error Log function 
def Save_Err_Log(Target='System', Status=None,):
    global status
    if Status ==None:
        Status = status
    
    Filename =  str(datetime.now().strftime("%Y_%m_%d"))
    Now = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    
    with open(f'./Log/{Filename}.txt', mode='a') as f:
        f.write(f'Time:{Now}, Target:{Target}, Status:{Status}\r\n')


async def event_lis():
    signal.signal(signal.SIGINT,exits)
    signal.signal(signal.SIGTERM,exits)

def exits(signum, frame):
    global FLAG
    global job1, job2
    global job10
    global Sampling_Flag
    global DN
    FLAG = False

    if Sampling_Flag:
        job1.cancel()
        job10.cancel()
        for k in range(DN):
            globals() [f'job{k+3}'].cancel()
    else:
        job2.cancel()
        job10.cancel()
        for k in range(DN):
            globals() [f'job{k+3}'].cancel()

    for task in asyncio.Task.all_tasks():
        task.cancel()
    print('Process Stopping ......')
    asyncio.get_event_loop().stop()
    asyncio.ensure_future(exit()) 

def main():
    global device_info_soil
    global Sampling_Flag, Query_Flag
    global Sample_Period
    global Query_Period
    global FIRST
    global job1, job2
    global DN

    globals()['loop'] = asyncio.get_event_loop()
    # scheduler.start(loop=loop)
    if FIRST:
         WritingStr()
        #  Run()
         FIRST = False
    job1 = scheduler.create_threadsafe(target=Get_Device_Data_Soil, kwargs={'ip':device_info_soil['dev6']['IP'], 'tp':'A'}, interval=Sample_Period)
    for k in range(DN):
        print(f'job{k}')
        globals() [f'job{k+3}'] = scheduler.create_threadsafe(target=Get_Device_Data, args=(f'dev{k+1}',device_info[f'dev{k+1}']['IP']),interval=Query_Period+30)
    
    globals()['job10'] = scheduler.create_threadsafe(target=event_lis, interval=5)
    # loop.run_until_complete(asyncio.ensure_future(event_lis()))
    loop.run_until_complete(scheduler.scheduler_loop())

    # loop.run_forever()
    time.sleep(0.5)


while FLAG:
    if Do_Flag:
        main()
        Do_Flag = False
    else:
        pass
