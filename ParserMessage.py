import os
import requests
import time
import json
import logging
import logging.handlers

URL = "http://192.168.20.230"
eventCreateURL = "http://192.168.20.228:5200/messages"
CAN_index = 98
CAN_index_write_value = 93

def parseToBitArray(data):
    
    return [int(i) for i in reversed(list(f'{data:b}'))]

def parseMessageRegisters(data, message_json):
    
    bitParseData = parseToBitArray(data[0])
    msSecond = 0
    second = 0
    for k, elm in enumerate(bitParseData):
        if k < 10: msSecond += 2 ** k * elm
        else: second += 2 ** (k - 10) * elm
    
    try:
        priority = message_json[str(data[2])]['priority']
        isAck = message_json[str(data[2])]['ack']
        isSound = message_json[str(data[2])]['sound']
        if "|" in message_json[f"{data[2]}"]["desc"]:
            if int(data[3]) == -1: newMsg = f'{message_json[f"{data[1]}"]["desc"]}. {message_json[f"{data[2]}"]["desc"].split(" | ")[1]}'
            elif int(data[3]) == 0: newMsg = f'{message_json[f"{data[1]}"]["desc"]}. {message_json[f"{data[2]}"]["desc"].split(" | ")[0]}'
        else: newMsg = f'{message_json[f"{data[1]}"]["desc"]}. {message_json[f"{data[2]}"]["desc"]}'
        
    except:
        newMsg,priority,isAck,isSound = f'ОШИБКА! Сообщение вне известных диапазонов {data[1]}:{data[2]}!',3,0,0
    
    return second, msSecond, newMsg, priority, isAck, isSound

def parseTime(DayHourMinute, YearMonth):

    bitParseData = parseToBitArray(YearMonth)
    month, year = 0, 0
    
    for k, elm in enumerate(bitParseData):
        if k < 4: month += 2 ** k * elm
        else:
            if k < 15: year += 2 ** (k - 4) * elm
    
    bitParseData = parseToBitArray(DayHourMinute)
    minute, hour, day = 0, 0, 0
    
    for k, elm in enumerate(bitParseData):
        if k < 6: minute += 2 ** k * elm
        else:
            if k < 11: hour += 2 ** (k - 6) * elm
            else: day += 2 ** (k - 11) * elm

    return year, month, day, hour, minute

def createMessage():
    
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "description_messages.json"), encoding='utf-8') as f:
        OP_MSG = json.load(f)
    
    while True:
        time_start = time.time()
        try:
            try:
                time_read = time.time()
                data_message = []
                count = 0
                comand = "/get_od_data.form?"
                for i in range(CAN_index+1):
                    if count == 20 or i == CAN_index:
                        data = requests.post(URL + comand[:-1]).text
                        [data_message.append(i) for i in data[:-3].split("&#&")]
                        count = 0
                        comand = "/get_od_data.form?"
                    comand += f"0xAB01_{i}&" 
                    count += 1
                print(f"Time read: {(time.time()-time_read)}")
            except Exception as e: logger.error(f"Error read Tag: {e}")

            ptr1 = int(data_message[92])
            ptrKvt = int(data_message[93])
            if ptr1 != ptrKvt or int(data_message[95]) > 0:
                timePLC = parseTime(int(data_message[2]), int(data_message[3]))
                count_message = int(data_message[1])
                
                data_message = data_message[4:92]
                data_message = [data_message[4*i: 4*(i+1)] for i in range(count_message)]

                for element in data_message:

                    element = [int(element[0]), int(element[1]), int(element[2]), float(element[3])]
                    message_info = parseMessageRegisters(element, OP_MSG)
                    
                    timeStamp = f'{str(timePLC[0])}-{str(timePLC[1]).zfill(2)}-{str(timePLC[2]).zfill(2)}T'+\
                                f'{str(timePLC[3]).zfill(2)}:{str(timePLC[4]).zfill(2)}:{str(message_info[0]).zfill(2)}.{str(message_info[0]).zfill(3)}Z'

                    json_eventCreate = {
                        "message": message_info[2],
                        "source": f"line{element[1]}",
                        "severity": message_info[3]
                            }
                    
                    time_send = time.time()
                    try:
                        
                        eventCreate = requests.post(eventCreateURL, json=json_eventCreate)
                        eventCreate.raise_for_status()
                    
                    except Exception as e: logger.error(f"Error send output json: {e}")
                    print(f"Time send: {(time.time()-time_send)}")
                try:
                    request  = requests.post(URL +f'/set_od_data.form?0xAB01_{CAN_index_write_value}={ptr1}')
                    request.raise_for_status()
                        
                except Exception as e: logger.error(f"Error send update tag: {e}")
                time.sleep(0.1)
         
        except Exception as e: logger.error(f"Error main cycle: {e}")
        print(f"Time cycle: {(time.time()-time_start)}")

def startScript():
    
    global logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s >> %(filename)10s] %(levelname)s:  %(message)s')
    logHandler = logging.handlers.TimedRotatingFileHandler(f'{os.path.dirname(__file__)}/log/logging.log', when='midnight', backupCount=7)
    logHandler.setLevel(logging.INFO)
    logHandler.setFormatter(formatter)

    logger.addHandler(logHandler)
    
    try:
        createMessage()
    except Exception as e: logger.error(f"Error main function: {e}")

startScript()
