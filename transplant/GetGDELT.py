"""
    get GDELT data for GLEAN model, structure as data = [[day1], [day2]]
    day1 = [[Actor1Name, EventCode, Actor2Name, AllNames], ...]
"""

"""
获取某一段时间内的训练数据
Author: zxf
"""

import datetime, logging,pickle,os
from clickhouse_driver import Client

logging.basicConfig(level=logging.DEBUG)

"""
SQL EXMAPLE:
    SELECT GlobalEventID, Actor1Name, EventCode, Actor2Name, AllNames 
    from (SELECT GlobalEventID, Actor1Name, EventCode,Actor2Name,SOURCEURL 
            FROM gdelt.event 
            where EventDate = '2022-06-01' AND  ActionGeo_ADM1Code = 'JA40') temp_event 
    inner join (SELECT AllNames, DocumentIdentifier 
                FROM gdelt.gkg where GkgDate = '2022-06-01') temp_gkg
    on temp_event.SOURCEURL == temp_gkg.DocumentIdentifier 
 """

data_sql = \
    '''
        SELECT GlobalEventID, Actor1Name, EventCode, Actor2Name, AllNames, EventDate
        from (SELECT GlobalEventID, Actor1Name, EventCode,Actor2Name,SOURCEURL, EventDate
                FROM gdelt.event 
                where EventDate = '{get_date}' AND  ActionGeo_ADM1Code = '{ADM1Code}') temp_event 
        inner join (SELECT AllNames, DocumentIdentifier 
                    FROM gdelt.gkg where GkgDate = '{get_date}') temp_gkg
        on temp_event.SOURCEURL == temp_gkg.DocumentIdentifier
    '''

logging.basicConfig(level=logging.INFO)

def getChainData(start_date, end_date, city_name, city_ADM1Code, base_dir):
    client=Client(host='172.20.201.131',port=9000,user='default',password='1',database='gdelt')
    iter_date = start_date
    data = []
    while(iter_date <= end_date):
        get_data_sql = data_sql.format(get_date=str(iter_date),ADM1Code=city_ADM1Code)
        event_data = client.execute(get_data_sql)
        if len(event_data)<1:
            iter_date += datetime.timedelta(days=1)
            continue
        
        for i in range(len(event_data)):
            
            continue_flag = False
            for j in range(1,5):
                if len(event_data[i][j]) == 0 :
                    continue_flag = True
                    break
            if continue_flag == True:
                continue
            # process allnames
            all_names = []
            for one_name in  event_data[i][4].split(';'):
                all_names.append(one_name.split(',')[0])
            str_names = ""
            for one_name in all_names:
                str_names += " " + one_name
            str_names = str_names.strip()
            
            # get one data
            temp_one = []
            temp_one.extend(event_data[i][0:4])
            temp_one.append(str_names)
            temp_one.append(str(event_data[i][5]))
            data.append(temp_one)
        logging.info("already get {num} data".format(num = len(data)))
        # iter date
        iter_date += datetime.timedelta(days=1)

    # storage the data
    deposit_dir = os.path.join(base_dir,city_name)
    if not os.path.exists(deposit_dir):
        os.mkdir(deposit_dir)
    data_filename = city_name + ".pkl"
    data_path = os.path.join(deposit_dir,data_filename)
    with open(data_path, 'wb') as f:
        pickle.dump(data,f)
    logging.info("storage file {file} successfully.".format(file=data_path))


if __name__ == '__main__':
    # begin_date = datetime.date(2018,1,1)
    # end_date = datetime.date(2022,3,1)
    # getChainData(begin_date, end_date,'bangkok',"/data/zxf/data/DGCN_Data")
    print("INFO: this is a get GDELT data for DGCN model program\n\
        first please input the period of date for data acquiring:\n\
        for example: 20180101-20220701\n\
        this period is default, you can leave this input empty, program will tacitly get this:")
    date_period = input()
    begin_date = datetime.date(2018,1,1)
    end_date = datetime.date(2022,7,1)
    if len(date_period) != 0:
        temp1,temp2 = date_period.split('-')
        try:
            begin_date = datetime.datetime.strptime(temp1,"%Y%m%d")
            end_date = datetime.datetime.strptime(temp2,"%Y%m%d")
        except Exception as e:
            print("parse date wrong!")
            exit()
    print("next, please input city's name and ActionGeo_ADM1Code, for example 'Bangkok:TH40':")
    city_name, ADM1Code_str = input().split(':')
    print("Make sure enter parameters correct:")
    print("begin date:{}".format(begin_date))
    print("end date:{}".format(end_date))
    print("query cityname is {0}, ActionGeo_ADM1Code is {1}".format(city_name, ADM1Code_str))

    print("for sure, please enter 'y/Y', else please enter other keys:")
    key_receive = input()
    if not (key_receive == 'y' or key_receive == 'Y'):
        exit()
    getChainData(begin_date, end_date, city_name, ADM1Code_str,"/data/zxf/data/GLEAN_Data")



