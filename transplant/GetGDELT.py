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

logging.basicConfig(level=logging.INFO)

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
                where EventDate = '{get_date}' AND  ActionGeo_ADM1Code = '{ADM1Code}' AND Actor1Name != '' AND EventCode != '' AND Actor2Name != '' AND SOURCEURL != '' ) temp_event 
        inner join (SELECT AllNames, DocumentIdentifier 
                    FROM gdelt.gkg where GkgDate = '{get_date}') temp_gkg
        on temp_event.SOURCEURL == temp_gkg.DocumentIdentifier
    '''
event_codes= set(['010', '011', '012', '013', '014', '015', '016', '017', '018', '019', '020', '021', '0211', '0212', '0213', '0214', '022', '023', '0231', '0232', '0233', '0234', '024', '0241', '0242', '0243', '0244', '025', '0251', '0252', '0253', '0254', '0255', '0256', '026', '027', '028', '030', '031', '0311', '0312', '0313', '0314', '032', '033', '0331', '0332', '0333', '0334', '034', '0341', '0342', '0343', '0344', '035', '0351', '0352', '0353', '0354', '0355', '0356', '036', '037', '038', '039', '040', '041', '042', '043', '044', '045', '046', '050', '051', '052', '053', '054', '055', '056', '057', '060', '061', '062', '063', '064', '070', '071', '072', '073', '074', '075', '080', '081', '0811', '0812', '0813', '0814', '082', '083', '0831', '0832', '0833', '0834', '084', '0841', '0842', '085', '086', '0861', '0862', '0863', '087', '0871', '0872', '0873', '0874', '090', '091', '092', '093', '094', '100', '101', '1011', '1012', '1013', '1014', '102', '103', '1031', '1032', '1033', '1034', '104', '1041', '1042', '1043', '1044', '105', '1051', '1052', '1053', '1054', '1055', '1056', '106', '107', '108', '110', '111', '112', '1121', '1122', '1123', '1124', '1125', '113', '114', '115', '116', '120', '121', '1211', '1212', '1213', '122', '1221', '1222', '1223', '1224', '123', '1231', '1232', '1233', '1234', '124', '1241', '1242', '1243', '1244', '1245', '1246', '125', '126', '127', '128', '129', '130', '131', '1311', '1312', '1313', '132', '1321', '1322', '1323', '1324', '133', '134', '135', '136', '137', '138', '1381', '1382', '1383', '1384', '1385', '139', '140', '141', '1411', '1412', '1413', '1414', '142', '1421', '1422', '1423', '1424', '143', '1431', '1432', '1433', '1434', '144', '1441', '1442', '1443', '1444', '145', '1451', '1452', '1453', '1454', '150', '151', '152', '153', '154', '160', '161', '162', '1621', '1622', '1623', '163', '164', '165', '166', '1661', '1662', '1663', '170', '171', '1711', '1712', '172', '1721', '1722', '1723', '1724', '1725', '173', '174', '175', '180', '181', '182', '1821', '1822', '1823', '183', '1831', '1832', '1833', '184', '185', '186', '190', '191', '192', '193', '194', '195', '196', '200', '201', '202', '203', '204', '2041', '2042'])
logging.basicConfig(level=logging.INFO)

def getChainData(start_date, end_date, country_name, city_name, city_ADM1Code, base_dir):
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
            if event_data[i][2] not in event_codes or len(event_data[i][1])==0 or \
                len(event_data[i][3])==0 or len(event_data[i][4])<20:
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
    deposit_dir = os.path.join(base_dir, country_name)

    if not os.path.exists(deposit_dir):
        os.mkdir(deposit_dir)
    deposit_dir = os.path.join(deposit_dir, f'{begin_date.year}-{end_date.year}')
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
        for example: 20170101-20191231\n\
        this period is default, you can leave this input empty, program will tacitly get this:")
    begin_date = datetime.date(2017,1,1)
    end_date = datetime.date(2019,12,31)
    date_period = input()
    if len(date_period) != 0:
        temp1, temp2 = date_period.split('-')
        try:
            begin_date = datetime.datetime.strptime(temp1,"%Y%m%d").date()
            end_date = datetime.datetime.strptime(temp2,"%Y%m%d").date()
        except Exception as e:
            print("parse date wrong, please input again:")
            exit()
    print("next, please input country, city and ActionGeo_ADM1Code, for example 'Thailand:Bangkok:TH40':")
    country_name, city_name, ADM1Code_str = input().split(':')
    # set output dir
    depos_dir = "/data/zxf/data/ResearchingData"
    print("next, please input deposit directory, leave empty means choose default \nwhich is </data/zxf/data/ResearchingData>:")
    depos_str = input()
    if len(depos_str) != 0:
        if not os.path.exists(depos_str):
            print("this dir is not exists")
            exit()
        depos_dir = depos_str
    print("Make sure enter parameters correct:")
    print("begin date:{}".format(begin_date))
    print("end date:{}".format(end_date))
    print("country is:{0}, cityname is {1}, ActionGeo_ADM1Code is {2}"\
        .format(country_name, city_name, ADM1Code_str))
    print(f"data deposit path: {depos_dir}")
    print("for sure, please enter 'y/Y', else please enter other keys:")
    key_receive = input()
    if not (key_receive == 'y' or key_receive == 'Y'):
        exit()
    getChainData(begin_date, end_date, country_name, city_name, ADM1Code_str, depos_dir)

