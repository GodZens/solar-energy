import os
import json
import glob
import pandas as pd
import numpy as np
import datetime

# 測站清單，中央氣象局的預報資料是以測站為單位蒐集
stat_list = ['F-D0047-017']

# 打包預報資料
def aggregate_data2(start_date, end_date , stat_list=stat_list, directory='.'):
    def read_file(stat_id, filename):
        try:
            with open(filename) as f:
                data = f.read()
        except UnicodeDecodeError:
            try:
                with open(filename, encoding='utf-8') as f:
                    data = f.read()
            except:
                print('Unknown error.', stat_id, filename) 
        except:
            print('Unknown error.', stat_id, filename)
        finally:
            f.close()
        return data

    array = []

    # 依次讀取測站目錄 & 測站目錄裡面的每一個文件
    for stat_id in stat_list:
        print('stat:', stat_id)
        for filename in glob.glob('{}/CWB.3H/{}/*.txt'.format(directory, stat_id)):
#             
            file_data = rebuild_crawler_time(filename)
#             start, end = target_date, target_date+datetime.timedelta(days=1)
            
            if end_date>file_data>start_date:

                print(file_data)
                # 01. 嘗試打開檔案，有可能遇到空白或損毀的檔案，則跳過
                file = read_file(stat_id, filename)

                # 02. 提取檔案的預報資訊，有可能讀到空白或亂碼檔案，故將該類檔案設為空陣列
                # 設定為空陣列的檔案在後續步驟會被跳過
                if(file[0]=='{'):
                    locus = json.loads(file)['records']['locations'][0]

                # 03. 檔案內包含多個鄉鎮的預報，逐項讀取
                for i in range(len(locus['location'])):
                    current = locus['location'][i]
                    # 04. 擷取前 12 小時的預報資料
                    # 預報內大部分變數包含未來 24 個 3 小時資料(72小時)
                    # 部分變數包含未來 12 個 6 小時資料(72小時)
                    for time in range(12):
                        try:
                            data = {}

                            # 05. 檢查當前預報點的屬性是否大於 10 個，否則跳過
                            forecast = current['weatherElement']
                            if(len(forecast) < 10):
                                continue

                            # 06. 縣市資訊 & 地區資訊
                            data['cityName'] = locus['locationsName']
                            data['locationName'] = current['locationName']
                            data['geocode'] = current['geocode']
                            data['lat'] = current['lat']
                            data['lon'] = current['lon']

                            # 07. 歷遍預報欄位(11個)
                            for feature in range(len(forecast)):
                                # 賦予屬性名稱，遇到以下欄位做特殊處理
                                name = f"{forecast[feature]['elementName']}/{forecast[feature]['description']}"

                                if(forecast[feature]['elementName'] == 'PoP6h'):
                                    try:
                                        # PoP6h 的預報間隔是 6 小時，故在每 2 筆逐 3 小時資料間插入同樣的值
                                        values = forecast[feature]['time'][int((time)/2)]['elementValue']
                                        data[name] = values[0]['value']
                                    except:
                                        data[element] = np.nan
                                elif(forecast[feature]['elementName'] == 'PoP12h'):
                                    continue
                                elif(forecast[feature]['elementName'] == 'WeatherDescription'):
                                    continue
                                else:
                                # 遇到其他(正常欄位)的處理方式
                                    try:
                                        # 如果預報欄位有兩個值的場合，需要特別處理
                                        values = forecast[feature]['time'][time]['elementValue']
                                        if(len(values) == 2):
                                            data[name] = values[0]['value']
                                            data[f'{name}(Unit)'] = values[1]['value']
                                        elif(len(values)==1):
                                            data[name] = values[0]['value']
                                        else:
                                            print('ERROR.')

                                    except:
                                        data[name] = np.nan

                            # 從欄位「天氣現象」獲取資料的時間戳記
                            data['datetime'] = forecast[1]['time'][time]['startTime']
                            data['crawler_time'] = filename
                            # 將整理完的資料加入陣列
                            array.append(data)

                        except:
                            print(f"Element Error: {filename}, {data['locationName']}, {time+1}, {name}")
    
    dataframe = pd.DataFrame(array)
    print('File Length: ', len(dataframe))
    
    return dataframe


# 儲存在 Windows 和 Ubuntu 的檔案名稱不同，在計算下載時間的時候會錯誤
# 透過程式將 2 種命名格式調整至固定格式
def replacer(s, newstring, index, nofail=False):
    # raise an error if index is outside of the string
    if not nofail and index not in range(len(s)):
        raise ValueError("index outside given string")

    # if not erroring, but the index is still not in the correct range..
    if index < 0:  # add it to the beginning
        return newstring + s
    if index > len(s):  # add it to the end
        return s + newstring

    # insert the new string between "slices" of the original
    return s[:index] + newstring + s[index + 1:]
def rebuild_crawler_time(string):
    try:
        new_string = string.replace(".txt", "")
        new_string = new_string.replace('%3A', ':')
        new_string = new_string.replace('_', ':')
        new_string = new_string[len(new_string)-19:len(new_string)-0]
        if(new_string.count(':')>2):
            new_string = replacer(new_string, " ", new_string.find(':'))
        new_string = pd.to_datetime(new_string)
    except:
        new_string = ''
    return new_string


# # 整合新舊資料 & 填充缺值
# ### 包含時間轉換、重新命名、去除重覆資料.....等
# ### 將整理完成的 3 小時預報和先前的合併
# ### 另外，根據預測時間，將資料分為 24 小時內和 3 小時內的預報

# 合併新舊預報資料，並儲存至本地資料夾
def merge_with_history(forecast, directory='.'):
    
    forecast_copy = forecast.copy()
    forecast_copy.rename(columns={
        'datetime': 'TIME_TO_INTERVAL', 'crawler_time': 'CrawlerTime',
        'locationName': 'LocationName', 'cityName': 'CityName',
        'lat': 'Latitude', 'lon': 'Longitude',
        'Wx/天氣現象': 'WeatherType', 
        'Wx/天氣現象(Unit)': 'WeatherType(index)',
        'PoP6h/6小時降雨機率': 'PoP6h(pred)',
        'PoP6h': 'PoP6h(pred)',
        'AT/體感溫度': 'ApparentTemperature(pred)',
        'T/溫度': 'Temperature(pred)',
        'CI/舒適度指數': 'ComfortIndex(pred)',
        'CI/舒適度指數(Unit)': 'ComfortIndex(index)',
        'RH/相對濕度': 'RelativeHumidity(pred)',
        'WS/風速': 'WindSpeed(pred)',
        'WS/風速(Unit)': 'WindSpeed(index)',
        'WD/風向': 'WindDirection(pred)',
        'Td/露點溫度': 'DewpointTemperature(pred)'}, inplace=True) 
    forecast_copy['CrawlerTime'] = forecast_copy['CrawlerTime'].apply(lambda x: rebuild_crawler_time(x))
    forecast_copy = build_multiple_lead_time_data(forecast_copy)
#     forecast_copy.to_csv(f'{directory}/CWB.3H/Save/CWB.3H.Merge.Multiple.csv', index=False)
    
    # read history data
    history = pd.read_csv(f'{directory}/CWB.3H/save/CWB.3H.Merge.Multiple.csv')
    history.rename(columns={
        'datetime': 'TIME_TO_INTERVAL', 'crawler_time': 'CrawlerTime',
        'locationName': 'LocationName', 'cityName': 'CityName',
        'lat': 'Latitude', 'lon': 'Longitude',
        'Wx/天氣現象': 'WeatherType', 
        'Wx/天氣現象(Unit)': 'WeatherType(index)',
        'PoP6h/6小時降雨機率': 'PoP6h(pred)',
        'PoP6h': 'PoP6h(pred)',
        'AT/體感溫度': 'ApparentTemperature(pred)',
        'T/溫度': 'Temperature(pred)',
        'CI/舒適度指數': 'ComfortIndex(pred)',
        'CI/舒適度指數(Unit)': 'ComfortIndex(index)',
        'RH/相對濕度': 'RelativeHumidity(pred)',
        'WS/風速': 'WindSpeed(pred)',
        'WS/風速(Unit)': 'WindSpeed(index)',
        'WD/風向': 'WindDirection(pred)',
        'Td/露點溫度': 'DewpointTemperature(pred)'}, inplace=True)
    
    history['TIME_TO_INTERVAL'] = pd.to_datetime(history['TIME_TO_INTERVAL'])
    rebuild_cwb = pd.concat([forecast_copy, history], axis=0, ignore_index=True)
    rebuild_cwb['TIME_TO_INTERVAL'] = pd.to_datetime(rebuild_cwb['TIME_TO_INTERVAL'])
    rebuild_cwb['CrawlerTime'] = pd.to_datetime(rebuild_cwb['CrawlerTime'])
    rebuild_cwb = rebuild_cwb.sort_values(by=['CityName', 'LocationName', 'TIME_TO_INTERVAL', 'CrawlerTime'], inplace=False)
    
    # 移除重複的預測資料
    rebuild_cwb = rebuild_cwb.drop_duplicates(['TIME_TO_INTERVAL', 'CityName', 'LocationName', 'TimeAhead'], keep="last")
    rebuild_cwb = fill_multiple_lead_time_data(rebuild_cwb)
    # 匯出整理後的資料，檔案類型：CSV
    print(f'history: {len(history)}, merge: {len(rebuild_cwb)}')
    get_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S').replace(':', '%3A')
    
    # 儲存 2 個檔案，分別表示當前最新的整合檔案(merge)，以及紀錄歷史整合結果(file name: get-time)
    rebuild_cwb.to_csv(f'{directory}/CWB.3H/save/CWB.3H.Merge.Multiple.csv', index=False)
#     rebuild_cwb.to_csv(f'{directory}/CWB.3H/save/{get_time}.csv', index=False)
    forecast_copy.to_csv(f'{directory}/CWB.3H/save/CWB.3H.Merge.Multiple(new).csv', index=False)

    return True

# 根據提前預報的時間，將資料分成:「 24小時前 」,「 3小時前」、「 3小時內」
def build_multiple_lead_time_data(data):
    forecast = data.copy()
    forecast['TIME_TO_INTERVAL'] = pd.to_datetime(forecast['TIME_TO_INTERVAL'])
    forecast['CrawlerTime'] = pd.to_datetime(forecast['CrawlerTime'])
    forecast['TimeAhead'] = forecast['TIME_TO_INTERVAL'] - forecast['CrawlerTime']
    forecast['DayAhead'] = forecast['TIME_TO_INTERVAL'].dt.date - forecast['CrawlerTime'].dt.date
    forecast.sort_values(by=['CityName', 'LocationName', 'TIME_TO_INTERVAL', 'TimeAhead'], inplace=True)
    forecast = forecast.reset_index(inplace=False, drop=True)
    
    # 01.
    merge0 = forecast.copy()
    merge0 = merge0.drop_duplicates(['CityName', 'LocationName', 'TIME_TO_INTERVAL'], keep="first")  
    merge0['TimeAhead'] = 0
    print('merge0' ,len(merge0))
    
    # 02.
    merge3 = forecast.copy()
    merge3 = merge3[merge3['TimeAhead']>=datetime.timedelta(hours=3)]
    merge3 = merge3.drop_duplicates(['CityName', 'LocationName', 'TIME_TO_INTERVAL'], keep="first")  
    merge3['TimeAhead'] = 3
    print('merge3', len(merge3))
    
    # 03.
    merge24 = forecast.copy()
    merge24 = merge24[merge24['DayAhead']>=datetime.timedelta(days=1)]
    merge24 = merge24.drop_duplicates(['CityName', 'LocationName', 'TIME_TO_INTERVAL'], keep="first")  
    merge24['TimeAhead'] = 24
    print('merge24', len(merge24))
    
    build = pd.concat([merge24, merge3, merge0], axis=0, ignore_index=True)
    build = build.drop(['DayAhead'], axis=1)
    return build

# 填補不同時間間隔的資料
def fill_multiple_lead_time_data(data):
    forecast = data.copy()
    forecast['TIME_TO_INTERVAL'] = pd.to_datetime(forecast['TIME_TO_INTERVAL'])
    forecast['CrawlerTime'] = pd.to_datetime(forecast['CrawlerTime'])
    forecast = forecast.sort_values(by=['CityName', 'LocationName', 'TIME_TO_INTERVAL', 'CrawlerTime'], inplace=False)
    forecast = forecast.reset_index(inplace=False, drop=True)
    
    # 01.最近抓取的資料
    merge0 = forecast.copy()
    merge0 = merge0.drop_duplicates(['CityName', 'LocationName', 'TIME_TO_INTERVAL'], keep="last")  
    merge0['TimeAhead'] = 0
    print('merge0' ,len(merge0))
    
    # 02.三小時前抓取的
    merge3 = forecast.copy()
    merge3 = merge3[merge3['TimeAhead'].eq(3)]
    filling = merge0[~merge0['TIME_TO_INTERVAL'].isin(merge3['TIME_TO_INTERVAL'].tolist())]
    merge3 = pd.concat([merge3, filling]).reset_index(inplace=False, drop=True)
    merge3['TimeAhead'] = 3
    print('merge3', len(merge3))
    
    # 03.一天以前抓取的
    merge24 = forecast.copy()
    merge24 = merge24[merge24['TimeAhead'].eq(24)]
    filling = merge3[~merge3['TIME_TO_INTERVAL'].isin(merge24['TIME_TO_INTERVAL'].tolist())]
    merge24 = pd.concat([merge24, filling]).reset_index(inplace=False, drop=True)
    merge24['TimeAhead'] = 24
    print('merge24', len(merge24))
    
    build = pd.concat([merge24, merge3, merge0], axis=0, ignore_index=True)
    return build


# 資料整理完成後，清除本地資料夾的原始預報資料
def remove_processed_data(directory='.'):

    for folder in os.listdir(directory + '/CWB.3H/'):
        
        # 依次讀取根目錄下的每一個資料夾下的每一個文件
        if(folder=='save'):
            print('found save file.')
            continue
            
        for filename in glob.glob('{}/CWB.3H/{}/*.txt'.format(directory, folder)):
            if os.path.isfile(filename):
                os.remove(filename)
            elif os.path.isdir(filename):
                shutil.rmtree(filename,True)
    
    return True


# # 主程式


# tdate = pd.to_datetime('2022-05-10')
# # 打包 1 小時預報資料
# forecast = aggregate_data(tdate)
# # 合併新舊預報資料，並儲存至本地資料夾
# merge_with_history(forecast)
# # 資料整理完成後，清除本地資料夾的原始預報資料
# # remove_processed_data()