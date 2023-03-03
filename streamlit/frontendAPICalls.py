import requests
import re

#USER REGISTRATION
URL = 'http://localhost:8000' 

#API health check
def api_healthCheck():
    response = requests.get(URL + '/')
    return response.json()

def api_userRegistration(username, password, plan):
    
    REGISTER_URL = URL + '/register'

    data = {
        "username": username,
        "password": password,
        "tier": plan
    }

    response = requests.post(REGISTER_URL, json=data)
    if response.status_code == 201:
        return True
    else:
        return False

    # if re.match(USERNAME_PATTERN, username):
    #     if re.match(PASSWORD_PATTERN, password):
    #         pass        
    #     else: return { message: 'Incorrect password format' }
    # else:
    #     return { message: "Provide correct username format" }

def api_userLogin(username, password):

    LOGIN_URL = URL + '/login'

    data = {
        "username": username,
        "password": password
    }

    response = requests.post(LOGIN_URL, json = data)
    
    if response.status_code == 200:
        data = {
            "status": response.status_code,
            "response": response.json()['token']
        }
        return data  
    else:
        data = {
            "status": response.status_code,
            "response": response.json()["detail"]
        }

        return data
    # except:
    #     print('API call error!')


def api_getGOESYear(token):

    try:
        GET_GOES_URL = URL + '/geos_get_year'

        response = requests.get(GET_GOES_URL, headers={'Authorization': f'Bearer {token}'})
        yearList = []
        if response.status_code == 200:
            for item in response.json():
                yearList.append(item['year'])
            return {
                "status_code": response.status_code,
                "year_list": yearList
            }
        else:
            return {
                "status_code": response.status_code
            }
    except:
        print("Issue with API call: geos_get_year")


def api_getGOESDay(token, year):
    try:
        GET_GOES_URL = URL + f'/geos_get_day/{year}'
        # print(token)
        response = requests.get(GET_GOES_URL, headers={'Authorization': f'Bearer {token}'})
        dayList = []
        if response.status_code == 200:
            for item in response.json():
                dayList.append(item['day'])

            # print(dayList)
            return dayList
        else:
            # print(response.json())
            return []
    except:
        print("Issue with API call: geos_get_day")

def api_getGOESHour(token, year, day):
    try:
        GET_GOES_URL = URL + f'/geos_get_hour/{year}/{day}'
        response = requests.get(GET_GOES_URL, headers={'Authorization': f'Bearer {token}'})
        hourList = []
        if response.status_code == 200:
            for item in response.json():
                hourList.append(item['hour'])

            # print(hourList)
            return hourList
        else:
            # print(response.json())
            return []
    except:
        print("Issue with API call: geos_get_hour")

def api_getNEXRADYear(token):

    try:
        GET_NEXRAD_URL = URL + '/nexrad_get_year'

        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        yearList = []
        if response.status_code == 200:
            for item in response.json():
                yearList.append(item['year'])

            print(yearList)
            return {
                "status_code": response.status_code,
                "response": yearList
            }
        else:
            print(response.json())
            return {
                "status_code": response.status_code
            }
    except:
        print("Issue with API call: api_getNEXRADYear")

def api_getNEXRADMonth(token, year):
    try:
        GET_NEXRAD_URL = URL + f'/nexrad_get_month/{year}'
        # print(token)
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        dayList = []
        if response.status_code == 200:
            for item in response.json():
                dayList.append(item['month'])

            # print(dayList)
            return dayList
        else:
            # print(response.json())
            return []
    except:
        print("Issue with API call: api_getNEXRADMonth")

def api_getNEXRADDay(token, year, month):
    try:
        GET_NEXRAD_URL = URL + f'/nexrad_get_day/{year}/{month}'
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        dayList = []
        if response.status_code == 200:
            for item in response.json():
                dayList.append(item['day'])

            # print(hourList)
            return dayList
        else:
            # print(response.json())
            return []
    except:
        print("Issue with API call: api_getNEXRADDay")

def api_getNEXRADSites(token, year, month, day):
    try:
        GET_NEXRAD_URL = URL + f'/nexrad_get_sites/{year}/{month}/{day}'
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        sitesList = []
        if response.status_code == 200:
            for item in response.json():
                sitesList.append(item['stationcode'])

            # print(hourList)
            return sitesList
        else:
            # print(response.json())
            return []
    except:
        print("Issue with API call: api_getNEXRADSites")

def api_getNEXRADSitesLoc(token):
    try:
        GET_NEXRAD_URL = URL + f'/get_nexrad_sites'
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        sitesLocList = []
        if response.status_code == 200:
            if response.json()['status_code'] == 200:
                return {
                    "status_code": 200,
                    "response": response.json()['response']
                }
            elif response.json()['status_code'] == 429:
                return {
                    "status_code": 429
                }
        elif response.status_code == 429:
            # print(response.json())
            return {
                "status_code": 429
            }
    except:
        print("Issue with API call: api_getNEXRADSitesLoc")

def api_GOESQueryFiles(token, product, year, day, hour):
    try:
        GET_GOES_URL = URL + f'/goes_query_files/{product}/{year}/{day}/{hour}'
        response = requests.get(GET_GOES_URL, headers={'Authorization': f'Bearer {token}'})
        if response.status_code == 200:
            if response.json()['status_code'] == 200:
                return {
                    "status_code": 200,
                    "message": response.json()['response']
                }
            elif response.json()['status_code'] == 429:
                return {
                    "status_code": 429
                }
        elif response.status_code == 429:
            return {
                "status_code": 429
            }
    except Exception as e:
        print("Issue with API call: api_GOESQueryFiles")

def api_NEXRADQueryFiles(token, year, month, day, site):
    try:
        GET_NEXRAD_URL = URL + f'/nexrad_query_files/{year}/{month}/{day}/{site}'
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        if response.status_code == 200:
            
            if response.json()['status_code'] == 200:
                return {
                    "status_code": 200,
                    "message": response.json()['response']
                }
            elif response.json()['status_code'] == 429:
                return {
                    "status_code": 429
                }
        elif response.status_code == 429:
            return {
                "status_code": 429
            }
    except:
        print("Issue with API call: api_NEXRADQueryFiles")


def copyFileToBucket(token, product, year, day, hour, filename, source_bucket="noaa-goes18"):
    print(product, year, day, hour, filename)
    try:
        GET_GOES_URL = URL + f'/copy_file_s3/{source_bucket}/{product}/{year}/{day}/{hour}/{filename}'
        response = requests.get(GET_GOES_URL, headers={'Authorization': f'Bearer {token}'})
        
        if response.status_code == 200:
            if response.json()['status_code'] == 200:
                return {
                    "status_code": 200,
                    "message": True
                }
            elif response.json()['status_code'] == 429:
                return {
                    "status_code": 429
                }
        elif response.status_code == 429:
            return {
                "status_code": 429,
                "message": False
            }
    except Exception as e:
        print(e)

def copyNEXRADFileToBucket(token, year, month, day, site, filename, source_bucket="noaa-nexrad-level2"):
    try:
        GET_NEXRAD_URL = URL + f'/copy_nexrad_file_s3/{source_bucket}/{year}/{month}/{day}/{site}/{filename}'
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        
        if response.status_code == 200:
            if response.json()['status_code'] == 200:
                return {
                    "status_code": 200,
                    "message": True
                }
            elif response.json()['status_code'] == 429:
                return {
                    "status_code": 429
                }
        elif response.status_code == 429:
            return {
                "status_code": 429,
                "message": False
            }
    except:
        print("Issue with API call: copyFileToBucket")

def get_goes_by_filename(token, filename, source_bucket="noaa-goes18"):
    try:
        GET_GOES_URL = URL + f'/get_goes_by_filename/{source_bucket}/{filename}'
        response = requests.get(GET_GOES_URL, headers={'Authorization': f'Bearer {token}'})
        
        if response.status_code == 200:
            print(response)
            return {
                "status_code":200,
                "message": response.json()['file_prefix']
            }
        elif response.status_code == 429:
            return {
                "status_code":429
            }
    except:
        print("Issue with API call: get_goes_by_filename")


def get_nexrad_file_link(token, filename, source_bucket="noaa-nexrad-level2"):
    try:
        GET_NEXRAD_URL = URL + f'/get_nexrad_file_link/{source_bucket}/{filename}'
        response = requests.get(GET_NEXRAD_URL, headers={'Authorization': f'Bearer {token}'})
        
        if response.status_code == 200:
            # print(response)
            return {
                "status_code":200,
                "response": response.json()['response']
            }
        elif response.status_code == 429:
            return {
                "status_code":429
            }
    except:
        print("Issue with API call: get_nexrad_file_link")


def createAPICountJSON(response):
    try:
        api_count_dict = {}
        # print(response[0][1])
        for res in response:
            # print(res[1])
            split_value = [s.strip() for s in res[1]['value'].split(',')]
            # print(split_value)
            result_api = split_value[1].split('=')[1].strip()
            # print(result_api)
            if result_api in api_count_dict:
                api_count_dict[result_api] += 1
            else:
                api_count_dict[result_api] = 1
        
        # print(api_count_dict)
        
        return api_count_dict

    except Exception as e:
        print(e)


def getAPISuccessCount(token, username, filter_range, log_name="fast_api_logs"):
    try:
        status = 2
        GET_API_SUCCESS_COUNT = URL + f'/get_full_logs/?log_name={log_name}&status={status}&userName={username}&filter_range={filter_range}'
        response = requests.get(GET_API_SUCCESS_COUNT, headers={'Authorization': f'Bearer {token}'})
        data = createAPICountJSON(response.json())
        if response.status_code == 200:
            return {
                "status_code": response.status_code,
                "response": data
            }
        else:
            return {
                "status_code": response.status_code
            }

    except Exception as e:
        print(e)

def getAPIFailureCount(token, username, filter_range, log_name="fast_api_logs"):
    try:
        status = 4
        GET_API_SUCCESS_COUNT = URL + f'/get_full_logs/?log_name={log_name}&status={status}&userName={username}&filter_range={filter_range}'
        response = requests.get(GET_API_SUCCESS_COUNT, headers={'Authorization': f'Bearer {token}'})
        data = createAPICountJSON(response.json())
        if response.status_code == 200:
            return {
                "status_code": response.status_code,
                "response": data
            }
        else:
            return {
                "status_code": response.status_code
            }

    except Exception as e:
        print(e)

def createAPICountJSONByDate(response):
    try:
        api_count_dict = {}
        
        for res in response:
            
            key = [s.strip() for s in res[0]['value'].split(" ")]
            
            dict_key = key[0]
            
            if dict_key in api_count_dict:
                api_count_dict[dict_key] += 1
            else:
                api_count_dict[dict_key] = 1
        
        return api_count_dict

    except Exception as e:
        print(e)


def getAPISuccessCountByDate(token, username, filter_range, log_name="fast_api_logs"):
    try:
        status = 2
        GET_API_SUCCESS_COUNT = URL + f'/get_full_logs/?log_name={log_name}&status={status}&userName={username}&filter_range={filter_range}'
        response = requests.get(GET_API_SUCCESS_COUNT, headers={'Authorization': f'Bearer {token}'})
        data = createAPICountJSONByDate(response.json())
        if response.status_code == 200:
            return {
                "status_code": response.status_code,
                "response": data
            }
        else:
            return {
                "status_code": response.status_code
            }

    except Exception as e:
        print(e)

def getAPIFailedCountByDate(token, username, filter_range, log_name="fast_api_logs"):
    try:
        status = 4
        GET_API_SUCCESS_COUNT = URL + f'/get_full_logs/?log_name={log_name}&status={status}&userName={username}&filter_range={filter_range}'
        response = requests.get(GET_API_SUCCESS_COUNT, headers={'Authorization': f'Bearer {token}'})
        data = createAPICountJSONByDate(response.json())
        if response.status_code == 200:
            return {
                "status_code": response.status_code,
                "response": data
            }
        else:
            return {
                "status_code": response.status_code
            }

    except Exception as e:
        print(e)


def getUserLogCount(token, username, filter_range, log_name="fast_api_logs"):
    try:
        status = 2
        GET_API_SUCCESS_COUNT = URL + f'/get_log_count/?log_name={log_name}&status={status}&userName={username}&filter_range={filter_range}'
        response = requests.get(GET_API_SUCCESS_COUNT, headers={'Authorization': f'Bearer {token}'})
        
        if response.status_code == 200:
            return {
                "status_code": response.status_code,
                "response": response.json()
            }
        else:
            return {
                "status_code": response.status_code
            }

    except Exception as e:
        print(e)

# def healthCheck(token):
#     try:
#         response = requests.get(URL + '/healthz', headers={'Authorization': f'Bearer {token}'})

#         if response.status_code == 200:
#             return {
#                 "status_code": 200
#             }
#         elif response.status_code == 429:
#             return {
#                 "status_code": 429
#             }

#     except Exception as e:
#         print(e)