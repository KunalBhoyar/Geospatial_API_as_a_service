import streamlit as st
import importlib.util
import os
import re
from frontendAPICalls import *

current_directory = os.getcwd()

# module_directory = os.path.abspath(os.path.join(current_directory, 'src', 'data'))
# module_path = os.path.join(module_directory, 'sqlite_main.py')
# ops_path = os.path.join(module_directory, 'backend_ops.py')

# spec = importlib.util.spec_from_file_location("sqlite_main", module_path)
# spec_ops = importlib.util.spec_from_file_location("backend_ops", ops_path)
# db_methods = importlib.util.module_from_spec(spec)
# ops = importlib.util.module_from_spec(spec_ops)
# spec.loader.exec_module(db_methods)
# spec_ops.loader.exec_module(ops)

AWS_ACCESS_KEY_ID = st.secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]
LOG_GROUP_NAME = st.secrets["LOG_GROUP_NAME"]
LOG_STREAMLIT_NAME= st.secrets["LOG_STREAMLIT_NAME"]

st.title('NEXRAD')

if "login_success" not in st.session_state:
    st.session_state["login_success"] = False
if 'limit_exceeded' not in st.session_state:
    st.session_state["limit_exceeded"] = False

def main():
    st.subheader('Search by Fields')

    if 'search_btn_clicked' not in st.session_state:
        st.session_state['search_btn_clicked'] = False
    if 'search_generate_link' not in st.session_state:
        st.session_state['search_generate_link'] = False
    if 'nexrad_prefix_filename' not in st.session_state:
        st.session_state['nexrad_prefix_filename'] = False
    if 'nexrad_filename' not in st.session_state:
        st.session_state['nexrad_filename'] = False

    year, month, day = st.columns(3)

    nexrad_year_list_res = api_getNEXRADYear(st.session_state.logged_in_user['token'])

    if nexrad_year_list_res['status_code'] == 200:

        with year:
            selectedYear = st.selectbox('Year', nexrad_year_list_res['response'])
            # st.session_state['selectedYear'] = selectedYear

        nexrad_month_list = api_getNEXRADMonth(st.session_state.logged_in_user['token'], selectedYear)
        with month:
            selectedMonth = st.selectbox('Month', nexrad_month_list)
            if selectedMonth < 10:
                selectedMonth = "0" + str(selectedMonth)
            # st.session_state['selectedMonth'] = selectedMonth

        nexrad_day_list = api_getNEXRADDay(st.session_state.logged_in_user['token'], selectedYear, selectedMonth)
        with day:
            selectedDay = st.selectbox('Day', nexrad_day_list)
            if selectedDay < 10:
                selectedDay = "0" + str(selectedDay)
            # st.session_state['selectedDay'] = selectedDay

        nexrad_sites_list = api_getNEXRADSites(st.session_state.logged_in_user['token'], selectedYear, selectedMonth, selectedDay)

        # st.session_state['selectedSiteList'] = nexrad_sites_list
        selectedSite = st.selectbox('Site', nexrad_sites_list)


        def handleSearch():
            st.session_state['search_btn_clicked'] = True

        def handleSearchGenLink():
            st.session_state['search_generate_link'] = True

        st.button('Search', on_click = handleSearch)

        selectedFile = ""

        if st.session_state['search_btn_clicked']:

            response = api_NEXRADQueryFiles(st.session_state.logged_in_user['token'], selectedYear, selectedMonth, selectedDay, selectedSite)
            
            if response['status_code'] == 200:
                selectedFile = st.selectbox("Files", response['message'])

                st.write(selectedFile)
                st.session_state.nexrad_filename = f'{selectedYear}/{selectedMonth}/{selectedDay}/{selectedSite}/{selectedFile}'
                st.button("Generate Link", on_click = handleSearchGenLink, key = "gen_link")  
                
            elif response['status_code'] == 429:
                handleLimitExceeded()

        if st.session_state['search_generate_link']:
            
            # try:
                # nexrad_file_status =  ops.copyFileFromNexradToS3(selectedFile, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            if not st.session_state['limit_exceeded']:

                nexrad_file_status_res =  copyNEXRADFileToBucket(st.session_state.logged_in_user['token'],
                                                                selectedYear, selectedMonth, selectedDay, selectedSite, selectedFile)

                if nexrad_file_status_res['status_code'] == 200:
                    if nexrad_file_status_res['message']:
                        st.write("AWS link")
                        st.write("https://damg7245-s3-storage.s3.amazonaws.com/" + st.session_state.nexrad_filename)                  
                    else:
                        pass
                elif nexrad_file_status_res['status_code'] == 429:
                    handleLimitExceeded()
                    

        #    Search By File Name

        ### Search by Filename

        if not st.session_state['limit_exceeded']:
            st.subheader("Search By File Name")

            searchedFilename = st.text_input("Filename", key="filename-search")

            if 'nex-file-searched' not in st.session_state:
                st.session_state['nex-file-searched'] = False

            if 'nex-file-name-check' not in st.session_state:
                st.session_state['nex-file-name-check'] = False

            if 'nex-file-link-generated' not in st.session_state:
                st.session_state['nex-file-link-generated'] = False

            def handleSearchedFileName():
                
                print(st.session_state['nex-file-searched'])
                if st.session_state['nex-file-searched']:
                    pattern = r'^[A-Z0-9]{4}\d{8}_\d{6}(?:_MDM)?_V\d{2}'

                    if re.match(pattern, st.session_state['nex-file-searched']):
                        st.session_state['nex-file-name-check'] = True
                        
                        aws_file_link_res = get_nexrad_file_link(st.session_state.logged_in_user['token'], 
                                                            st.session_state['nex-file-searched'])
                        
                        if aws_file_link_res['status_code'] == 200:
                            prefix = "https://damg7245-s3-storage.s3.amazonaws.com/"
                            st.session_state['nex-file-link-generated'] = prefix + aws_file_link_res['response']
                            st.session_state['nex-filename-search']= ""
                        
                        elif aws_file_link_res['status_code'] == 429:
                            handleLimitExceeded()

                        else:
                            st.error('No such file exists!', icon = "⚠️")
                            st.session_state['nex-file-searched']= ""
                            st.session_state['nex-filename-search']= ""
                            st.session_state['nex-file-link-generated'] = ""
                            # ops.create_steamlit_logs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
                            #                     f"NEXRAD:: Search Query Results: No such file exists!",
                            #                     LOG_GROUP_NAME,
                            #                     LOG_STREAMLIT_NAME)
                    else:
                        # print(re.match(pattern, st.session_state['file-searched']) )
                        st.error('Provide proper file name', icon = "⚠️")
                        st.session_state['nex-file-searched']= ""
                        st.session_state['nex-filename-search']= ""
                        st.session_state['nex-file-link-generated'] = ""
                                    

            if not searchedFilename == "":
                st.session_state['nex-file-searched'] = searchedFilename
                st.button("Generate File Link", on_click = handleSearchedFileName)


            if st.session_state['nex-file-name-check']:
                st.write("File name")
                st.write(st.session_state['nex-file-searched'])

            if st.session_state['nex-file-link-generated']:
                st.write(st.session_state['nex-file-link-generated'])
    
    else:
        st.write("Limit exceeded!")



def handleLimitExceeded():
    st.header("API calls limit exceeded!")
    st.session_state['limit_exceeded'] = True

if __name__ == "__main__":
    if st.session_state['login_success']:
        # healthResponse = api_getNEXRADSitesLoc(st.session_state.logged_in_user['token'])
        # print(healthResponse)
        # if healthResponse['status_code'] == 429:
        #     handleLimitExceeded()
        # else:
        #     main()
        if st.session_state['limit_exceeded']:
            handleLimitExceeded()
        else:
            main()
    else:
        st.write('Please login to access this page!')
