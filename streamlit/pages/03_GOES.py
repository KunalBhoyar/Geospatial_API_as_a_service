import streamlit as st
import importlib.util
import os
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

import boto3
import re

AWS_ACCESS_KEY_ID = st.secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]
LOG_GROUP_NAME = st.secrets["LOG_GROUP_NAME"]
LOG_STREAMLIT_NAME= st.secrets["LOG_STREAMLIT_NAME"]

st.title('GOES')

if "login_success" not in st.session_state:
    st.session_state["login_success"] = False
if 'limit_exceeded' not in st.session_state:
    st.session_state["limit_exceeded"] = False

def main():
    # st.session_state
    if st.session_state['limit_exceeded']:
        st.header('APIs call limit exceeded!')

    if 'btn_clicked' not in st.session_state:
        st.session_state['btn_clicked'] = False
    if 'generate_link' not in st.session_state:
        st.session_state['generate_link'] = False
    if 'prefix_filename' not in st.session_state:
        st.session_state['prefix_filename'] = False
    if "searched_filename" not in st.session_state:
        st.session_state["searched_filename"] = False    
    if 'file-searched' not in st.session_state:
        st.session_state['file-searched'] = False
    if 'file-name-check' not in st.session_state:
        st.session_state['file-name-check'] = False
    if 'file-link-generated' not in st.session_state:
        st.session_state['file-link-generated'] = False
    
    st.subheader('Search by Fields')

    product  = ["ABI-L1b-RadC"]

    selectedProduct = st.selectbox("Product", product)

    year, day, hour = st.columns(3)    

    year_list_response = api_getGOESYear(st.session_state.logged_in_user["token"])

    if year_list_response["status_code"] != 429:

        with year:
            selectedYear = st.selectbox("Year", year_list_response["year_list"])
            # ops.create_steamlit_logs(ops.getCloudwatchInstance(), f"Selected Year {selectedYear}")

        day_list = api_getGOESDay(st.session_state.logged_in_user["token"], selectedYear)

        with day:
            selectedDay = st.selectbox("Day", day_list)
            if selectedDay < 10:
                selectedDay = "00" + str(selectedDay)
            elif selectedDay >= 10 and selectedDay < 100:
                selectedDay = "0" + str(selectedDay)
            # ops.create_steamlit_logs(ops.getCloudwatchInstance(), f"Selected Day {selectedDay}")

        hour_list = api_getGOESHour(st.session_state.logged_in_user["token"], selectedYear, selectedDay)

        with hour:
            selectedHour = st.selectbox("Hour", hour_list)
            if selectedHour < 10:
                selectedHour = "0" + str(selectedHour)
            # ops.create_steamlit_logs(ops.getCloudwatchInstance(), f"Selected Hour {selectedHour}")

        # files = ["Select search query"]

        # def handleSearch():
            # files = ops.geos_query_files(product[0], selectedYear, selectedDay, selectedHour)


        # if 'btn_clicked' not in st.session_state:
        #     st.session_state['btn_clicked'] = False
        # if 'generate_link' not in st.session_state:
        #     st.session_state['generate_link'] = False
        # if 'prefix_filename' not in st.session_state:
        #     st.session_state['prefix_filename'] = False
        # if "searched_filename" not in st.session_state:
        #     st.session_state["searched_filename"] = False



        def callback():
            st.session_state['btn_clicked'] = True
            
            # ops.create_steamlit_logs(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
            #                         f"Search Hit {selectedProduct} {selectedYear} {selectedDay} {selectedHour}",
            #                         LOG_GROUP_NAME,
            #                         LOG_STREAMLIT_NAME)
            

        def generateLink():
            st.session_state['generate_link'] = True

        st.button("Search", on_click=callback, key = "search")

        selectedFile=""

        # st.session_state

        if st.session_state['btn_clicked']:

            # print("called here", selectedHour)
            if not st.session_state['limit_exceeded']:
                # import ipdb; ipdb.set_trace();
                response = api_GOESQueryFiles(st.session_state.logged_in_user["token"],
                                                                        product[0], selectedYear, 
                                                                        selectedDay, selectedHour)
                if not st.session_state['limit_exceeded'] and response['status_code'] == 200:
                    selectedFile = st.selectbox("Files", response['message'])
                    st.session_state.searched_filename = f'{product[0]}/{selectedYear}/{selectedDay}/{selectedHour}/{selectedFile}'
                    st.write(selectedFile)

                    st.button("Generate Link", on_click = generateLink, key = "gen_link_kkkkkk")   

                elif response['status_code'] == 429:
                    handleLimitExceeded()   
            else:
                handleLimitExceeded()
            

        if st.session_state['generate_link']:
            
            # try:
                # gen_link_file_status = ops.downloadFileAndMove(selectedFile, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
            if not st.session_state['limit_exceeded']:

                copyFileToBucketRes = copyFileToBucket(st.session_state.logged_in_user["token"], product[0], selectedYear, selectedDay, selectedHour, selectedFile)
            
        
                if copyFileToBucketRes['status_code'] == 200:
                    gen_link_file_status = copyFileToBucketRes['message']
                    # print(gen_link_file_status, 'status')
                
                    if gen_link_file_status:
                        st.write("AWS link")
                        st.write("https://damg7245-s3-storage.s3.amazonaws.com/" + st.session_state.searched_filename)
                    else:
                        pass  
                elif copyFileToBucketRes['status_code'] == 429:
                    handleLimitExceeded()           


        ### Search by Filename

        if not st.session_state['limit_exceeded']:
            st.subheader("Search By File Name")

            searchedFilename = st.text_input("Filename", key="filename-search")

            # if 'file-searched' not in st.session_state:
            #     st.session_state['file-searched'] = False

            # if 'file-name-check' not in st.session_state:
            #     st.session_state['file-name-check'] = False

            # if 'file-link-generated' not in st.session_state:
            #     st.session_state['file-link-generated'] = False

            def handleSearchedFileName():
                
                print(st.session_state['file-searched'])
                if st.session_state['file-searched']:
                    pattern = r"^OR_ABI-L1b-RadC-M6C\d{2}_G\d+_s20\d{12}_e20\d{12}_c20\d{12}\.nc$"

                    if re.match(pattern, st.session_state['file-searched']):
                        st.session_state['file-name-check'] = True
        

                        get_goes_by_filename_res = get_goes_by_filename(st.session_state.logged_in_user["token"] ,st.session_state['file-searched'])

                        if get_goes_by_filename_res['status_code'] == 200:

                            aws_file_link = get_goes_by_filename_res['message']
                            print("AWS FIle Link", aws_file_link)
                            if aws_file_link:
                                prefix = "https://damg7245-s3-storage.s3.amazonaws.com/"
                                st.session_state['file-link-generated'] = prefix + aws_file_link
                                st.session_state['filename-search']= ""
                            else:
                                st.error('No such file exists!', icon = "⚠️")
                                st.session_state['file-searched']= ""
                                st.session_state['filename-search']= ""
                                st.session_state['file-link-generated'] = ""
                        
                        elif get_goes_by_filename_res['status_code'] == 429:
                            handleLimitExceeded()

                    else:
                        print(re.match(pattern, st.session_state['file-searched']) )
                        st.error('Provide proper file name', icon = "⚠️")
                        st.session_state['file-searched']= ""
                        st.session_state['filename-search']= ""
                        st.session_state['file-link-generated'] = ""


            if not searchedFilename == "":
                st.session_state['file-searched'] = searchedFilename
                st.button("Generate File Link", on_click = handleSearchedFileName)


            if st.session_state['file-name-check']:
                st.write("File name")
                st.write(st.session_state['file-searched'])

            if st.session_state['file-link-generated']:
                st.write(st.session_state['file-link-generated'])
    
    else:
        st.write("Limit Exceeded!")

def handleLimitExceeded():
    st.header("API calls limit exceeded!")
    st.session_state['limit_exceeded'] = True

# if st.session_state['limit_exceeded']:
#     handleLimitExceeded()

if __name__ == "__main__":
    if st.session_state['login_success']:
        print(st.session_state['limit_exceeded'])
        if st.session_state['limit_exceeded']:
            handleLimitExceeded()
        else:
            main()
    else:
        st.write('Please login to access this page!')




