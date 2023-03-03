import streamlit as st
from frontendAPICalls import *
import re
import time


#Hide the sidebar for login/register page

# st.markdown("""
#     <style>
#     .css-1vencpc {
#         display: none;
#     }
#     </style>
# """, unsafe_allow_html=True)

if "login_success" not in st.session_state:
    st.session_state['login_success'] = False
    st.session_state['login_error'] = False
    st.session_state.logged_in_user = False

def main():

    USERNAME_PATTERN = r"^[a-zA-Z0-9]+$"
    PASSWORD_PATTERN = r"^[a-zA-Z0-9]+$"

    st.header('GOES and NEXRAD satellite datasets exploration app 📡')

    placeholder = st.empty()

    with placeholder.form("login"):
        st.markdown("#### Login")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

    if submit and re.match(USERNAME_PATTERN, username) and re.match(PASSWORD_PATTERN, password):
        response = api_userLogin(username, password)
        if response['status'] == 200:
            placeholder.empty()
            st.session_state.login_success = True
            st.session_state.login_error = False
            st.session_state.logged_in_user = {
                "username": username,
                "password": password,
                "token": response['response']
            }
            st.success("Login successful")
            with st.spinner(''):
                time.sleep(2)
                # st.success().empty()  
            afterLoginPage()
            # st.markdown(f'<meta http-equiv="refresh" content="0; url=/UserHome" />', unsafe_allow_html=True)
        else:
            st.write(response['response'])
    elif submit and not re.match(USERNAME_PATTERN, username) and not re.match(PASSWORD_PATTERN, password):
        st.error("Login failed")
        st.session_state.login_success = False
        st.session_state.login_error = True
    else:
        pass

    def handleRegisterPage():
        st.markdown(f'<meta http-equiv="refresh" content="0; url=/Register" />', unsafe_allow_html=True)

    if not st.session_state.login_success:
        col1, col2 = st.columns(2)
        with col1:
            st.write("New user register!")
        with col2:
            st.button('Register', on_click=handleRegisterPage)

def afterLoginPage():
    def handleLogout():
        st.session_state.logged_in = False
        st.session_state.login_success = False
    
    st.write('Access the sidebar tabs to navigate to other section!')
    st.button('Logout', on_click=handleLogout)


if __name__ == "__main__":
    def handleLogout():
        st.session_state.login_success = False
        st.session_state.login_error = False
        st.session_state.logged_in_user = False

    if st.session_state.login_success:
        afterLoginPage()
    else:
        main()