import streamlit as st
import streamlit.components.v1 as components
from frontendAPICalls import *
import re
import time

def loadStyles():
    def local_css(file_name):
      with open(file_name) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    local_css("streamlit\pages\styles\plan.css")

    components.html(
        """
    <script>
    window.addEventListener("load", (event) => {
        Array.from(window.parent.document.querySelectorAll('div[data-testid="stExpander"] div[data-testid="stMarkdownContainer"] p')).find(el => el.innerText === "Gold").parentNode.parentNode.classList.add('gold-plan');
        Array.from(window.parent.document.querySelectorAll('div[data-testid="stExpander"] div[data-testid="stMarkdownContainer"] p')).find(el => el.innerText === "Platinum").parentNode.parentNode.classList.add('platinum-plan');
    })
    console.log("test");
    </script>
    """,
        height=0,
        width=0,
    )

# st.markdown("""
#     <style>
#     .css-1vencpc {
#         display: none;
#     }
#     </style>
# """, unsafe_allow_html=True)





def main():
        USERNAME_PATTERN = r"^[a-zA-Z0-9]+$"
        PASSWORD_PATTERN = r"^[a-zA-Z0-9]+$"

        # Define session states for registration
        if 'register_username' not in st.session_state:
            st.session_state.register_username = False
            st.session_state.register_password = False
            st.session_state['register_plan'] = False

        if not st.session_state.login_success:
            st.header('Register')

            def handleRegistration():
                    
                if st.session_state.register_password and st.session_state.register_username and st.session_state.register_plan:
                    response = api_userRegistration(st.session_state.register_username, st.session_state.register_password, st.session_state.register_plan)
                    if response:
                        st.session_state.register_username = ""
                        st.session_state.register_password = ""  
                        return True          
                    else:
                        st.session_state.register_username = ""
                        st.session_state.register_password = ""
                        return False

            registerPlaceholder = st.empty()

            registerPlaceholder, col2 = st.columns(2, gap="large")

            with registerPlaceholder.form("register"):
                    st.markdown("#### Register")
                    username = st.text_input("Username")
                    password = st.text_input("Password", type="password")
                    submit = st.form_submit_button("Register") 

            def handleGoldPlan():
                st.session_state.register_plan = "gold"
            def handleFreePlan():
                st.session_state.register_plan = "free"
            def handlePlatinumPlan():
                st.session_state.register_plan = "platinum"

            with col2:
                st.header("Plans")
                with st.expander("Free"):
                    st.write("Free Tier")
                    st.write("API Call Limit: 10")
                    st.button("Select Free Tier", on_click = handleFreePlan)
                with st.expander("Gold"):
                    st.write("Gold Tier")
                    st.write("API Call Limit: 15")
                    st.button("Select Gold Tier", on_click = handleGoldPlan)
                with st.expander("Platinum"):
                    st.write("Platinum Tier")
                    st.write("API Call Limit: 20")
                    st.button("Select Platinum Tier", on_click = handlePlatinumPlan)

            if submit and st.session_state.register_plan:
                if re.match(USERNAME_PATTERN, username) and re.match(PASSWORD_PATTERN, password):
                        st.session_state.register_username = username
                        st.session_state.register_password = password
                        registerPlaceholder.empty()
                        if handleRegistration():
                            st.success("Registered successfully!")
                            st.success("Login to access home page!")
                            time.sleep(1)
                            st.markdown(f'<meta http-equiv="refresh" content="0; url=/" />', unsafe_allow_html=True)
                        else:
                             st.write("Please register again!")
                        # st.markdown(f'<meta http-equiv="refresh" content="0; url=/" />', unsafe_allow_html=True)
                else:
                    st.error("Please provide proper username and password")
            elif submit:
                st.error("Please select a plan")

def afterRegistrationPage():
    st.write('Access the sidebar tabs to navigate to other section!')
    st.button('Logout', on_click=handleLogout)

if "login_success" not in st.session_state:
    st.session_state["login_success"] = False
     
if __name__ == "__main__":
    def handleLogout():
        st.session_state.login_success = False

    if st.session_state.login_success:
        afterRegistrationPage()
    else:
        loadStyles()
        main()

# if submit and re.match(USERNAME_PATTERN, username) and re.match(PASSWORD_PATTERN, password) and st.session_state.register_plan:
#       st.session_state.register_username = username
#       st.session_state.register_password = password
#       registerPlaceholder.empty()
#       st.success("Registered successfully")
# else:
#       pass



# if submit and email == actual_email and password == actual_password:
#     placeholder.empty()
#     st.success("Login successful")
# elif submit and email != actual_email and password != actual_password:
#     st.error("Login failed")
# else:
#     pass

