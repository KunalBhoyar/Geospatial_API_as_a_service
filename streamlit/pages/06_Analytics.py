import streamlit as st
from frontendAPICalls import *
import pandas as pd
import altair as alt

if "login_success" not in st.session_state:
    st.session_state["login_success"] = False

st.header("User Analytics 📈")

def main():   

    # st.button("Refresh", on_click=handleRefresh, key="refresh_btn")
    
    if 'selected-time-frame' not in st.session_state:
        st.session_state['selected-time-frame'] = "last_hour"

    if st.session_state.logged_in_user['username'] == "admin":

        if 'selected-date-frame' not in st.session_state:
            st.session_state['selected-date-frame'] = "last_week"

        filter_range = "last_week"
        selectedTimeFrame = st.selectbox("Time frame", options=["Last Hour", "Last Day", "Last Week", "Last Month"])

        st.write(f"Count of all the successful API calls for the {selectedTimeFrame}")

        if selectedTimeFrame == "Last Hour":
            st.session_state['selected-time-frame'] = "last_hour"
        elif selectedTimeFrame == "Last Day":
            st.session_state['selected-time-frame'] = "last_day"
        elif selectedTimeFrame == "Last Week":
            st.session_state['selected-time-frame'] = "last_week"
        elif selectedTimeFrame == "Last Month":
            st.session_state['selected-time-frame'] = "last_month"

        user_api_success_dict = getAPISuccessCount(st.session_state.logged_in_user['token'],
                                st.session_state.logged_in_user['username'],
                                st.session_state['selected-time-frame'])

        user_all_success_api_hits_df = pd.DataFrame(list(user_api_success_dict['response'].items()), columns=['APIs', 'Count'])

        # print(user_all_success_api_hits_df)

        st.bar_chart(data=user_all_success_api_hits_df,x="APIs", y="Count", width=4, height=400)

        user_api_failure_dict = getAPIFailureCount(st.session_state.logged_in_user['token'],
                                st.session_state.logged_in_user['username'],
                                st.session_state['selected-time-frame'])


        user_all_failure_api_hits_df = pd.DataFrame(list(user_api_failure_dict['response'].items()), columns=['APIs', 'Count'])

        # print(user_all_success_api_hits_df)

        st.write(f"Count of all the API calls with status not equal to 200 OK for the {selectedTimeFrame}")

        st.bar_chart(data=user_all_failure_api_hits_df,x="APIs", y="Count", width=4, height=400)

        selectedDateFrame = st.selectbox("Time frame", options=["Last Week", "Last Month"])

        user_all_success_api_hits_by_date_dict = getAPISuccessCountByDate(st.session_state.logged_in_user['token'],
                                st.session_state.logged_in_user['username'],
                                st.session_state['selected-date-frame'])
        
        user_all_success_api_hits_by_date_df = pd.DataFrame(list(user_all_success_api_hits_by_date_dict['response'].items()), columns=['Date', 'Count'])

        st.write(f"Count of all the successful API calls by day for the {selectedDateFrame}")

        st.bar_chart(data=user_all_success_api_hits_by_date_df,x="Date", y="Count", width=4, height=400)

        user_all_failed_api_hits_by_date_dict = getAPIFailedCountByDate(st.session_state.logged_in_user['token'],
                                st.session_state.logged_in_user['username'],
                                st.session_state['selected-date-frame'])
        
        user_all_failed_api_hits_by_date_df = pd.DataFrame(list(user_all_failed_api_hits_by_date_dict['response'].items()), columns=['Date', 'Count'])

        st.write(f"Count of all the API calls by day with status not equal to 200 OK for the {selectedDateFrame}")

        st.bar_chart(data=user_all_failed_api_hits_by_date_df,x="Date", y="Count", width=4, height=400)

        ## Pie chart 
        if len(user_all_success_api_hits_df) > 0 and len(user_all_failure_api_hits_df):
            
            all_user_api_hits_df = pd.concat([user_all_success_api_hits_df, user_all_failure_api_hits_df], ignore_index=True)

            percentage_values = {}

            total_api_calls = all_user_api_hits_df['Count'].values.sum()

            for index, row in all_user_api_hits_df.iterrows():
                print(row["APIs"], row['Count'])
                if row['APIs'] in percentage_values:
                    percentage_values[row['APIs']] += (row['Count'] / total_api_calls) * 100 
                else:
                    percentage_values[row['APIs']] = (row['Count'] / total_api_calls) * 100 

            percentage_values_df = pd.DataFrame(list(percentage_values.items()), columns=['APIs', 'Percentage'])


            st.write("Percentage chart of all the API calls")

            pie_chart = alt.Chart(percentage_values_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="Percentage", type="quantitative"),
                color=alt.Color(field="APIs", type="nominal"),
            )

            st.altair_chart(pie_chart)

            total_pass_fail_calls = user_all_success_api_hits_df['Count'].values.sum() + user_all_failure_api_hits_df['Count'].values.sum()

            fail_pass_percentage_dict = {
                "APIs": ["Pass", "Fail"],
                "Count": [(user_all_success_api_hits_df['Count'].values.sum() / total_pass_fail_calls)*100, (user_all_failure_api_hits_df['Count'].values.sum() / total_pass_fail_calls) * 100]
            }

            fail_pass_percentage_df = pd.DataFrame(fail_pass_percentage_dict)
            
            st.write("Percentage chart of all the successful/ unsuccessful API calls")

            pass_fail_pie_chart = alt.Chart(fail_pass_percentage_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="Count", type="quantitative"),
                color=alt.Color(field="APIs", type="nominal"),
            )

            st.altair_chart(pass_fail_pie_chart)

    else:

        filter_range = "last_week"
        selectedTimeFrame = st.selectbox("Time frame", options=["Last Hour", "Last Day", "Last Week", "Last Month"])

        st.write(f"Count of all the successful API calls for the {selectedTimeFrame}")

        if selectedTimeFrame == "Last Hour":
            st.session_state['selected-time-frame'] = "last_hour"
        elif selectedTimeFrame == "Last Day":
            st.session_state['selected-time-frame'] = "last_day"
        elif selectedTimeFrame == "Last Week":
            st.session_state['selected-time-frame'] = "last_week"
        elif selectedTimeFrame == "Last Month":
            st.session_state['selected-time-frame'] = "last_month"

        user_api_success_dict = getAPISuccessCount(st.session_state.logged_in_user['token'],
                                st.session_state.logged_in_user['username'],
                                st.session_state['selected-time-frame'])

        user_all_success_api_hits_df = pd.DataFrame(list(user_api_success_dict['response'].items()), columns=['APIs', 'Count'])

        # print(user_all_success_api_hits_df)

        st.bar_chart(data=user_all_success_api_hits_df,x="APIs", y="Count", width=4, height=400)

        user_api_failure_dict = getAPIFailureCount(st.session_state.logged_in_user['token'],
                                st.session_state.logged_in_user['username'],
                                st.session_state['selected-time-frame'])


        user_all_failure_api_hits_df = pd.DataFrame(list(user_api_failure_dict['response'].items()), columns=['APIs', 'Count'])

        st.write(f"Count of all the API calls with status not equal to 200 OK for the {selectedTimeFrame}")

        st.bar_chart(data=user_all_failure_api_hits_df,x="APIs", y="Count", width=4, height=400)

        ## Pie chart 
        if len(user_all_success_api_hits_df) > 0 or len(user_all_failure_api_hits_df) > 0:
            
            all_user_api_hits_df = pd.concat([user_all_success_api_hits_df, user_all_failure_api_hits_df], ignore_index=True)

            percentage_values = {}

            total_api_calls = all_user_api_hits_df['Count'].values.sum()

            for index, row in all_user_api_hits_df.iterrows():
                print(row["APIs"], row['Count'])
                if row['APIs'] in percentage_values:
                    percentage_values[row['APIs']] += (row['Count'] / total_api_calls) * 100 
                else:
                    percentage_values[row['APIs']] = (row['Count'] / total_api_calls) * 100 

            percentage_values_df = pd.DataFrame(list(percentage_values.items()), columns=['APIs', 'Percentage'])


            st.write("Percentage chart of all the API calls")

            pie_chart = alt.Chart(percentage_values_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="Percentage", type="quantitative"),
                color=alt.Color(field="APIs", type="nominal"),
            )

            st.altair_chart(pie_chart)

            total_pass_fail_calls = user_all_success_api_hits_df['Count'].values.sum() + user_all_failure_api_hits_df['Count'].values.sum()

            fail_pass_percentage_dict = {
                "APIs": ["Pass", "Fail"],
                "Count": [(user_all_success_api_hits_df['Count'].values.sum() / total_pass_fail_calls)*100, (user_all_failure_api_hits_df['Count'].values.sum() / total_pass_fail_calls) * 100]
            }

            fail_pass_percentage_df = pd.DataFrame(fail_pass_percentage_dict)
            
            st.write("Percentage chart of all the successful/ unsuccessful API calls")

            pass_fail_pie_chart = alt.Chart(fail_pass_percentage_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="Count", type="quantitative"),
                color=alt.Color(field="APIs", type="nominal"),
            )

            st.altair_chart(pass_fail_pie_chart)




def handleRefresh():
    main()

if __name__ == "__main__":
    if st.session_state.login_success:
        main()
    else:
        st.write('Please login to access this page!')


    