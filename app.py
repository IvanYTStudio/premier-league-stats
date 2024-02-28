import streamlit as st
import pandas as pd

#Data source: https://www.kaggle.com/datasets/nico2890/football-10-years-of-epl-with-goal-timing?resource=download

file_1 = 'goal_time_epl.parquet'
file_2 = 'results_epl.parquet'

goal_times = pd.read_parquet(file_1)
results = pd.read_parquet(file_2)

teams = results['HomeTeam'].unique()
teams.sort()

goal_times.rename(columns={'id':'Game_ID'}, inplace=True)
results.rename(columns={'id':'Game_ID'}, inplace=True)

results_2 = results
results_2.loc[results_2['FTHG'] > results_2['FTAG'], 'Result'] = 1
results_2.loc[results_2['FTHG'] < results_2['FTAG'], 'Result'] = 2
results_2.loc[results_2['FTHG'] == results_2['FTAG'], 'Result'] = 0

complete_results = results.merge(goal_times,on='Game_ID').reset_index(drop=True)
penalties = complete_results[complete_results['Penalty'] == 1].reset_index(drop=True)

@st.cache_data
def most_goals(goal_type="Scored", ground="All", rows=100):
    if goal_type == 'Scored':
        if ground == 'All':
            club_goals = pd.concat([
                results[['HomeTeam', 'FTHG']].rename(columns={'HomeTeam':'Team', 'FTHG':'Goals'}),
                results[['AwayTeam', 'FTAG']].rename(columns={'AwayTeam':'Team', 'FTAG':'Goals'})
            ])
        elif ground == 'Home':
            club_goals = results[['HomeTeam', 'FTHG']].rename(columns={'HomeTeam':'Team', 'FTHG':'Goals'})
        else:
            club_goals = results[['AwayTeam', 'FTAG']].rename(columns={'AwayTeam':'Team', 'FTAG':'Goals'})
    else:
        if ground == 'All':
            club_goals = pd.concat([
                results[['HomeTeam', 'FTAG']].rename(columns={'HomeTeam':'Team', 'FTAG':'Goals'}),
                results[['AwayTeam', 'FTHG']].rename(columns={'AwayTeam':'Team', 'FTHG':'Goals'})
            ])
        elif ground == 'Home':
            club_goals = results[['HomeTeam', 'FTAG']].rename(columns={'HomeTeam':'Team', 'FTAG':'Goals'})
        else:
            club_goals = results[['AwayTeam', 'FTHG']].rename(columns={'AwayTeam':'Team', 'FTHG':'Goals'})

    club_goals = club_goals.groupby('Team').sum().sort_values(by='Goals', ascending=False)
    if rows == 'All':
        return club_goals.head(100)
    else:
        return club_goals.head(rows)

@st.cache_data
def most_wins_draws_loses(result_type='Wins', ground='All', team='All', rows=100):
    if team=='All':
        if result_type == 'Wins':
            if ground == 'All':
                result_games = (results[(results['Result'] == 1)].rename(columns={'HomeTeam':'Team'}).groupby('Team').count()['Game_ID'] + results[(results['Result'] == 2)].rename(columns={'AwayTeam':'Team'}).groupby('Team').count()['Game_ID']).sort_values(ascending=False)
            elif ground =='Home':
                result_games = results[(results['Result'] == 1)].groupby('HomeTeam').count()['Game_ID'].sort_values(ascending=False)
            else:
                result_games = results[(results['Result'] == 2)].groupby('AwayTeam').count()['Game_ID'].sort_values(ascending=False)
        elif result_type == "Loses":
            if ground == 'All':
                result_games = (results[(results['Result'] == 1)].rename(columns={'AwayTeam':'Team'}).groupby('Team').count()['Game_ID'] + results[(results['Result'] == 2)].rename(columns={'HomeTeam':'Team'}).groupby('Team').count()['Game_ID']).sort_values(ascending=False)
            elif ground =='Home':
                result_games = results[(results['Result'] == 2)].groupby('HomeTeam').count()['Game_ID'].sort_values(ascending=False)
            else:
                result_games = results[(results['Result'] == 1)].groupby('AwayTeam').count()['Game_ID'].sort_values(ascending=False)
        else:
            if ground == 'All':
                result_games = (results[(results['Result'] == 0)].rename(columns={'HomeTeam':'Team'}).groupby('Team').count()['Game_ID'] + results[(results['Result'] == 0)].rename(columns={'AwayTeam':'Team'}).groupby('Team').count()['Game_ID']).sort_values(ascending=False)
            elif ground == 'Home':
                result_games = results[(results['Result'] == 0)].groupby('HomeTeam').count()['Game_ID'].sort_values(ascending=False)
            else:
                result_games = results[(results['Result'] == 0)].groupby('AwayTeam').count()['Game_ID'].sort_values(ascending=False)
    else:
        if result_type == 'Wins':
            if ground == 'All':
                result_games = (results[(results['Result'] == 1) & (results['HomeTeam'] == team)].count()['Game_ID'] + results[(results['Result'] == 2) & (results['AwayTeam'] == team)].count()['Game_ID'])
            elif ground =='Home':
                result_games = results[(results['Result'] == 1) & (results['HomeTeam'] == team)].count()['Game_ID']
            else:
                result_games = results[(results['Result'] == 2) & (results['AwayTeam'] == team)].count()['Game_ID']
        elif result_type == "Loses":
            if ground == 'All':
                result_games = (results[(results['Result'] == 1) & (results['AwayTeam'] == team)].count()['Game_ID'] + results[(results['Result'] == 2) & (results['HomeTeam'] == team)].count()['Game_ID'])
            elif ground =='Home':
                result_games = results[(results['Result'] == 2) & (results['HomeTeam'] == team)].count()['Game_ID']
            else:
                result_games = results[(results['Result'] == 1) & (results['AwayTeam'] == team)].count()['Game_ID']
        else:
            if ground == 'All':
                result_games = (results[(results['Result'] == 0) & (results['HomeTeam'] == team)].count()['Game_ID'] + results[(results['Result'] == 0) & (results['AwayTeam'] == team)].count()['Game_ID'])
            elif ground == 'Home':
                result_games = results[(results['Result'] == 0) & (results['HomeTeam'] ==  team)].count()['Game_ID']
            else:
                result_games = results[(results['Result'] == 0) & (results['AwayTeam'] ==  team)].count()['Game_ID']
    if rows == 'All':
            return result_games.head(100)
    else:
        return result_games.head(rows)

@st.cache_data
def most_own_goals(rows=100):
    own_goals_df = complete_results[complete_results['OwnGoal'] == 1].rename(columns={'Game_ID':'Own Goals'})
    own_goals_df.loc[own_goals_df['Team'] == 'Home','Team'] = own_goals_df['AwayTeam']
    own_goals_df.loc[own_goals_df['Team'] == 'Away','Team'] = own_goals_df['HomeTeam']
    own_goals_df = own_goals_df.groupby('Team').count()['Own Goals'].sort_values(ascending=False)
    if rows == 'All':
        return own_goals_df.head(100)
    else:
        return own_goals_df.head(rows)

@st.cache_data
def penalty_results(rows=100):

    def penalty_scorer(row):
        if row['Team'] == 'Home':
            return row['HomeTeam']
        else:
            return row['AwayTeam']

    penalties['Penalties Scored'] = penalties.apply(penalty_scorer, axis=1)
    if rows == 'All':
        return penalties.groupby('Penalties Scored').count()['Game_ID'].sort_values(ascending=False).head(100)
    else:
        return penalties.groupby('Penalties Scored').count()['Game_ID'].sort_values(ascending=False).head(rows)

@st.cache_data
def penalty_percentage(rows=100):
    goals_df = most_goals(goal_type='Scored', rows=100)
    pens_df = penalty_results(rows=100)

    goals_df = goals_df.join(pens_df)
    goals_df.rename(columns={'Game_ID':'Penalties'}, inplace=True)
    goals_df['Penalties Percentage'] = goals_df['Penalties']/goals_df['Goals']*100
    goals_df.sort_values(by='Penalties Percentage', ascending=False, inplace=True)
    goals_df['Penalties Percentage'] = goals_df['Penalties Percentage'].map(lambda x: "{:.2f}%".format(x))
    if rows == 'All':
        return goals_df[['Penalties Percentage']].head(100)
    else:
        return goals_df[['Penalties Percentage']].head(rows)

st.set_page_config(page_title='PL 2011/2012 - 2022/2023 Stats', page_icon='favicon.ico', layout='wide')

with open('styles.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.title('Premier League 2011/2012 - 2022/2023', anchor='page_title')

tab1, tab2, tab3, tab4 = st.tabs(["Results", "Goals", "Own goals", "Penalties"])

with tab1:

    tab1_break_col_1, tab1_header_col1, tab1_header_col2, tab1_header_col3, tab1_break_col_2 = st.columns([1,0.5,0.5,0.5,1])
    with tab1_header_col1:
        results_type = st.selectbox(label="Result", options=['Wins', 'Draws', 'Loses'])
    with tab1_header_col2:
        results_ground = st.selectbox(label="Ground", options=['All', 'Home', 'Away'], key='result_ground')
    with tab1_header_col3:
        results_top = st.selectbox(label='Top', options=[5, 10, 20, 'All'], index=1, key='results_key')

    break_col_1, tab1_col_1, tab1_col_2, tab1_col_3, break_col_1  = st.columns([1.3,0.25,0.7,0.25,1])

    for club, result in most_wins_draws_loses(result_type=results_type, ground = results_ground, rows=results_top).iteritems():
                with tab1_col_1:
                    try:
                        st.image(f'Club logos/{club}.png', width=30)
                    except Exception as e:
                        st.image('Placeholder.png', width=30)
                    else:
                        pass
                with tab1_col_2:
                    st.text(club)
                with tab1_col_3:
                    st.text(result)


with tab2:
    tab2_break_col_1, tab2_header_col1, tab2_header_col2, tab2_header_col3, tab2_break_col_2 = st.columns([1,0.5,0.5,0.5,1])
    with tab2_header_col1:
        goals_type = st.selectbox(label="Goal type", options=['Scored', 'Conceded'])
    with tab2_header_col2:
        goals_ground = st.selectbox(label="Ground", options=['All', 'Home', 'Away'], index=1, key='goals_ground')
    with tab2_header_col3:
        goals_top = st.selectbox(label='Top', options=[5, 10, 20, 'All'], index=1, key='goals_key')
    
    break_col_1, tab2_col_1, tab2_col_2, tab2_col_3, break_col_1  = st.columns([1.3,0.25,0.7,0.25,1])

    for club, goals in most_goals(ground = goals_ground, goal_type=goals_type, rows=goals_top).iterrows():
        with tab2_col_1:
            try:
                st.image(f'Club logos/{club}.png', width=30)
            except Exception as e:
                st.image('Placeholder.png', width=30)
            else:
                pass
        with tab2_col_2:
            st.text(club)
        with tab2_col_3:
            st.text(goals.to_string().replace('Goals ',''))
    

with tab3:
    tab3_break_col_1, tab3_header_col_1, tab3_break_col_2 = st.columns([1,0.5,1])
    with tab3_header_col_1:
        own_goals_top = st.selectbox(label='Top', options=[5, 10, 20, 'All'], index=1, key='own_goals_key')
        
    break_col_1, tab3_col_1, tab3_col_2, tab3_col_3, break_col_1  = st.columns([1.3,0.25,0.7,0.25,1])

    for club, goals in most_own_goals(rows=own_goals_top).iteritems():
        with tab3_col_1:
            try:
                st.image(f'Club logos/{club}.png', width=30)
            except Exception as e:
                st.image('Placeholder.png', width=30)
            else:
                pass
        with tab3_col_2:
            st.text(club)
        with tab3_col_3:
            st.text(goals)

with tab4:
    tab4_break_col_1, tab4_header_col_1, tab4_header_col_2, tab4_break_col_2, tab4_header_col_3, tab4_header_col_4, tab4_break_col_3 = st.columns([1,0.5,0.3,0.3,0.5,0.3,1])
    with tab4_header_col_1:
        st.title('Penalties', anchor='stat_title')
    with tab4_header_col_2:
        penalties_top = st.selectbox(label='Top', options=[5, 10, 20, 'All'], index=1, key='penalties_key')
    with tab4_header_col_3:
        st.title('Penalties (%)', anchor='stat_title')
    with tab4_header_col_4:
        penalties_percentage_top = st.selectbox(label='Top', options=[5, 10, 20, 'All'], index=1,  key='penalties_percentage_key')


    break_col_1, tab4_col_1, tab4_col_2, tab4_col_3, break_col_2, tab4_col_4, tab4_col_5, tab4_col_6, break_col_3  = st.columns([1.3,0.25,0.7,0.25,0.39,0.25, 0.7, 0.25,1])

    for club, goals in penalty_results(rows=penalties_top).iteritems():
        with tab4_col_1:
            try:
                st.image(f'Club logos/{club}.png', width=30)
            except Exception as e:
                st.image('Placeholder.png', width=30)
            else:
                pass
        with tab4_col_2:
            st.text(club)
        with tab4_col_3:
            st.text(goals)

    for club, goals in penalty_percentage(rows=penalties_percentage_top).iterrows():
        with tab4_col_4:
            try:
                st.image(f'Club logos/{club}.png', width=30)
            except Exception as e:
                st.image('Placeholder.png', width=30)
            else:
                pass
        with tab4_col_5:
            st.text(club)
        with tab4_col_6:
            st.text(goals.to_string().replace('Penalties Percentage',''))