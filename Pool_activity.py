import pandas as pd


# dateparse = lambda dates: pd.datetime.strptime(dates,'%d/%m/%Y %I:%M:%S %p')
# df = pd.read_csv('GreenPool1_all_account_activities.csv', parse_dates=['Date'], date_parser=dateparse)

df = pd.read_csv('./poolactivity.csv')
# df = df.drop('Activity', 1)
# df = df.drop('IP address', 1)
# df = df.drop('Country', 1)

# df = df[df.Activity == 'Trader Pool Runner Risk changed']
# df = df.drop('Activity', 1)
# df['date'] = pd.to_datetime(df['date'])
# print(df['date'])

# df['date'] = df['date'].strftime("%Y-%m-%d %H:%M:%S.")

# print(df['Date'])

index = []
account_pool_list = []
sport_list = []
competition_name_list = []
event_name_list = []
market_name_list = []
selection_name_list = []
old_back_riskp_list = []
new_back_riskp_list = []
old_lay_riskp_list = []
new_lay_riskp_list = []

for i in range(len(df['description'])):
    index.append(i)
    str = df['description'].iloc[i]
    # print(df['Description'].iloc[i])
    # str = 'Account: GreenPool1, Trader Pool Runner Risk (Cricket / IPL / Rajasthan Royals v Royal Challengers Bangalore / Match Odds / Royal Challengers Bangalore: (back) 0.18 -> 0.18, (lay) 0.27 -> 0.18'
    str = str[9:]
    account_pool, str = str.split('; ', 1)
    temp, str = str.split(' (', 1)
    sport, competition_name, event_name, market_name, str = str.split(' / ', 4)
    selection_name, str = str.split(': ', 1)
    temp, str = str.split('(back) ', 1)
    old_back_riskp, str = str.split(' -> ', 1)
    new_back_riskp, str = str.split(', (lay) ', 1)
    old_lay_riskp, new_lay_riskp = str.split(' -> ', 1)

    account_pool_list.append(account_pool)
    sport_list.append(sport)
    competition_name_list.append(competition_name)
    event_name_list.append(event_name)
    market_name_list.append(market_name)
    selection_name_list.append(selection_name)

    if(old_back_riskp == 'No'):
        old_back_riskp = ''
    if(new_back_riskp == 'No'):
        new_back_riskp = ''
    if (old_lay_riskp == 'No'):
        old_lay_riskp = ''
    if (new_lay_riskp == 'No'):
        new_lay_riskp = ''

    old_back_riskp_list.append(old_back_riskp)
    new_back_riskp_list.append(new_back_riskp)
    old_lay_riskp_list.append(old_lay_riskp)
    new_lay_riskp_list.append(new_lay_riskp)

df_out = pd.DataFrame({
        # 'username': df['username'],
        'username': account_pool_list,
        'date': df['date'],
        'sport': sport_list,
        'competition_name': competition_name_list,
        'event_name': event_name_list,
        'market_name': market_name_list,
        'selection_name': selection_name_list,
        'old_back_riskp': old_back_riskp_list,
        'new_back_riskp': new_back_riskp_list,
        'old_lay_riskp': old_lay_riskp_list,
        'new_lay_riskp': new_lay_riskp_list})

# df_out = df_out[df_out.competition_name == 'IPL']
# print(df_out['Date'].head(100))


# Write excel
# writer = pd.ExcelWriter('./out.csv', engine='xlsxwriter')
# df_out.to_excel(writer, sheet_name='Sheet1', index=False)


df_out.to_csv('./activity.csv')
# writer.save()

