import wget
import os
import pandas as pd
from utils import init_db, load
def extract_corona_data():
    url='https://github.com/CSSEGISandData/COVID-19/raw/master/csse_covid_19_data/csse_covid_19_time_series/'
    file_names=['time_series_covid19_confirmed_global.csv',
                'time_series_covid19_deaths_global.csv',
                'time_series_covid19_recovered_global.csv']
    for file_name in file_names:
        if os.path.exists(f"data/{file_name}"):
            os.remove(f"data/{file_name}")
        wget.download(url+file_name,f"data/{file_name}")

def transforming_corona_data():

    file_names=['time_series_covid19_confirmed_global.csv',
                'time_series_covid19_deaths_global.csv',
                'time_series_covid19_recovered_global.csv']

    data=[]
    for file_name in file_names:
        print(file_name)
        df=pd.read_csv(f"data/{file_name}")

        df=df.groupby("Country/Region").sum().reset_index()
        df.drop(columns=['Lat','Long'],inplace=True)
        df.iloc[:,1:]=df.iloc[:,1:].diff(axis=1)
        df=df.melt(id_vars=['Country/Region'],var_name='date',value_name='Number')
        df.dropna(inplace=True)
        df.reset_index(drop=True,inplace=True)
        df_date=df['date'].str.split('/', expand=True) #Month/Day/Year
        df['date']=df_date[0]+'/'+df_date[1]+'/20'+df_date[2]
        df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
        df.columns=['country', 'date', file_name.split('_')[3]]
        df[file_name.split('_')[3]]=df[file_name.split('_')[3]].astype(int)
        data.append(df)
    merged_df=pd.merge(data[0],data[1],on=['country','date'])
    merged_df=pd.merge(merged_df,data[2],on=['country','date'])
    return merged_df
def load_corona_data(merged_df):
    init_db()
    load(merged_df,'corona')

if __name__ == '__main__':
    extract_corona_data()
    merged_df=transforming_corona_data()
    load_corona_data(merged_df)

