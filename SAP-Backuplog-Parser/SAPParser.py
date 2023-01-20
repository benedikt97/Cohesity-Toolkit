#from tkinter import Y
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import sys
import os
import multiprocessing
from matplotlib import rcParams


def sapplot(path):
    mstreshhold = 128
    
    ### Parse CSV to Dataframe
    df = pd.read_csv(path, delim_whitespace=True, header=0, skip_blank_lines=True, on_bad_lines='skip')
    print(df.head(2))
    ### Uncomment for Excel Output
    #sheetname = path + '-excel.xlsx' 
    #with pd.ExcelWriter(sheetname) as writer:
     #    df.to_excel(writer, sheet_name='Sheet_name_1')
    for col in df:
        try:
            df[col] = df[col].str.replace(',','.')
        except:
            pass

    ###Delete Running Jobs
    if 'Status' in df:
        for indexdf in range(len(df.index)):
            if str(df['Status'].iloc[indexdf]) != 'successful':
                df.drop(indexdf)
                #print('Removed: ' + str(indexdf) + 'in' + path + str(df['Status'].iloc[indexdf]))
            else:
                pass
            #print(str(df['Status'].iloc[indexdf]))

    ### Correct Time Columns
    df['start'] = df['Startdate'] + ' ' + df['Starttime']
    df['end'] = df['Enddate'] + ' ' + df['Endtime']
    df.drop(['Startdate', 'Starttime', 'Enddate', 'Endtime'], axis=1, inplace=True)
    df['start'] = pd.to_datetime(df['start'])
    df['end'] = pd.to_datetime(df['end'])
  
    ### Edit Table Values
    print(path +' - Edit Table Values')
    df['Location'] = 'Unknown'
    if 'Host' in df:
        #df['Host'] = df['Host'].str.replace('ideweiiss', 'ss')
        df['DB'] = df['Name'].str.slice(0,1)
        df['SizeGB'] = pd.to_numeric(df['SizeGB'], errors='coerce')
        df['MB/s'] = pd.to_numeric(df['MB/s'], errors='coerce')
        df['Zeit'] = pd.to_numeric(df['Zeit'], errors='coerce')
    else:
        df['DB'] = df['System']
        df['Name'] = df['System']
        df['Host'] = df['System']
        df['DB'] = df['DB'].str.slice(0,1)
        df['SizeGB'] = pd.to_numeric(df['SizeGB'], errors='coerce')
        df['MB/s'] = pd.to_numeric(df['MB/s'], errors='coerce')
        df['Zeit'] = pd.to_numeric(df['Zeit'], errors='coerce')
        
    ### Add Table Values
    df['Location'] = 'Unknown'
    location = []
    for indexdf in range(len(df.index)):
        if str(df['Host'].iloc[indexdf]).find('idewei'):
            #df['Location'].iloc[indexdf] = 'Weil'
            df.at[indexdf, 'Location'] = 'Weil'
        else:
            #df['Location'].iloc[indexdf] = 'Hana'
            df.at[indexdf, 'Location'] = 'Hana'
    multistreaming = []
    df['Multistreaming'] = 'Unknown'
    for indexdf in range(len(df.index)):
        if (df['SizeGB'].iloc[indexdf]) < mstreshhold:
            #df['Multistreaming'].iloc[indexdf] = 'No'
            df.at[indexdf, 'Multistreaming'] = 'No'
        else:
            #df['Multistreaming'].iloc[indexdf] = 'Yes'
            df.at[indexdf, 'Multistreaming'] = 'Yes'

    
    ###Sort dataframe
    df.sort_values(by='MB/s', inplace=True, ascending=False)
    df.to_csv(path.replace('.txt', '') + '-fastest.csv')

    
    ###Sort dataframe
    df.sort_values(by='start', inplace=True)

 

    ### Create Dataframe in Timedomain
    print(path + ' - Creata Dataframe in Timedomain')
    dictt = {'Time':[], 'Streams':[], 'IO':[]}
    dft = pd.DataFrame(dictt)
    start_date = df.at[0, 'start']#df['start'].iloc[0]
    act_date = df.at[0, 'start']#df['start'].iloc[0]
    end_date = df['end'].iloc[-1]
    delta = datetime.timedelta(minutes=1)
    threads = 0
    indexdft = 0
    indexdf = 0
    iosum = 0
    while act_date <= end_date:
        indexdft += 1
        for indexdf in range(len(df.index)):
            if act_date < df.at[indexdf ,'end'] and act_date > df.at[indexdf, 'start']:
                threads += 1
                iosum += df.at[indexdf, 'MB/s']
        dft.loc[indexdft] = [act_date] + [threads] + [iosum]
        threads = 0
        iosum = 0
        act_date += delta
 


    ###Create a visualization
    print(path + ' - Plot Visualization')

    ### Location Plot
    if 'Host' in df:
            sns.set_theme()
            sns.set_context("poster", font_scale=0.3)
            plottitle=(path.replace('.txt', '') + ' - Total Size= ' + str(df['SizeGB'].sum()) + 'GB - Duration= ' + str(end_date-start_date)+ '-Bubbles - Hosts -')
            print(plottitle)
            g = sns.relplot(x="start", y="MB/s", hue="Location", size="SizeGB",
                alpha=.5, palette="bright",            
                data=df, aspect=1.7).set(title=plottitle)
            plt.savefig('graphs/' + path.replace('.txt', '')+ ' - Host-Bubbles.png', dpi=1000, bbox_inches='tight')
            plt.clf()

    ### Multistreaming Plot
    sns.set_theme()
    sns.set_context("poster", font_scale=0.3)
    plottitle=(path.replace('.txt', '') + ' - Total Size= ' + str(df['SizeGB'].sum()) + 'GB - Duration= ' + str(end_date-start_date)+ '-Bubbles - Multistreaming -' )
    print(plottitle)
    g = sns.relplot(x="start", y="MB/s", hue="Multistreaming", size="SizeGB",
        alpha=.5, palette="bright",            
        data=df, aspect=1.7).set(title=plottitle)
    plt.savefig('graphs/' + path.replace('.txt', '')+ ' - Multistreaming.png', dpi=1000, bbox_inches='tight')
    plt.clf()

    ### DB Plot
    sns.set_theme()
    sns.set_context("poster", font_scale=0.3)
    plottitle=(path.replace('.txt', '') + ' - Total Size= ' + str(df['SizeGB'].sum()) + 'GB - Duration= ' + str(end_date-start_date)+ '-Bubbles - DB' )
    print(plottitle)
    g = sns.relplot(x="start", y="MB/s", hue="DB", size="SizeGB",
                alpha=.5, palette="bright",            
                data=df, aspect=1.7).set(title=plottitle)
    plt.savefig('graphs/' + path.replace('.txt', '')+ ' - DB-Bubbles.png', dpi=1000, bbox_inches='tight')
    plt.clf()

    plottitle=(path.replace('.txt', '') + ' - Total Size= ' + str(df['SizeGB'].sum()) + 'GB - Duration= ' + str(end_date-start_date)+ 'IO-Graph' )
    sns.set_theme()
    sns.set_context("poster", font_scale=0.3)
    g = sns.lineplot(x="Time", y="IO",
             data=dft).set_title(plottitle)
    plt.savefig('graphs/' + path.replace('.txt', '')+ ' - IO-graph.png', dpi=1000, bbox_inches='tight')
    plt.clf()

    plottitle=(path.replace('.txt', '') + ' - Total Size= ' + str(df['SizeGB'].sum()) + 'GB - Duration= ' + str(end_date-start_date)+ 'Hosts-Graph' )
    sns.set_theme()
    sns.set_context("poster", font_scale=0.3)
    g = sns.lineplot(x="Time", y="Streams",
             data=dft).set_title(plottitle)
    plt.savefig('graphs/' + path.replace('.txt', '')+ ' - Hosts-graph.png', dpi=1000, bbox_inches='tight')
    plt.clf()

if __name__ == '__main__':
    files = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith('.txt')]

    f_index = 0 
    while f_index < len(files):

        if len(files) > f_index:
            x1 = multiprocessing.Process(target=sapplot, args=(files[f_index],))
            x1.start()
            print('Started Thread - ' + files[f_index])
        
        if len(files) > f_index+1:
            x2 = multiprocessing.Process(target=sapplot, args=(files[f_index+1],))
            x2.start()
            print('Started Thread - ' + files[f_index+1])

        if len(files) > f_index+2:
            x3 = multiprocessing.Process(target=sapplot, args=(files[f_index+2],))
            x3.start()
            print('Started Thread - ' + files[f_index+2])

        x1.join()
        x2.join()
        x3.join()

        f_index += 3





        
