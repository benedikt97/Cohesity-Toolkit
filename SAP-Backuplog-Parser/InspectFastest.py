from shelve import DbfilenameShelf
from syslog import LOG_DEBUG
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import sys
import os
import multiprocessing
from matplotlib import rcParams



if __name__ == '__main__':
    files = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith('fastest.csv')]
    df = pd.concat(map(pd.read_csv, files), ignore_index=True)
    df.sort_values(by='Name', ignore_index=True, inplace=True)
    print(df)
    df.to_csv('summary.csv')


    l_db =[]
    l_count=[]
    l_ave=[] 
    l_values =[] 
    ctr = 0 
    dbname = 0
    summbs = 0
    valuestr = ''

    for indexdf in range(len(df.index)):
        if indexdf == 0:
            pass
        else:
            if str(df.at[indexdf, 'Name']) == str(df.at[indexdf-1, 'Name']):
                if ctr == 0:
                    ctr = 2
                    summbs += df.at[indexdf, 'MB/s'] + df.at[indexdf-1, 'MB/s']
                    valuestr = (str(df.at[indexdf-1, 'MB/s']) + ' + ')
                    valuestr += (str(df.at[indexdf, 'MB/s']) + ' + ')
                else:
                    ctr += 1 
                    summbs += df.at[indexdf, 'MB/s'] 
                    valuestr += (str(df.at[indexdf, 'MB/s']) + ' + ')
            else:
                if ctr != 0:
                    l_db.append(str(df.at[indexdf-1, 'Name']))
                    l_count.append(ctr)
                    l_ave.append(summbs/ctr)
                    l_values.append(valuestr)
                    ctr = 0
                    summbs = 0
   
    
    temp_dict = {'DB' : l_db, 'Count' : l_count, 'AveMB/s' : l_ave, 'Values' : l_values}
    df1 = pd.DataFrame(temp_dict)
    df1.sort_values(by='AveMB/s', ignore_index=True, inplace=True, ascending=False)
    print(df1.head(30)) 
                


    sns.set_theme(style="ticks")

    # Initialize the figure with a logarithmic x axis
    f, ax = plt.subplots(figsize=(7, 6))

    # Plot the orbital period with horizontal boxes
    #sns.boxplot(x="AveMB/s", y="DB", data=df1,
    #            whis=[0, 100], width=.6, palette="vlag")

    ## Add in points to show each observation
    sns.stripplot(x="MB/s", y="DB", data=df,
                size=4, color=".3", linewidth=0)

    # Tweak the visual presentation
    ax.xaxis.grid(True)
    ax.set(ylabel="")
    sns.despine(trim=True, left=True)
    plt.savefig('Summary.png', dpi=1000, bbox_inches='tight')

    sheetname = 'FastDBs-excel.xlsx' 
    with pd.ExcelWriter(sheetname) as writer:
        df1.to_excel(writer, sheet_name='Sheet_name_1')
    print('Done')