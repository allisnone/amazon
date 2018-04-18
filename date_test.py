import pandas as pd

rng = pd.date_range('1/1/2011', periods=72, freq='H')

rng.argmax()

txt= 'D:/baiduyun/百度云同步盘/广告数据下载/自动广告数据下载/auto-keyword-report-2017-10-01-50172017500.txt'
txt_df = pd.read_table(txt,header=0,sep='\t')
print(txt_df)