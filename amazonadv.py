# -*- coding:utf-8 -*-
import datetime,time,os,io,glob
import pandas as pd
import fileOperation as fo
import sendMail as sm
import numpy as np
#pip install openpyxl
"""
ROOT_DIR="/Xperia Z2/内部存储设备/BarScannerExcels/"
FILE_PROFIX="BarScan"
FILE_TPY=".xls"
ROOT_DIR="C:/BarScannerExcels/"
"""

def search_all_files_return_by_time_reversed(path, reverse=True):
    return sorted(glob.glob(os.path.join(path, '*')), 
                  key=lambda x: time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(os.path.getmtime(x))), reverse=reverse)

def get_all_scan_file_by_key(given_dir="C:/BarScannerExcels/data",key_word='BarScan',reverse=True):
    """
    根据最后修改的时间排序文件名
    """
    all_files = search_all_files_return_by_time_reversed(given_dir, reverse)
    result = []
    for i in range(0,len(all_files)):
        if key_word in all_files[i]:
            result.append(all_files[i].replace('\\','/'))
        else:
            pass
    return result


def get_all_scan_file(given_dir="C:/BarScannerExcels/data",key_word='BarScan',file_type='.xls'):
    """
        获取特定目录的最新扫描文件名，按字符顺序降序排序
    """
    """
    :param given_dir: str type, given DIR
    :return: str type, all file name in the given DIR
    """
    all_file_names=[]
    #print(given_dir)
    #print(os.walk(given_dir))
    for root,dirs,allfiles in os.walk(given_dir):
        if root==given_dir:
            for file in allfiles:
                if key_word in file and file_type in file:
                    all_file_names.append(given_dir+file)
    all_file_names=sorted(all_file_names,reverse=True)
    return all_file_names


def get_scan_data(column=[],given_file_name=None,given_dir="C:/BarScannerExcels/",key_word='BarScan',file_type='.xls'):
    """
        获取需要的更新数据
    """
    """
    :param given_file_name: str type, given file name
    :param given_dir: str type, given DIR
    :return: dataframe type, scanned data
    """
    bar_data_df=pd.DataFrame(data={},columns=column)
    allfiles=get_all_scan_file()
    file_name=''
    if given_file_name:
        file_name=given_file_name
        if '.xls' in file_name:
            file_type='.xls'
        elif '.xlsx' in file_name:
            file_type = '.xlsx'
        elif '.csv' in file_name:
            file_type = '.csv'
        elif '.txt' in file_name:
            file_type = '.txt'
        else:
            print('Invalid file type for file: %s' % given_file_name)
    else:
        allfiles=get_all_scan_file(given_dir,key_word,file_type)
        if allfiles:
            file_name=allfiles[0]
        else:
            print('No any file in DIR: %s; Please give the write file name or DIR!!' % given_dir)
    if file_name and (file_type=='.xls' or file_type=='.xlsx'):
        try: 
            raw_bar_data_df=pd.read_excel(file_name,header=0,encoding='gb2312')
            #df_0=pd.read_csv(file_name,names=column_list, header=0,encoding='gb2312')#'utf-8')   #for python3
            #print(file_name)
            #print(raw_bar_data_df)
            if column:
                bar_data_df=raw_bar_data_df[column]
            else:
                bar_data_df = raw_bar_data_df
            #print(bar_data_df)
            #bar_data_df=bar_data_df.set_index(' NTAsset')
        except:
            print('Read Excel file ERROR: ...' % file_name)
            pass
    elif file_name and file_type=='.csv' :
        try: 
            raw_bar_data_df=pd.read_csv(file_name,names=column,header=1,encoding='gb2312')
            #df_0=pd.read_csv(file_name,names=column_list, header=0,encoding='gb2312')#'utf-8')   #for python3
            #print(file_name)
            if column:
                bar_data_df = raw_bar_data_df[column]
            else:
                bar_data_df = raw_bar_data_df
            #bar_data_df=bar_data_df.set_index(' NTAsset')
        except:
            #print('file: %s does not exist...' % file_name)
            print('Read CSV file ERROR: ...' % file_name)
            #pass
    elif file_name and file_type=='.txt':
        #try:
        if True: 
            #txt_df = pd.read_table(file_name,header=1,encoding='gb2312',sep='\t')
            #print(txt_df)
            try:
                raw_bar_data_df = pd.read_table(file_name,header=0,sep='\t')
            except:
                raw_bar_data_df = pd.read_table(file_name,header=0,sep='\t',encoding='gb2312')  #error_bad_lines=False)#sep='delimiter',skiprows=2)
            if column and not raw_bar_data_df.empty():
                bar_data_df = raw_bar_data_df[column]
            else:
                bar_data_df = raw_bar_data_df
            """
            file_object = open(file_name, encoding='UTF-8')
            all_the_text = file_object.read()#.decode('utf-8')#decode('gb2312')
            #print('all_the_text=\n',all_the_text)
            file_object.close()
            all_the_text_csv = all_the_text.replace('\t',',')
            print('all_the_text_csv=\n',all_the_text_csv)
            file_name_csv = file_name[:-4] + '.csv'
            print('file_name_csv=',file_name_csv)
            file_obj_w = open(file_name_csv,'w')
            file_obj_w.write(all_the_text_csv)
            #file_object.writelines(list_of_text_strings)
            file_obj_w.close()
            column = all_the_text_csv.split('\n')[0].split(',')
            print('column=',column)
            try:
                #raw_bar_data_df=pd.read_csv(file_name_csv,header=1)#,encoding='gb2312'
                raw_bar_data_df=pd.read_csv(file_name_csv,names=column,header=1)#,encoding='gb2312')
            except:
                #raw_bar_data_df=pd.read_csv(file_name_csv,sep=None)
                raw_bar_data_df=pd.read_csv(file_name_csv,names=column,header=1,encoding='gb2312', error_bad_lines=False)#sep='delimiter',skiprows=2)#
            #all_lines_io = io.StringIO(all_the_text)
            #print('all_lines_io=',all_lines_io)
            """
            """
            file_object = open(file_name)
            all_lines = file_object.readlines( )
            #3print(all_lines)
            column = all_lines[0].split('\t')
            print('column=',column)
            """
            #raw_bar_data_df = pd.read_fwf(all_lines_io,header=None)#encoding='gb2312')
            #raw_bar_data_df = pd.read_fwf(all_the_text,header=None)#encoding='gb2312')v
            #raw_bar_data_df=pd.read_csv(file_name,header=0,encoding='gb2312')
            #df_0=pd.read_csv(file_name,names=column_list, header=0,encoding='gb2312')#'utf-8')   #for python3
            #raw_bar_data_df=pd.read_csv(file_name,names=column,header=1,encoding='gb2312')
            ###raw_bar_data_df = pd.read_table(file_name,names=column,header=0,encoding='gb2312',delim_whitespace=True,index_col=0)
            #raw_bar_data_df = pd.read_fwf(file_name,names=column,header=0,encoding='gb2312')
            #print(raw_bar_data_df)
            #print(file_name)
            #bar_data_df=raw_bar_data_df[column]
            #bar_data_df=bar_data_df.set_index(' NTAsset')
        #except:
            #print('Read TXT file ERROR:%s ...' % file_name)
    else:
        #print('Invalid file type for file: %s' % given_file_name)
        print('Make sure you give the right file name, file type： %s or DIR: %s' % (given_file_name,given_dir))
    return bar_data_df


def compare_scan_data(new_scan_data,history_bar_data):
    """
    将更新的扫描数据和数据源比较，更新数据源并发送email给asset prime
    """
    """
    :param new_scan_data: dataframe type, scanned data
    :param history_bar_data: dataframe type, rwa data
    :return:  
    """
    update_column_list=[' GDNTAsset',' SN', ' PEC','FrameRackPosition',' SiteLocation']
    update_data=pd.DataFrame(data={},columns=update_column_list)
    updated_history_bar_data=update_data
    if new_scan_data.empty or history_bar_data.empty:
        pass
    else:
        #update_barms=new_scan_data[' NTAsset'].values.tolist()
        update_barms=new_scan_data.index.values.tolist()
        #print(history_bar_data.columns.tolist())
        #hist_barms=history_bar_data[' NTAsset'].values.tolist()
        hist_barms=history_bar_data.index.values.tolist()
        update_existing_barms=list(set(update_barms).intersection(set(hist_barms)))
        new_insert_barms=list(set(update_barms).difference(set(update_existing_barms)))
        #history_bar_data=history_bar_data.set_index(' NTAsset')
        #print(history_bar_data)
        #new_scan_data=new_scan_data.set_index(' NTAsset')
        update_data=new_scan_data[new_scan_data.index.isin(update_existing_barms)]
        new_insert_data=new_scan_data[new_scan_data.index.isin(new_insert_barms)]
        if update_existing_barms:
            #update_column_list=['FrameRackPosition']
            update_data=new_scan_data.fillna(-1)
            for bams in update_existing_barms:
                for col in update_column_list:
                    if update_data.at[bams,col] != -1 and history_bar_data.at[bams,col]!=update_data.at[bams,col]:
                        history_bar_data.at[bams,col]=update_data.at[bams,col]
            #history_bar_data=history_bar_data.drop(update_existing_barms)
        print('history_bar_data: \n',history_bar_data)
        print('new_insert_data: \n',new_insert_data)
        updated_history_bar_data=pd.concat([history_bar_data,new_insert_data],ignore_index=False)
        print('updated_history_bar_data \n',updated_history_bar_data)
        updated_history_bar_data=updated_history_bar_data.sort_index()[update_column_list]
        update_data=updated_history_bar_data[updated_history_bar_data.index.isin(update_barms)][update_column_list]
        #print(update_data[[' GDNTAsset',' SN', ' PEC','FrameRackPosition',' SiteLocation']])
        #print('Final result:')
        #print(updated_history_bar_data)
        #print('Final affected data:')
        #print(update_data)
    return update_data,updated_history_bar_data

def clear_hist_scan_files(data_dir,temp_dir,max_temp_file=20):
    new_files=get_all_scan_file(given_dir=data_dir)
    all_temp_files=get_all_scan_file(given_dir=temp_dir)
    new_move_files=list(set(new_files).difference(set(all_temp_files)))
    print('Backup scan files to temp dir: ')
    fo.copyFiles(sourceDir=data_dir, targetDir=temp_dir)
    fo.removeFileInFirstDir(targetDir=data_dir)
    new_temp_files=all_temp_files+new_move_files
    new_temp_files=sorted(new_temp_files,reverse=True)
    print('Clear and delete files to temp files.')
    for file in new_temp_files[20:]:
        if os.path.isfile(file): 
            os.remove(file)
    return new_temp_files[:20]

def consolidate_scan_data(scan_dir):
    ROOT_DIR="C:/BarScannerExcels/"
    all_files=get_all_scan_file()
    column_list=[' NTAsset',' GDNTAsset',' SN', ' PEC','FrameRackPosition',' SiteLocation']
    consolidate_bar_data_df=pd.DataFrame(data={},columns=column_list)
    latest_file_name=''
    if all_files:
        latest_file_name=all_files[0]
        print('Lastest scan file name: %s' % all_files[0])
        print('Start to conolidate scan data...')
        if len(all_files)==1:
            consolidate_bar_data_df=get_scan_data(column=column_list,given_file_name=all_files[0])
        else:
            indx=len(all_files)-1
            consolidate_bar_data_df=get_scan_data(column=column_list,given_file_name=all_files[indx])
            while indx>0:
                indx=indx-1
                new_scan_data=get_scan_data(column=column_list,given_file_name=all_files[indx])
                #print(all_files[indx])
                #print(new_scan_data)
                update_data,consolidate_bar_data_df = compare_scan_data(new_scan_data, consolidate_bar_data_df)
        clear_hist_scan_files(data_dir=ROOT_DIR+'data/', temp_dir=ROOT_DIR+'temp/')
    else:
        pass
    return consolidate_bar_data_df

def get_eqm_data():
    eqm_column_list=['Region','Test Organization','Site','Cabinet Position','Owner',
                     'Ericsson SN','BAMS ID','Asset ID','Product No','R-state',
                     'Functional Designation','Manufacturing Day','Position','Comments',
                     'Destination','Borrower','Transferred To','Category',
                     'Free Text Description','Update Date','Update User']
    eqm_data=pd.DataFrame(data={},columns=eqm_column_list)
    eqm_key_work = 'Guangzhou_EQM'
    eqm_file_type = '.csv'
    eqm_file_dir ='C:/BarScannerExcels/eqm/'
    latest_eqm_files = get_all_scan_file(given_dir=eqm_file_dir, key_word=eqm_key_work, file_type=eqm_file_type)
    latest_eqm_file=''
    if latest_eqm_files:
        latest_eqm_file = latest_eqm_files[0]
        eqm_data = get_scan_data(column=eqm_column_list,given_file_name=latest_eqm_file)
    else:
        print('There no EQM data in DIR: %s' % eqm_file_dir)
    #print(eqm_data)
    #print(latest_eqm_file)
    return eqm_data

def update_position_eqm_data(scan_data,raw_eqm_data):
    """
    将更新的扫描数据和数据源比较，更新数据源并发送email给asset prime
    """
    """
    :param new_scan_data: dataframe type, scanned data
    :param history_bar_data: dataframe type, rwa data
    :return:  
    """
    raw_eqm_data_columns = ['Region', 'Test Organization', 'Site', 'Cabinet Position', 'Owner',
                            'Ericsson SN', 'BAMS ID', 'Asset ID', 'Product No', 'R-state',
                            'Functional Designation', 'Manufacturing Day', 'Position', 'Comments',
                            'Destination', 'Borrower', 'Transferred To', 'Category',
                            'Free Text Description', 'Update Date', 'Update User']
    
    update_column_list=[' GDNTAsset',' SN', ' PEC','FrameRackPosition',' SiteLocation']
    update_data=pd.DataFrame(data={},columns=update_column_list)
    updated_history_bar_data=update_data
    if scan_data.empty or raw_eqm_data.empty:
        pass
    else:
        #update_barms=new_scan_data[' NTAsset'].values.tolist()
        update_barms=scan_data.index.values.tolist()
        #print(history_bar_data.columns.tolist())
        #hist_barms=history_bar_data[' NTAsset'].values.tolist()
        #raw_eqm_data = raw_eqm_data0
        #hist_barms=raw_eqm_data.index.values.tolist()
        hist_barms=raw_eqm_data['BAMS ID'].values.tolist()
        update_existing_barms=list(set(update_barms).intersection(set(hist_barms)))
        new_insert_barms=list(set(update_barms).difference(set(update_existing_barms)))
        #history_bar_data=history_bar_data.set_index(' NTAsset')
        #print(history_bar_data)
        #new_scan_data=new_scan_data.set_index(' NTAsset')
        update_data=scan_data[scan_data.index.isin(update_existing_barms)]
        new_insert_data=scan_data[scan_data.index.isin(new_insert_barms)]
        raw_eqm_data_temp = raw_eqm_data.set_index('BAMS ID')
        if update_existing_barms:
            #update_column_list=['FrameRackPosition']
            update_data=scan_data.fillna(-1)
            for bams in update_existing_barms:
                #for col in update_column_list:
                if update_data.at[bams,'FrameRackPosition'] != -1 and raw_eqm_data_temp.at[bams,'Cabinet Position']!=update_data.at[bams,'FrameRackPosition']:
                    raw_eqm_data_temp.at[bams,'Cabinet Position']=update_data.at[bams,'FrameRackPosition']
            #history_bar_data=history_bar_data.drop(update_existing_barms)
        print('history_bar_data: \n',raw_eqm_data)
        print('new_insert_data: \n',new_insert_data)
        updated_history_bar_data=pd.concat([raw_eqm_data,new_insert_data],ignore_index=False)
        print('updated_history_bar_data \n',updated_history_bar_data)
        updated_history_bar_data=updated_history_bar_data.sort_index()[update_column_list]
        update_data=updated_history_bar_data[updated_history_bar_data.index.isin(update_barms)][update_column_list]
        #print(update_data[[' GDNTAsset',' SN', ' PEC','FrameRackPosition',' SiteLocation']])
        #print('Final result:')
        #print(updated_history_bar_data)
        #print('Final affected data:')
        #print(update_data)
    return update_data,updated_history_bar_data
    

def get_gims_data():
    csv_file_name="C:/BarScannerExcels/GIMs_data.xls"
    raw_bar_data_df=pd.read_excel(csv_file_name,header=0,encoding='gb2312')
    need_columns = ['LABEL_ID', 'PEC', 'SN','NTASSET', 'USERGROUP']
    #print(raw_bar_data_df[need_columns].tail(20))
    #print(raw_bar_data_df.columns.values.tolist())
    return raw_bar_data_df[need_columns]

def update_eqm_sn_from_gims():
    return

def get_max_item():
    
    return


def find_new_items(new_df,old_df,keyword_columns=['Ad Group Name','Customer Search Term'],
                filter_column = 'Last Day of Impression',given_dir="D:/baiduyun/百度云同步盘/amazon新品/广告数据下载/自动广告数据下载/"):
    #file_type = '.csv'
    """
    合并最新的两个目标文件，去掉重复关键字，并判断是否是新加的关键词
    """
    """
    column_lines = 'Campaign Name    Ad Group Name    Customer Search Term    Keyword    Match Type    First Day of Impression    Last Day of Impression    Impressions    Clicks    CTR    Total Spend    Average CPC    ACoS    Currency    Orders placed within 1-week of a click    Product Sales within 1-week of a click    Conversion Rate within 1-week of a click    Same SKU units Ordered within 1-week of click    Other SKU units Ordered within 1-week of click    Same SKU units Product Sales within 1-week of click    Other SKU units Product Sales within 1-week of click'
    column=column_lines.split('    ')
    #column=column_lines.split('\t')
    print('column=',column)
    """
    if len(keyword_columns)==1:
        new_df['keywords'] = new_df[keyword_columns[0]]
        old_df['keywords'] = old_df[keyword_columns[0]]
    elif len(keyword_columns)==2:
        new_df['space'] =' '
        new_df[keyword_columns[0]] = new_df[keyword_columns[0]].astype(str)
        new_df[keyword_columns[1]] = new_df[keyword_columns[1]].astype(str)
    
        new_df['keywords'] = new_df[keyword_columns[0]] + new_df['space'] + new_df[keyword_columns[1]]
        del new_df['space']
        old_df['space'] =' '

        old_df[keyword_columns[0]] = old_df[keyword_columns[0]].astype(str)
        old_df[keyword_columns[1]] = old_df[keyword_columns[1]].astype(str)
        old_df['keywords'] = old_df[keyword_columns[0]] + old_df['space'] + old_df[keyword_columns[1]]
        del old_df['space']
    #print(old_df)
    contat_df = pd.concat([new_df, old_df],ignore_index=False)
    #contat_df.to_csv(given_dir+'contat_df.csv')
    drop_duplicate = contat_df.drop_duplicates(subset='keywords',keep=False)# keep='first', inplace=False)
    #http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop_duplicates.html
    #drop_duplicate.to_csv(given_dir+'drop_duplicate.csv')
    #f_column = 'Last Day of Impression'
    f_column = '结束日期'
    if filter_column==f_column:
        max_last_day = max(old_df[filter_column])
        drop_duplicate = drop_duplicate[drop_duplicate[filter_column]>max_last_day]
    new_df['new'] = new_df['keywords'].isin(drop_duplicate['keywords'].values.tolist())
    del new_df['keywords']
    return new_df
    
def amazon_test():
    #ROOT_DIR="D:/baiduyun/百度云同步盘/amazon新品/广告数据下载/自动广告数据下载/"
    #ROOT_DIR = "D:/baiduyun/百度云同步盘/广告数据下载/自动广告数据下载/"
    ROOT_DIR = "D:/baiduyun/百度云同步盘/广告数据下载/新广告报告/"
    given_dir = ROOT_DIR
    #key_word = 'auto-keyword-report'
    file_name_key_word = '搜索词'
    file_type = '.txt'
    file_type = '.xlsx'
    column = []
    all_files2 = []
    #all_files = get_all_scan_file(given_dir, file_name_key_word, file_type)
    all_files = get_all_scan_file_by_key(given_dir,file_name_key_word)
    print('all_files=',all_files)
    new_df_file_name = ''
    #key_columns=['Ad Group Name','Customer Search Term']
    key_columns=['广告组名称','客户搜索词']
    #f_column = 'Last Day of Impression'
    f_column = '结束日期'
    if len(all_files)<=0:
        pass
    elif len(all_files)==1:
        pass
        new_df_file_name = all_files[0]
        new_df = get_scan_data(column, new_df_file_name, given_dir, file_name_key_word, file_type)
        #new_df.to_csv(new_df_file_name[:-4]+'.csv')
        new_df.to_excel(new_df_file_name[:-4]+'.xlsx')
    else:
        new_df_file_name = all_files[0]
        old_df_file_name = all_files[1]
        new_df = get_scan_data(column, new_df_file_name, given_dir, file_name_key_word, file_type)
        print('new_df_file_name=',new_df_file_name)
        print('old_df_file_name=',old_df_file_name)
        old_df = get_scan_data(column, old_df_file_name, given_dir, file_name_key_word, file_type)
        updated_new_df = find_new_items(new_df,old_df,keyword_columns=key_columns,
                filter_column = f_column,given_dir=ROOT_DIR)
        
        #updated_new_df.to_csv(new_df_file_name[:-4]+ 'updated' + '.csv')
        #updated_new_df.to_excel(new_df_file_name[:-4]+ 'updated' + '.xlsx')#,sheet_name=new_df_file_name[:-4])
        #print(new_df_file_name.split('/')[-1])
        file_name = new_df_file_name.split('/')[-1].split('.')[0]
        pre_file_name = new_df_file_name.split('.')[0]
        pre_file_name = pre_file_name.replace(file_name_key_word,'result/'+file_name_key_word)
        updated_new_df.to_excel(pre_file_name+ ' updated' + '.xlsx',sheet_name=file_name)
        
    print('比较完毕')
    
    
def amazon_test0():
    #ROOT_DIR="D:/baiduyun/百度云同步盘/amazon新品/广告数据下载/自动广告数据下载/"
    #ROOT_DIR = "D:/baiduyun/百度云同步盘/广告数据下载/自动广告数据下载/"
    ROOT_DIR = "D:/baiduyun/百度云同步盘/广告数据下载/新广告报告/"
    given_dir = ROOT_DIR
    #key_word = 'auto-keyword-report'
    key_word = '搜索词'
    file_type = '.txt'
    #file_type = '.csv'
    """
    column_lines = 'Campaign Name    Ad Group Name    Customer Search Term    Keyword    Match Type    First Day of Impression    Last Day of Impression    Impressions    Clicks    CTR    Total Spend    Average CPC    ACoS    Currency    Orders placed within 1-week of a click    Product Sales within 1-week of a click    Conversion Rate within 1-week of a click    Same SKU units Ordered within 1-week of click    Other SKU units Ordered within 1-week of click    Same SKU units Product Sales within 1-week of click    Other SKU units Product Sales within 1-week of click'
    column=column_lines.split('    ')
    #column=column_lines.split('\t')
    print('column=',column)
    """
    column     = []
    all_files = get_all_scan_file(given_dir, key_word, file_type)
    print(all_files)
    given_file_name = all_files[0]
    txt_df = get_scan_data(column, given_file_name, given_dir, key_word, file_type)
    
    given_file_name1 = all_files[1]
    #print(txt_df)
    print('given_file_name=',given_file_name)
    old_df = get_scan_data(column, given_file_name1, given_dir, key_word, file_type)
    filter_column = 'Last Day of Impression'
    max_last_day = max(old_df[filter_column])
    print(max_last_day)
    print('given_file_name1=',given_file_name1)
    #print(old_df)
    txt_df_all = pd.concat([txt_df, old_df],ignore_index=False)
    print(txt_df_all)
    txt_df_all['space'] =' '
    txt_df_all['keywords'] = txt_df_all['Ad Group Name'] + txt_df_all['space'] + txt_df_all['Customer Search Term']
    del txt_df_all['space']
    print(txt_df_all['keywords'])
    txt_df_all.to_csv(given_dir+'txt_df_all.csv')
    duplicate = txt_df_all.duplicated(subset='keywords',keep=False)# keep='first')# keep=first, last, false
    #http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.duplicated.html
    duplicate.to_csv(given_dir+'duplicate1.csv')
    txt_df_all['duplicate'] = duplicate
    print(duplicate)
    txt_df_all.to_csv(given_dir+'txt_df_all1.csv')
    drop_duplicate = txt_df_all.drop_duplicates(subset='keywords',keep=False)# keep='first', inplace=False)
    #http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop_duplicates.html
    print(drop_duplicate)
    drop_duplicate.to_csv(given_dir+'drop_duplicate.csv')
   
    new_adding_searchterm = drop_duplicate[drop_duplicate[filter_column]>max_last_day]
    new_adding_searchterm.to_csv(given_dir+'new_adding_searchterm.csv')
    print(new_adding_searchterm)
    
    print(new_adding_searchterm['keywords'].values.tolist())
    print(txt_df)
    txt_df['space'] =' '
    txt_df['keywords'] = txt_df['Ad Group Name'] + txt_df['space'] + txt_df['Customer Search Term']
    txt_df['new'] = txt_df['keywords'].isin(new_adding_searchterm['keywords'].values.tolist())
    #txt_df.apply()#np.where(txt_df['keywords'] in new_adding_searchterm['keywords'].values.tolist(),True,False)
    print(txt_df)
    

amazon_test()

def test():
    history_bar_data=pd.DataFrame({' NTAsset': ['BAMS-1001068584','BAMS-1001068511','BAMS-1001068512','BAMS-1001068513'],
                                   ' GDNTAsset': ['CO12034450','CO12034451','CO12034452','CO12034453'],
                                   ' SN': ['NNTM000078J0','NNTM000078J1','NNTM000078J2','NNTM000078J3'], 
                                   ' PEC': ['RNC','PP15K','ATCA','SUN T4'],
                                   'FrameRackPosition': ['/CNGN18/Lab/CP00/33','/CNGN18/Lab/CN01/33','/CNGN18/Lab/CN02/33','/CNGN18/Lab/CN04/33'],
                                   ' SiteLocation':['IU','IU','IU','IU']})
    history_bar_data = history_bar_data.set_index(' NTAsset')
    #print('Raw DB data:')
    #raw_db_file_name="C:/BarScannerExcels/hist/CGC_asset.xls"
    #history_bar_data.to_excel(raw_db_file_name)
    eqm_data=get_eqm_data()
    print(eqm_data)
    print(eqm_data.columns.values.tolist())
    #print(history_bar_data)
    ROOT_DIR="C:/BarScannerExcels/"
    bar_data_df = consolidate_scan_data(ROOT_DIR+'data/')
    print('Scanning data:')  
    print(bar_data_df)
    update_data,updated_history_bar_data = compare_scan_data(bar_data_df,history_bar_data)
    print('Compare new scan data with DB raw data')
    update_data_file_name = 'C:/BarScannerExcels/temp/updated_scan_data.xls'
    final_db_data_file_name = 'C:/BarScannerExcels/temp/db_data.xls'
    
    #asset_prime = 'anna.chen@ericsson.com'
    #asset_prime = 'tony.cao@ericsson.com'
    asset_prime = 'jason.g.zhang@ericsson.com'
    #copy_addr='PDLEENGSDG@pdl.internal.ericsson.com'
    #copy_addr = ['jason.g.zhang@ericsson.com','tony.cao@ericsson.com']
    copy_addr = 'jason.g.zhang@ericsson.com'
    bcopy_addr = 'jason.g.zhang@ericsson.com'
    if update_data.empty:
        print("no data need to update")
        pass
    else:
        update_data.to_excel(update_data_file_name)
        print('Scan data write as:  %s' % update_data_file_name)
        updated_history_bar_data.to_excel(final_db_data_file_name)
        print('Final DB data write as:  %s' % final_db_data_file_name)
        """
        sm.send_mail(from_addr='jason.g.zhang@ericsson.com', to_addr='jason.g.zhang@ericsson.com',cc_addr='jason.g.zhang@ericsson.com',
           subjest_content='E-location update', mail_content='Please see the asset update in the attachment', 
           attachment=update_data_file_name)
        """
        sm.send_mail(from_addr='jason.g.zhang@ericsson.com', to_addr=asset_prime,
           subjest_content='E-location update', mail_content='Please see the asset update in the attachment', 
           cc_addr=copy_addr,bcc_addr=bcopy_addr,
           attachment=update_data_file_name)
        #updated_history_bar_data.to_excel()
        print('Send the new scan data to asset prime completed.')




#test()
#consolidate_scan_data()
