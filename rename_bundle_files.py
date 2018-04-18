# -*- coding:utf-8 -*-
import random
import os,sys


def workable_file_name(file_name,i=1):
    if os.path.exists(file_name):
        if file_name.find('.')<0:
            new_file_name = file_name + '_%s' % i
        else:#如果多余两个. 可能得到的不是预期
            new_file_name = file_name.replace('.','_%s.'%i)
        i = i + 1
        return workable_file_name(new_file_name, i)
    else:
        return file_name

def rename_all(cwd,remove_word='副本'):
    """
    把文件名特定字符串去掉， 比如 "副本"
    """
    print('cwd=',cwd)
    get_dir = os.listdir(cwd)  #遍历当前目录，获取文件列表
    print('get_dir=',get_dir)
    for i in get_dir:          
        sub_dir = os.path.join(cwd,i)  # 把第一步获取的文件加入路径
        if os.path.isdir(sub_dir):     #如果当前仍然是文件夹，递归调用
            rename_all(sub_dir,remove_word)
        else:
            if remove_word in i:
                new_file_name = os.path.join(cwd,i).replace(remove_word,'')
                new_file_name = workable_file_name(new_file_name, i=1)
                os.rename(os.path.join(cwd,i),new_file_name)  #用新名字取代旧名字
            else:
                pass
    return

if __name__ == "__main__":
    chgdir = 'D:/test/DD帐号有水印/'#'D:/test/'
    remove_keyword = ' 副本'
    rename_all(chgdir, remove_keyword)
    if len(sys.argv)>=2:
        if sys.argv[1] and isinstance(sys.argv[1], str):
            chgdir = sys.argv[1]  #start date string 
        if len(sys.argv)>=3:
            if sys.argv[2] and isinstance(sys.argv[2], str):
                remove_keyword = sys.argv[2]  #start date string 
    else:
        print('请输入要批量命名的DIR和删除的关键字，如： rename.exe D:/test/ " 副本"')
    
    rename_all(chgdir, remove_keyword)
    remove_keyword1 = '副本'
    rename_all(chgdir, remove_keyword1)
    remove_keyword1 = '-副本'
    rename_all(chgdir, remove_keyword1)
    remove_keyword1 = '- 副本'
    rename_all(chgdir, remove_keyword1)
        
    