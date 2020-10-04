# from tkinter import *
import tkinter as tk
import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import io
import requests
import re
from bs4 import BeautifulSoup
from functools import partial, reduce
from download_data import get_url, clean_dataframe, get_csv, download_data
from transform_data import *
from prepare_option_data import prepare_option_data
import os
import glob

master = tk.Tk()
master.geometry('600x800')
tk.Label(master, text="いつのデータを取得しますか？", anchor='w', width=30).grid(row=0)
tk.Label(master, text="年-月-日のフォーマットで入力してください", anchor='w', width=30, font=("Helvetica 9 italic", 10)).grid(row=1)
# tk.Label(master, text="前日は１、二日前なら２…と入力してください", anchor='w', width=30, font=("Helvetica 9 italic", 10)).grid(row=2)

# tk.Label(master, text="ログ").grid(row=5)
today = dt.today().strftime("%Y-%m-%d")
e1 = tk.Entry(master)
e1.insert(10, today)
e1.grid(row=0, column=1)

# ログ記録用の箱をつくる
lower_frame = tk.Frame(master, bg='#80c1ff', bd='5')
lower_frame.place(relx=0.5, rely=0.2, relwidth=0.75, relheight=0.6, anchor='n')
log_box = tk.Text(lower_frame, state='disabled')
log_box.place(relwidth=1, relheight=1)


## 1-2. パラメーターの設定
# 元データをダウンロードするためのフォルダ

dirname = os.getcwd()
RAW_DATA = os.path.join(dirname, '元データ/')

# 完成したデータを保存するためのフォルダ
SAVED_DATA = os.path.join(dirname, '完成データ/')

def log(msg):
    log_box.config(state='normal')
    log_box.insert('end', msg+'\n')
    log_box.see('end')
    log_box.config(state='disabled')
    log_box.update()


def main():

    day = dt.strptime(e1.get(), '%Y-%m-%d')
    print("day", day)

    # day = dt.today() - timedelta(days=days)
    colnames = ['institutions_sell_code', 'institutions_sell', 
            'institutions_sell_eng', 'volume_sell', 'institutions_buy_code', 
            'institutions_buy', 'institutions_buy_eng', 'volume_buy']

    print("2. データのダウンロード")
    log("2. データのダウンロード")

    text, df_wholeday = download_data(datatype=3, day=day, colnames=colnames, RAW_DATA=RAW_DATA)
    log(text)
    text, df_wholeday_JNET = download_data(datatype=4, day=day, colnames=colnames, RAW_DATA=RAW_DATA)
    log(text)
    text, df_night = download_data(datatype=1, day=day, colnames=colnames, RAW_DATA=RAW_DATA)
    log(text)
    text, df_night_JNET = download_data(datatype=2, day=day, colnames=colnames, RAW_DATA=RAW_DATA)
    log(text)
    
    ## 3. データの初期整理
    text = "\n3. オプションの表計算を開始"
    print(text)
    log(text)

    df_wholeday_op_final = prepare_option_data(df_wholeday, day)
    df_wholeday_JNET_op_final = prepare_option_data(df_wholeday_JNET, day)
    df_night_op_final = prepare_option_data(df_night, day)
    df_night_JNET_op_final = prepare_option_data(df_night_JNET, day)

    ## 9. CSV/Excelファイルに保存
    text = "\n4. Excelファイルに保存します"
    print(text)
    log(text)

    filename = SAVED_DATA + "オプション" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".xlsx"
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    df_wholeday_op_final.to_excel(writer, sheet_name='日中立会取引')
    df_wholeday_JNET_op_final.to_excel(writer, sheet_name='日中JNET取引')
    df_night_op_final.to_excel(writer, sheet_name='夜間立会取引')
    df_night_JNET_op_final.to_excel(writer, sheet_name='夜間JNET取引')
    writer.save()

    text = "Excelファイルの保存が完了しました"
    print(text)
    log(text)

    text = "\n画面を閉じてください"
    print(text)
    log(text)

b = tk.Button(master, text="データを取得する", command=main).grid(row=4, 
                                                               column=1, 
                                                               sticky=tk.W, 
                                                               pady=4)

master.mainloop()