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
from download_data import get_url, clean_dataframe, get_csv
from transform_data import *
from prepare_option_data import prepare_option_data
import os
import glob

master = tk.Tk()
master.geometry('600x800')
tk.Label(master, text="何日前のデータを取得しますか？", anchor='w', width=30).grid(row=0)
tk.Label(master, text="今日のデータなら０", anchor='w', width=30, font=("Helvetica 9 italic", 10)).grid(row=1)
tk.Label(master, text="前日は１、二日前なら２…と入力してください", anchor='w', width=30, font=("Helvetica 9 italic", 10)).grid(row=2)

# tk.Label(master, text="ログ").grid(row=5)
e1 = tk.Entry(master)
e1.insert(10, 0)
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

    days = int(e1.get())
    print("days", days)

    day = dt.today() - timedelta(days=days)
    colnames = ['institutions_sell_code', 'institutions_sell', 
            'institutions_sell_eng', 'volume_sell', 'institutions_buy_code', 
            'institutions_buy', 'institutions_buy_eng', 'volume_buy']

    print("2. データのダウンロード")
    log("2. データのダウンロード")
    
    ### 2-2. 日中立会取引データのダウンロード
    url_wholeday = get_url(datatype=3, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日の日中立会取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
        log(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
        log(text)
    elif requests.get(url_wholeday).ok:
        s=requests.get(url_wholeday).content
        df_wholeday = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = RAW_DATA + "日中立会取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        df_wholeday.to_csv(filename)
        text = "%s年%s月%s日の日中立会取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
        log(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
        log(text)
    
    ### 2-3. 日中JNET取引データのダウンロード
    url_wholeday_JNET = get_url(datatype=4, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日の日中JNET取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
        log(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
        log(text)
    elif requests.get(url_wholeday_JNET).ok:
        s=requests.get(url_wholeday_JNET).content
        df_wholeday_JNET = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = RAW_DATA + "日中JNET取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        df_wholeday_JNET.to_csv(filename)
        text = "%s年%s月%s日の日中JNET取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
        log(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
        log(text)
    
    ### 2-4. ナイト立会取引データのダウンロード
    url_night = get_url(datatype=1, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日のナイト立会取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
        log(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
        log(text)
    elif requests.get(url_night).ok:
        s=requests.get(url_night).content
        df_night = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = RAW_DATA + "ナイト立会取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        df_night.to_csv(filename)
        text = "%s年%s月%s日のナイト立会取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
        log(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
        log(text)
    
    ### 2-5. ナイトJNET取引データのダウンロード
    url_night_JNET = get_url(datatype=2, day=day)
    if url_wholeday == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日のナイトJNET取引データが見つかりません"%(day.year, day.month, day.day)
        print(text)
        log(text)
    elif url_wholeday == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
        log(text)
    elif requests.get(url_night_JNET).ok:
        s=requests.get(url_night_JNET).content
        df_night_JNET = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = RAW_DATA + "ナイトJNET取引" + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        df_night_JNET.to_csv(filename)
        text = "%s年%s月%s日のナイトJNET取引データをダウンロードしました"%(day.year, day.month, day.day)
        print(text)
        log(text)
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
        log(text)
    
    
    ## 3. データの初期整理
    text = "\n3. オプションの表計算を開始"
    print(text)
    log(text)

    df_wholeday_op_final = prepare_option_data(df_wholeday)
    df_wholeday_JNET_op_final = prepare_option_data(df_wholeday_JNET)
    df_night_op_final = prepare_option_data(df_night)
    df_night_JNET_op_final = prepare_option_data(df_night_JNET)

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