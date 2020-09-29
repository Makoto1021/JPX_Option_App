import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import timedelta
import io
import requests
import re
from bs4 import BeautifulSoup
from functools import partial, reduce


def get_url(datatype, day):
    """
    datatypeの値は1~4のいずれかをとる。
        1: ナイト・セッション、立会取引
        2: ナイト・セッション、JNET取引
        3: 日中、立会取引
        4: 日中、JNET取引
    """
    main_page_url = "https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html"
    date = str(day.year) + '{:02d}'.format(day.month) + '{:02d}'.format(day.day)
    response = requests.get(main_page_url)
    
    if datatype == 1:
        csv = "volume_by_participant_night.csv"
        download_url = "%s_%s"%(date, csv)
    elif datatype == 2:
        csv = "volume_by_participant_night_J-NET.csv"
        download_url = "%s_%s"%(date, csv)
    elif datatype == 3:
        csv = "volume_by_participant_whole_day.csv"
        download_url = "%s_%s"%(date, csv)
    elif datatype == 4:
        csv = "volume_by_participant_whole_day_J-NET.csv"
        download_url = "%s_%s"%(date, csv)
    else:
        download_url == None
    
    if response.ok:
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.findAll('a', href=re.compile(csv)):
            if download_url in a["href"]:
                url =  "https://www.jpx.co.jp/" + a["href"]
                return url
            else:
                url = None
        if url == None:
            print("ダウンロードできるデータが見つかりません")
            text = "No data"
            return text
    else:
        print("メインページが見つかりません")
        text = "No page"
        return text


# URLからCSVを読み込み
def get_csv(request, colnames):
    df = pd.read_csv(io.StringIO(request.decode('utf-8')), header=0, names = colnames, usecols=[0,1, 2, 3, 4, 5, 6, 7])
    return df

# 読み込んだCSVをきれいに整理
def clean_dataframe(df, day):
    
    code_indices = []
    
    for index, row in df.iterrows():
        if row['institutions_sell_code'] == 'JPX Code':
            code_indices.append(index)
            
    df['JPX_code'] = np.nan
    df['instrument'] = np.nan
    for first, second in zip(code_indices, code_indices[1:]):

        code = df['institutions_sell'][first]
        df.loc[df.index[first:second], 'JPX_code'] = code
        inst = df['institutions_sell'][first+1]
        df.loc[df.index[first:second], 'instrument'] = inst

    last = code_indices[-1]
    code = df['institutions_sell'][last]
    inst = df['institutions_sell'][last+1]
    df.loc[df.index[last:], 'JPX_code'] = code
    df.loc[df.index[last:], 'instrument'] = inst
    
    df = df.drop(df[df['institutions_sell_code']=="JPX Code"].index)
    df = df.drop(df[df['institutions_sell_code']=="Instrument"].index)
    # df['date'] = dt.date(day.year, day.month, day.day)
    df['date'] = day.strftime("%Y-%m-%d")
    df.reset_index(drop=True)
    
    return df

def get_K(k):
    k_split = k.split(" ")
    if k_split[0].replace('\xa0', '') == 'ATM':
        k = k_split[2]
    else:
        k = k_split[0]
    return k

def clean_option_table(table, gengetsu):
    df_option = pd.DataFrame(columns=["清算値", "建玉残", "取引高", "売気配IV_買気配IV", "売気配(数量) 買気配(数量)", 
                                  "IV", "前日比", "現在値", "権利行使価格", "CALL_PUT", 
                                  "デルタ", "ガンマ", "セータ", "ベガ"])
    for i, r in table.iterrows():
        if i%6==0:
            # print(r[8])
            k = r[8]
            delta_c = table.iloc[i+3, 0]
            gamma_c = table.iloc[i+3, 1]
            theta_c = table.iloc[i+3, 2]
            vega_c = table.iloc[i+3, 3]
            delta_p = table.iloc[i+5, 0]
            gamma_p = table.iloc[i+5, 1]
            theta_p = table.iloc[i+5, 2]
            vega_p = table.iloc[i+5, 3]
            settlement_c = r[0]
            open_interest_c = r[1]
            volume_c = r[2]
            ask_bid_IV_c = r[3]
            ask_bid_size_c = r[4]
            IV_c = r[5]
            change_c = r[6]
            last_c = r[7]
            settlement_p = r[16]
            open_interest_p = r[15]
            volume_p = r[14]
            ask_bid_IV_p = r[13]
            ask_bid_size_p = r[12]
            IV_p = r[11]
            change_p = r[10]
            last_p = r[9]
            new_row_c = pd.DataFrame({"清算値":[settlement_c], "建玉残":[open_interest_c], "取引高":[volume_c], 
                                      "売気配IV_買気配IV":[ask_bid_IV_c], "売気配(数量) 買気配(数量)":[ask_bid_size_c], 
                                      "IV":[IV_c], "前日比":[change_c], "現在値":[last_c], "権利行使価格":[k], "CALL_PUT":"コール", 
                                      "デルタ":[delta_c], "ガンマ":[gamma_c], "セータ":[theta_c], "ベガ":[vega_c]})

            new_row_p = pd.DataFrame({"清算値":[settlement_p], "建玉残":[open_interest_p], "取引高":[volume_p], 
                                      "売気配IV_買気配IV":[ask_bid_IV_p], "売気配(数量) 買気配(数量)":[ask_bid_size_p], 
                                      "IV":[IV_p], "前日比":[change_p], "現在値":[last_p], "権利行使価格":[k], "CALL_PUT":"プット", 
                                      "デルタ":[delta_p], "ガンマ":[gamma_p], "セータ":[theta_p], "ベガ":[vega_p]})

            df_option = df_option.append(new_row_c, ignore_index=True)
            df_option = df_option.append(new_row_p, ignore_index=True)
            
    df_option['権利行使価格'] = df_option['権利行使価格'].apply(get_K)
    df_option['権利行使価格'] = df_option['権利行使価格'].str.replace(",", "")
    df_option["限月"] = gengetsu

    return df_option

def get_option_table(gengetsu):
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Referer': 'https://www.jpx.co.jp/english/markets/index.html',
        'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7,it;q=0.6,la;q=0.5',
    }
    
    if gengetsu == 1:
        url = 'https://svc.qri.jp/jpx/english/nkopm/'
    elif gengetsu == 2:
        url = 'https://svc.qri.jp/jpx/english/nkopm/1'
    elif gengetsu == 3:
        url = 'https://svc.qri.jp/jpx/english/nkopm/2'
            
    response = requests.get(url, headers=headers)
    table = pd.read_html(response.text, attrs={"class": "price-table"})[0]
    table = clean_option_table(table, gengetsu)
    return table

def download_data(datatype, day, colnames, RAW_DATA):
    url = get_url(datatype=datatype, day=day)
    filetype = {1:"ナイト立会取引", 2:"ナイトJNET取引", 3:"日中立会取引", 4:"日中JNET取引"}
    if url == "No data":
        # 休日などでファイルがない場合はエラーメッセージを返します
        text = "%s年%s月%s日の%sデータが見つかりません"%(day.year, day.month, day.day, filetype[datatype])
        print(text)
        return text, None
    elif url == "No page":
        text = "メインページが見つかりません。https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html　を確認してください"
        print(text)
        return text, None
    elif requests.get(url).ok:
        s=requests.get(url).content
        df = clean_dataframe(get_csv(s, colnames = colnames), day = day)
        filename = RAW_DATA + filetype[datatype] + str(day.year) +"-"+ str(day.month) +"-"+ str(day.day) + ".csv"
        df.to_csv(filename)
        text = "%s年%s月%s日の%sデータをダウンロードしました"%(day.year, day.month, day.day, filetype[datatype])
        print(text)
        return text, df
    else:
        text = "何らかの理由でデータが見つかりません"
        print(text)
        return text, None