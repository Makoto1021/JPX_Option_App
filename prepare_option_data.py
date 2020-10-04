import pandas as pd
import numpy as np
from numpy import sqrt,mean,log,diff
from transform_data import fut_or_op, get_meigara, get_strike_price, get_gengetsu, make_long_df
from download_data import clean_option_table, get_option_table
import quandl
from datetime import datetime as dt
from datetime import timedelta
import calendar
import requests
import io
import scipy.stats as si

def prepare_option_data(df, day):
    
    df["先物/OP"] = np.nan
    df["先物/OP"] = df.JPX_code.apply(fut_or_op)
    df = df.dropna(subset=['先物/OP'])
    
    df["銘柄"] = np.nan
    df["銘柄"] = df.JPX_code.apply(get_meigara)
    df = df.dropna(subset=['銘柄'])
    
    df = df[df["先物/OP"].isin(["プット", "コール"])]
    df['権利行使価格'] = df.instrument.apply(get_strike_price)

    
    # 限月の抽出
    df = get_gengetsu(df)
    # Sellとbuyを縦長に積み重ねる
    df = make_long_df(df)
    
    # オプション取引情報をダウンロード
    option_cols = ["権利行使価格", "CALL_PUT", "デルタ", "IV", "限月", "清算値"]
    df_option_1 = get_option_table(1)
    df_option_2 = get_option_table(2)
    df_option_3 = get_option_table(3)
    df_option = pd.concat([df_option_1, df_option_2], ignore_index=True)[option_cols]
    df_option = pd.concat([df_option, df_option_3], ignore_index=True)[option_cols]
    df_option = df_option.rename({"CALL_PUT":"先物/OP"}, axis=1)
    df_option["清算値"] = df_option["清算値"].astype(float)
    df = pd.merge(df, df_option, how="left", on=["権利行使価格", "先物/OP", "限月"])
    
    
    # デルタ計算に必要な情報を計算する
    df["SQ_day"] = df.apply(lambda x: get_SQ_day(year=x["限月_年"], month=x["限月_月"]), axis=1)
    df['SQ_day'] = pd.to_datetime(df['SQ_day'], format='%Y%m%d', errors='ignore')
    df["T"] = (df["SQ_day"].sub(day.date(), axis=0)/ np.timedelta64(1, 'D'))/365 # SQまでの残日数(日)/365. 後ほどデルタの計算で使う
    df_day = get_225mini("https://port.jpx.co.jp/jpx/template/quote.cgi?F=tmp/future_daytime")
    # df = df.join(df_day, how="left", on="限月_月", rsuffix='_other')
    df = pd.merge(df, df_day, how="left", on="限月_月" )
    df = df.rename(columns={"現在値":"S"})
    R = get_R(day=day)
    df["S"] = df["S"].astype(float) * np.exp(-R*df["T"])
    df_hv = df.groupby(["限月_月", "限月_年", "date"]).count().reset_index().iloc[:, 0:3]
    df_hv["HV"] = df_hv.apply(lambda x: get_HV(gengetsu_month=x["限月_月"], gengetsu_year=x["限月_年"], end_date=x["date"]), axis=1)
    df = pd.merge(df, df_hv, on=["限月_月", "限月_年", "date"])
    # df["quote_price"] = ...
    df["IV"] = df.apply(lambda x: implied_vol(option_type=x["先物/OP"], option_price=x["清算値"], s=x["S"], k=x["権利行使価格"], r=R, T=x["T"], q=0), axis=1)
    df["デルタ"] = df.apply(lambda x: get_delta(option_type=x["先物/OP"], s=x["S"], k=x["権利行使価格"], r=R, T=x["T"], sigma=x["IV"]), axis=1)
    
    
    
    df["volume"] = df.volume.astype(int)
    df["institutions_code"] = df.institutions_code.astype(int)
    
    
    df = df.groupby(["先物/OP", 
                     "権利行使価格",      
                     "institutions_code",  
                     "institutions",
                     "sell_buy"]).agg({"volume":"max", "デルタ":"max"}).unstack(level="sell_buy")
    
    
    # nanを０で置き換える（売り買いの差を計算するため）
    df["volume", "buy"] = df["volume", "buy"].fillna(0).astype(int)
    df["volume", "sell"] = df["volume", "sell"].fillna(0).astype(int)
    
    # デルタはここではbuyとsell両方に値が入っているが、どちらも同じ値。最終的にひとつだけを選ぶ
    df["デルタ", "buy"] = df["デルタ", "buy"].replace("-", np.nan)
    df["デルタ", "sell"] = df["デルタ", "sell"].replace("-", np.nan)
    df["デルタ", "buy"] = df["デルタ", "buy"].fillna(df["デルタ", "sell"]).astype(float)
    df["デルタ", "sell"] = df["デルタ", "sell"].fillna(df["デルタ", "buy"]).astype(float)
    
    # デルタをひとつだけ選ぶ（ここではbuyの方から）   
    df["デルタ", "value"] = df["デルタ", "buy"]
    df = df.drop([["デルタ", "buy"], ["デルタ", "sell"]], axis=1)
    
    
    # 権利行使価格の高い順に並び替え
    df = df.sort_index(ascending=[True, False], level=[0, 1])
    
    # 売り買いの差を出す
    df["volume", "diff"] = df["volume", "buy"] - df["volume", "sell"]
    df = df.drop([["volume", "buy"], ["volume", "sell"]], axis=1)
    
    # デルタとボリューム（Diff）を掛け算
    df["デルタ", "value"] = df["デルタ", "value"] * df["volume", "diff"]
    
    # df = df.assign(delta=df["デルタ", "value"]).set_index('delta', append=True)
    # df = df.drop('value', axis=1, level=1)
    df.index = df.index.droplevel("institutions_code")
    df_deltas = df.groupby(['institutions']).sum().drop("diff", axis=1, level=1)
    df_deltas.columns = df_deltas.columns.droplevel()
    df_deltas.columns = ["value"]
    df_deltas.columns = pd.MultiIndex.from_product([df_deltas.columns, ['new']])
    df = df.drop("value", axis=1, level=1)
    
    # CALLを左、PUTを右
    # 権利行使価格の並び順も左が高い金額、右が安い金額
    df_temp = df.copy(deep=True)
    df = df.sort_index(axis=0, level=[0, 1], ascending=[True, True])
    df_unstacked = df.unstack(level=[0, 1], fill_value=0).sort_index(axis=1, ascending=[True, True, True, False])
    df_unstacked.columns = df_unstacked.columns.droplevel().droplevel()
    df_final = df_unstacked.merge(df_deltas, on=['institutions'], how='left')
    df_final = df_final.assign(delta=df_final["value", "new"]).set_index('delta', append=True)
    df_final = df_final.drop('value', axis=1, level=0)
    df_final = df_final.sort_index(axis=0, level=1, ascending=False)
    df_final = df_final.fillna(0).astype(int)
    
    return df_final


def newton_vol(option_type, S, K, T, premium, r, sigma):
    # Reference: https://aaronschlegel.me/implied-volatility-functions-python.html
    #option_type: プット or コール 
    #S: 原資産価格
    #K: 権利行使価格
    #T: SQまでの残日数／365
    #premium: オプションのプレミアム値
    #r: 10年もの国債利回り
    #sigma: ヒストリカル・ボラティリティ

    d1 = (np.log(S / K) + (r - 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = (np.log(S / K) + (r - 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    
    if option_type == "コール":
        fx = S * si.norm.cdf(d1, 0.0, 1.0) - K * np.exp(-r * T) * si.norm.cdf(d2, 0.0, 1.0) - premium
    elif option_type == "プット":
        fx = K * np.exp(-r * T) * si.norm.cdf(-d2, 0.0, 1.0) - S * si.norm.cdf(-d1, 0.0, 1.0) - premium
    else:
        return "プットかコールか選択してください"

    vega = (1 / np.sqrt(2 * np.pi)) * S * np.sqrt(T) * np.exp(-(si.norm.cdf(d1, 0.0, 1.0) ** 2) * 0.5)
    tolerance = 0.000001
    x0 = sigma
    xnew  = x0
    xold = x0 - 1

    while abs(xnew - xold) > tolerance:
    
        xold = xnew
        xnew = (xnew - fx - premium) / vega
        
    return abs(xnew)
    
def newton_vol_div(option_type, S, K, T, premium, r, q, sigma): 
    # Reference: https://aaronschlegel.me/implied-volatility-functions-python.html
    #option_type: プット or コール 
    #S: 原資産価格
    #K: 権利行使価格
    #T: SQまでの残日数／365
    #premium: オプションのプレミアム値
    #r: 10年もの国債利回り
    #sigma: ヒストリカル・ボラティリティ

    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = (np.log(S / K) + (r - q - 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))

    if option_type == "コール":
        fx = S * np.exp(-q * T) * si.norm.cdf(d1, 0.0, 1.0) - K * np.exp(-r * T) * si.norm.cdf(d2, 0.0, 1.0) - premium
    elif option_type == "プット":
        fx = K * np.exp(-r * T) * si.norm.cdf(-d2, 0.0, 1.0) - S * np.exp(-q * T) * si.norm.cdf(-d1, 0.0, 1.0) -  premium
    else:
        return "プットかコールか選択してください"
    
    vega = (1 / np.sqrt(2 * np.pi)) * S * np.exp(-q * T) * np.sqrt(T) * np.exp((-si.norm.cdf(d1, 0.0, 1.0) ** 2) * 0.5)
    
    tolerance = 0.000001
    x0 = sigma
    xnew  = x0
    xold = x0 - 1

    i = 1
    while abs(xnew - xold) > tolerance:
        print(i)
        xold = xnew
        xnew = (xnew - fx - premium) / vega
        i+=1
    return abs(xnew)

def implied_vol(option_type, option_price, s, k, r, T, q):
    # apply bisection method to get the implied volatility by solving the BSM function
    # ref: https://www.quantconnect.com/tutorials/introduction-to-options/historical-volatility-and-implied-volatility
    precision = 0.00001
    upper_vol = 500.0
    max_vol = 500.0
    min_vol = 0.0001
    lower_vol = 0.0001
    iteration = 0

    while 1:
        iteration +=1
        mid_vol = (upper_vol + lower_vol)/2.0
        price = bsm_price(option_type, mid_vol, s, k, r, T, q)
        # print("price", price)

        if option_type == 'コール':
            lower_price = bsm_price(option_type, lower_vol, s, k, r, T, q)

            if (lower_price - option_price) * (price - option_price) > 0:
                lower_vol = mid_vol
                # print("1st if")
            else:
                upper_vol = mid_vol
                # print("1st else")
            if abs(price - option_price) < precision:
                # print("2nd if")
                break 
            if mid_vol > max_vol - 5 :
                # print("3rd if")
                mid_vol = 0.000001
                break

        elif option_type == 'プット':
            upper_price = bsm_price(option_type, upper_vol, s, k, r, T, q)
            # print("upper_price", type(upper_price))
            # print("option_price", type(option_price))
            # print("price", type(price))
            # print("here!", (upper_price - option_price) * (price - option_price))

            if (upper_price - option_price) * (price - option_price) > 0:
                # print("4th if")
                upper_vol = mid_vol
            else:
                # print("2nd else")
                lower_vol = mid_vol
            if abs(price - option_price) < precision: 
                # print("5th if")
                break 
            if iteration > 50:
                # print("iteration exceedced 50")
                break

    return mid_vol

def bsm_price(option_type, sigma, s, k, r, T, q):
    # ref: https://www.quantconnect.com/tutorials/introduction-to-options/historical-volatility-and-implied-volatility
    # calculate the bsm price of European call and put options
    sigma = float(sigma)
    s = float(s)
    k = float(k)
    r = float(r)
    T = float(T)
    q = float(q)
    d1 = (np.log(s / k) + (r - q + sigma ** 2 * 0.5) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == 'コール':
        price = np.exp(-r*T) * (s * np.exp((r - q)*T) * si.norm.cdf(d1) - k *  si.norm.cdf(d2))
        return price
    elif option_type == 'プット':
        price = np.exp(-r*T) * (k * si.norm.cdf(-d2) - s * np.exp((r - q)*T) *  si.norm.cdf(-d1))
        return price
    else:
        print('No such option type %s') %option_type

def get_HV(gengetsu_month, gengetsu_year, end_date):
    quandl.ApiConfig.api_key = "jRwFwyFb4MzMxzbBxNHk"
    month = {"01":"F", "02":"G", "03":"H", "04":"J", "05":"K", "06":"M", "07":"N", "08":"Q", "09":"U", "10":"V", "11":"X", "12":"Z"}
    end_date = dt.strptime(end_date, '%Y-%m-%d')
    start_date = end_date - timedelta(days=20)
    start_date = str(start_date.year) + "-" + str(start_date.strftime('%m')) + "-" + str(start_date.strftime('%d'))
    end_date = str(end_date.year) + "-" + str(end_date.strftime('%m')) + "-" + str(end_date.strftime('%d'))
    code = "OSE/NK225M" + month[str(gengetsu_month)] + str(gengetsu_year)
    df = quandl.get(dataset=code, start_date=start_date, end_date=end_date)
    close = df['Sett Price']
    r = diff(log(close))
    r_mean = mean(r)
    diff_square = [(r[i]-r_mean)**2 for i in range(0,len(r))]
    std = sqrt(sum(diff_square)*(1.0/(len(r)-1)))
    vol = std*sqrt(252)
    return vol

def get_R(day):
    main_page_url = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
    response = requests.get(main_page_url)
    s = requests.get(main_page_url).content
    df = pd.read_csv(io.StringIO(s.decode('cp932')), header=1)
    reiwa = day.year - 2018
    reference_date = "R" + str(reiwa) + "." + str(day.month) + "." + str(day.day)
    # reference_date = "R2.9.30"
    if df.loc[df["基準日"]==reference_date].shape[0]!=0:
        R = float(df.loc[df["基準日"]==reference_date]["10年"].values[0])
    else:
        sub_page_url = "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"
        response = requests.get(sub_page_url)
        s = requests.get(sub_page_url).content
        df = pd.read_csv(io.StringIO(s.decode('cp932')), header=1)
        R = float(df.loc[df["基準日"]==reference_date]["10年"].values[0])
    return R

def get_SQ_day(year, month):
    year = int(year)
    month = int(month)
    c = calendar.Calendar(firstweekday=calendar.SUNDAY)
    monthcal = c.monthdatescalendar(year,month)
    third_friday = [day for week in monthcal for day in week if \
                    day.weekday() == calendar.FRIDAY and \
                    day.month == month][2]
    return third_friday

def get_225mini(url):
    df_day = pd.read_html(url)[0]
    df_day = df_day.drop([0, 1], axis=0)
    df_day.columns=["銘柄", "限月_月", "取引日",  
                    "始値", "高値", "安値", "現在値", 
                    "前日比", "取引高", "売り気配",
                    "売り気配_数量", "買い気配", "買い気配_数量", "清算値段", 
                    "制限値幅", "建玉残高"]
    df_day = df_day.reset_index(drop=True)
    df_day = df_day.iloc[3:6, :].reset_index(drop=True)
    df_day.loc[1] = df_day.loc[1].shift(1)
    df_day.loc[2] = df_day.loc[2].shift(1)
    df_day["銘柄"] = '日経225mini'
    df_day["限月_月"] = df_day["限月_月"].str.extract(r'([0-9]+\/[0-9]+)')
    df_new = df_day["限月_月"].str.split("/", n = 1, expand = True) 
    df_day["限月_月"] = df_new[1].astype(int)
    
    df_new_price = df_day["現在値"].str.split("(", n = 1, expand = True) 
    df_day["現在値"] = df_new_price[0].str.replace(',','')
    df_day = df_day[["限月_月", "現在値"]]
    return df_day

def get_delta(option_type, s, k, r, T, sigma):
    sigma = float(sigma)
    s = float(s)
    k = float(k)
    r = float(r)
    T = float(T)
    d1 = (log(s/k) + (r + (sigma ** 2)/2) * T) / (sigma * sqrt(T))
    if option_type == "コール":
        delta = si.norm.cdf(d1)
    elif option_type == "プット":
        delta = si.norm.cdf(d1) - 1
    else:
        return "enter put or call for option_type"
    return delta
