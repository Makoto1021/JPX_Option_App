import pandas as pd
import numpy as np
from transform_data import fut_or_op, get_meigara, get_strike_price, get_gengetsu, make_long_df
from download_data import clean_option_table, get_option_table

def prepare_option_data(df):
    
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
    option_cols = ["権利行使価格", "CALL_PUT", "デルタ", "IV", "限月"]
    df_option_1 = get_option_table(1)
    df_option_2 = get_option_table(2)
    df_option_3 = get_option_table(3)
    df_option = pd.concat([df_option_1, df_option_2], ignore_index=True)[option_cols]
    df_option = pd.concat([df_option, df_option_3], ignore_index=True)[option_cols]
    df_option = df_option.rename({"CALL_PUT":"先物/OP"}, axis=1)
    
    
    """
    # デルタ計算に必要な情報を計算する
    df["SQ_day"] = df.apply(lambda x: get_SQ_day(year=x["限月_年"], month=x["限月_月"]), axis=1)
    df['SQ_day'] = pd.to_datetime(df['SQ_day'], format='%Y%m%d', errors='ignore')
    df["T"] = (df["SQ_day"].sub(day.date(), axis=0)/ np.timedelta64(1, 'D'))/365 # SQまでの残日数(日)/365. 後ほどデルタの計算で使う
    df_day = get_225mini("https://port.jpx.co.jp/jpx/template/quote.cgi?F=tmp/future_daytime")
    # df = df.join(df_day, how="left", on="限月_月", rsuffix='_other')
    df = pd.merge(df, df_day, how="left", on="限月_月" )
    df = df.rename(columns={"現在値":"S"})
    R = get_R()
    df["S"] = df["S"].astype(int) * np.exp(-R*df["T"])
    """
    
    df = pd.merge(df, df_option, how="left", on=["権利行使価格", "先物/OP", "限月"])
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
    
    return df_final