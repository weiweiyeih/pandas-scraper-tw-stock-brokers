import requests
import pandas as pd
import datetime
import time
import re

# use rex to make "<!-- \tGenLink2stk('AS2891','中信金'); //-->" to "2891中信金"
def get_stock_id_name(text):
    m = re.search(r"GenLink2stk\('AS(\d+)','(.+?)'\);", text)
    if m:
        return m.group(1) + m.group(2)
    else:
        return text

# edit the column of "買超-券商名稱". make it separate digits and letters
def separate_digits_letters(text):
    m = re.search(r"(\d+)(\D+)", text)
    if m:
        return str(m.group(1)), str(m.group(2))
    else:
        return text, ""

# add "買超-" to the beginning of the column name
def add_buy(text):
    return "買超-" + text

# add "賣超-" to the beginning of the column name
def add_sell(text):
    return "賣超-" + text

# Table engineering - buy
def process_buy_table(df):
    # apply to the first column of table
    stock_id_name = df.iloc[:, 0].apply(get_stock_id_name)
    # put it back to the table
    df.iloc[:, 0] = stock_id_name
    # remove the first row
    df = df.iloc[1:] 
    # use the first row as header
    df.columns = df.iloc[0]
    # remove the first row again
    df = df.iloc[1:]
    
    # save "券商代號" and "券商名稱" to two columns
    df["券商代號"], df["券商名稱"] = zip(*df["券商名稱"].map(separate_digits_letters))
    
    # make "券商代號" is string
    df["券商代號"] = df["券商代號"].astype(str)
    
    # make "券商代號" the first column
    df = df[["券商代號", "券商名稱"] + [col for col in df.columns if col not in ["券商代號", "券商名稱"]]]
    
    # add "買超-" to the beginning of the column name
    df.columns = df.columns.map(add_buy)
    return df

# Table engineering - sell
def process_sell_table(df):
    # apply to the first column of table
    stock_id_name = df.iloc[:, 0].apply(get_stock_id_name)
    # put it back to the table
    df.iloc[:, 0] = stock_id_name
    # remove the first row
    df = df.iloc[1:] 
    # use the first row as header
    df.columns = df.iloc[0]
    # remove the first row again
    df = df.iloc[1:]
    
    # save "券商代號" and "券商名稱" to two columns
    df["券商代號"], df["券商名稱"] = zip(*df["券商名稱"].map(separate_digits_letters))
    
    # make "券商代號" is string
    df["券商代號"] = df["券商代號"].astype(str)
    
    # make "券商代號" the first column
    df = df[["券商代號", "券商名稱"] + [col for col in df.columns if col not in ["券商代號", "券商名稱"]]]
    
    # add "賣超-" to the beginning of the column name
    df.columns = df.columns.map(add_sell)
    return df

# Get 卷商買賣超排行
url = "https://5850web.moneydj.com/z/zg/zgb/zgb0.djhtm?a=1470&b=1470&c=B&d=1"
response = requests.get(url)
response.encoding = 'big5'  # Specify the correct encoding here

scraper = pd.read_html(response.text)

df_buy = process_buy_table(scraper[3])
df_sell = process_sell_table(scraper[4])

# Get "Goofinfo!"

# use POST method to get the data
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
}

# 1. Get all_stocks (today)

# Initialize the final dataframe
all_stocks = pd.DataFrame()

for rank in range(0, 8): # (0, 8)
    url = f"https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E6%88%90%E4%BA%A4%E5%83%B9+%28%E9%AB%98%E2%86%92%E4%BD%8E%29%40%40%E6%88%90%E4%BA%A4%E5%83%B9%40%40%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E&RANK={rank}"
    
    response = requests.post(url, headers=headers)
    response.encoding = 'uft8'  # Specify the correct encoding here
    
    scraper = pd.read_html(response.text)
    
    stocks = scraper[61]
    
    all_stocks = pd.concat([all_stocks, stocks], axis=0)

    print(f"Fetched {rank} page for all_stocks")
    
    time.sleep(10)
    
    
# delete the rows where "名稱" == "名稱"
all_stocks = all_stocks[all_stocks["名稱"] != "名稱"]    

# delete the columns which have all NaN
all_stocks = all_stocks.dropna(axis=1, how="all")


# 2. Get all_stocks_yesterday
# get yesterday's date
yesterday = datetime.date.today() - datetime.timedelta(days=1)
yesterday = yesterday.strftime("%Y/%m/%d") # e.g. '2024/03/22'

# Initialize the final dataframe
all_stocks_yesterday = pd.DataFrame()

for rank in range(0, 8): # (0, 8) -> 2m16s
    url = f"https://goodinfo.tw/tw2/StockList.asp?MARKET_CAT=%E7%86%B1%E9%96%80%E6%8E%92%E8%A1%8C&INDUSTRY_CAT=%E6%88%90%E4%BA%A4%E5%83%B9+%28%E9%AB%98%E2%86%92%E4%BD%8E%29%40%40%E6%88%90%E4%BA%A4%E5%83%B9%40%40%E7%94%B1%E9%AB%98%E2%86%92%E4%BD%8E&RANK={rank}&RPT_TIME={yesterday}"
    
    response = requests.post(url, headers=headers)
    response.encoding = 'uft8'  # Specify the correct encoding here
    
    scraper = pd.read_html(response.text)
    
    stocks = scraper[61]
    
    all_stocks_yesterday = pd.concat([all_stocks, stocks], axis=0)

    print(f"Fetched {rank} page for all_stocks_yesterday")
    
    time.sleep(10)
    
    
# delete the rows where "名稱" == "名稱"
all_stocks_yesterday = all_stocks[all_stocks["名稱"] != "名稱"]    

# delete the columns which have all NaN
all_stocks_yesterday = all_stocks.dropna(axis=1, how="all")

# # save to csv
# all_stocks_yesterday.to_csv("all_stocks_yesterday.csv", index=False)

# keep only "代號", "成交 張數" and  "成交額 (百萬)" in all_stocks_yesterday
all_stocks_yesterday = all_stocks_yesterday[["代號", "成交 張數", "成交額 (百萬)"]]

# rename "成交 張數" and  "成交額 (百萬)" to "前一日成交 張數" and  "前一日成交額 (百萬)"
all_stocks_yesterday.columns = ["代號", "前一日成交 張數", "前一日成交額 (百萬)"]

# merge all_stocks and all_stocks_yesterday based on "代號"
all_stocks = all_stocks.merge(all_stocks_yesterday, on="代號", how="left")

# Merge all_stocks with df_buy and df_sell, save to excel

# merge df_buy and all_stocks based on "買超-券商代號" and "代號"
df_buy = df_buy.merge(all_stocks, left_on="買超-券商代號", right_on="代號", how="left")

# remove the columns "排名", "代號", "名稱"
df_buy = df_buy.drop(columns=["排 名", "代號", "名稱"]) 

# save df_buy to excel file in a sheet named "買超"
df_buy.to_excel("券商進出排行.xlsx", sheet_name="買超", index=False)

# merge df_sell and all_stocks based on "賣超-券商代號" and "代號"
df_sell = df_sell.merge(all_stocks, left_on="賣超-券商代號", right_on="代號", how="left")

# remove the columns "排名", "代號", "名稱"
df_sell = df_sell.drop(columns=["排 名", "代號", "名稱"]) 

# save df_sell to an existing excel file in a new sheet named "賣超"
with pd.ExcelWriter("券商進出排行.xlsx", mode='a') as writer:
    df_sell.to_excel(writer, sheet_name="賣超", index=False)
    
# concat df_buy and df_sell side by side
df = pd.concat([df_buy, df_sell], axis=1)
with pd.ExcelWriter("券商進出排行.xlsx", mode='a') as writer:
    df.to_excel(writer, sheet_name="both", index=False)
    


