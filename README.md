# JPX_Option_App
App to generate JPX Option trading data

# Data soource
Data is downloaded from JPX official website. https://www.jpx.co.jp/markets/derivatives/participant-volume/index.html
Table to calculate greeks is fetched from here: https://svc.qri.jp/jpx/nkopm/

# Usage
## Activate virtual environment
In terminal, run 
`conda create --name jpx_env python=3.6`
`conda activate jpx_env`
`pip install -r requirements.txt`

## Run the app
In terminal, run `python jpx_option.py`
It opens tkinter app. Click "データを取得する" to get the calculated table. 
Raw data will be saved in "元データ" folder in `.xlsx` format. The calculated table will be saved in the folder "完成データ" in `.xlsx`.
Run once a day. 
