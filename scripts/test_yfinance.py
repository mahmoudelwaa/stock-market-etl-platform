import yfinance as yf

df=yf.Ticker("NVDA")

data = df.history(period="5d")

print(data)

print("\nColumns:", list(data.columns))
print("\nData types:\n", data.dtypes)
