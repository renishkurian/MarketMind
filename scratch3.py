import yfinance as yf
print("Starting yf")
df = yf.download(["RELIANCE.NS"], period='1d', interval='1m', progress=False, auto_adjust=False, threads=False)
print("Done yf", df.empty)
