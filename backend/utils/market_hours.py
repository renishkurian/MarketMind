import pytz
from datetime import datetime, time, timedelta

IST = pytz.timezone('Asia/Kolkata')

NSE_HOLIDAYS_CURRENT_YEAR = {
    # Hardcoded NSE Holidays for 2026 (YYYY-MM-DD)
    "2026-01-15", # Municipal Corporation Election
    "2026-01-26", # Republic Day
    "2026-03-03", # Holi
    "2026-03-26", # Shri Ram Navami
    "2026-03-31", # Shri Mahavir Jayanti
    "2026-04-03", # Good Friday
    "2026-04-14", # Dr. Baba Saheb Ambedkar Jayanti
    "2026-05-01", # Maharashtra Day
    "2026-05-28", # Bakri Id
    "2026-06-26", # Muharram
    "2026-09-14", # Ganesh Chaturthi
    "2026-10-02", # Mahatma Gandhi Jayanti
    "2026-10-20", # Dussehra
    "2026-11-08", # Diwali (Muhurat session only - usually closed for full day)
    "2026-11-10", # Diwali-Balipratipada
    "2026-11-24", # Sri Guru Nanak Dev Jayanti
    "2026-12-25"  # Christmas
}

def get_current_ist_time():
    return datetime.now(IST)

def is_market_open():
    now = get_current_ist_time()
    
    # Check if holiday
    if now.strftime("%Y-%m-%d") in NSE_HOLIDAYS_CURRENT_YEAR:
        return False
        
    # Check if weekend
    if now.weekday() >= 5: # 5=Saturday, 6=Sunday
        return False
        
    # Check market hours (9:15 AM to 3:30 PM IST)
    market_open = time(9, 15)
    market_close = time(15, 30)
    current_time = now.time()
    
    return market_open <= current_time <= market_close

def is_trading_day(dt: datetime = None):
    if dt is None:
        dt = get_current_ist_time()
        
    if dt.strftime("%Y-%m-%d") in NSE_HOLIDAYS_CURRENT_YEAR:
        return False
        
    if dt.weekday() >= 5:
        return False
        
    return True

def get_market_status():
    now = get_current_ist_time()
    if not is_trading_day(now):
        return "CLOSED"
        
    current_time = now.time()
    pre_open_start = time(9, 0)
    market_open = time(9, 15)
    market_close = time(15, 30)
    
    if pre_open_start <= current_time < market_open:
        return "PRE_OPEN"
    elif market_open <= current_time <= market_close:
        return "OPEN"
    else:
        return "CLOSED"

def time_to_next_open():
    now = get_current_ist_time()
    current_date = now.date()
    market_open_time = time(9, 15)
    
    # Try current day
    next_open = IST.localize(datetime.combine(current_date, market_open_time))
    
    if now < next_open and is_trading_day(now):
        return next_open - now
        
    # Search for next trading day
    days_added = 1
    while days_added < 15: # safety limit
        next_date = current_date + timedelta(days=days_added)
        next_datetime = IST.localize(datetime.combine(next_date, market_open_time))
        if is_trading_day(next_datetime):
            return next_datetime - now
        days_added += 1
        
    return timedelta(0)
