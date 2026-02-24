import os
import yfinance as yf
import schedule
import time
from datetime import datetime
from pytz import timezone

from crewai import Agent, Task, Crew
from crewai.tools import tool


# =========================================================
# 1. DEFINE TOOL (Agent Action)
# =========================================================

@tool
def fetch_stock_price(ticker: str) -> str:
    """
    Fetches the latest mid-market price for a given ticker.
    """
    stock = yf.Ticker(ticker)
    price = stock.fast_info.get("last_price", "Unavailable")
    return f"The latest price of {ticker} is {price}"


# =========================================================
# 2. SET API KEY (Replace with your real key)
# =========================================================

os.environ["OPENAI_API_KEY"] = "DO8RSCHGR8JNZURO"


# =========================================================
# 3. DEFINE AGENT
# =========================================================

stock_agent = Agent(
    role="Portfolio Manager",
    goal="Monitor stocks and provide updates if price shifts significantly.",
    backstory="You are a high-frequency trading assistant focused on accuracy.",
    tools=[fetch_stock_price],   # ✅ NOW VALID TOOL
    verbose=True
)


# =========================================================
# 4. FUNCTION TO RUN DURING MARKET HOURS
# =========================================================

def run_agentic_update():

    est = timezone("US/Eastern")
    now_est = datetime.now(est)

    # Weekdays only (Mon–Fri)
    if now_est.weekday() <= 4:

        start_time = now_est.replace(hour=8, minute=30, second=0, microsecond=0)
        end_time = now_est.replace(hour=16, minute=1, second=0, microsecond=0)

        if start_time <= now_est <= end_time:

            print(f"[{now_est}] Market is open. Running agentic update...")

            task = Task(
                description=(
                    "Fetch the current price of AAPL using the tool "
                    "and summarize the market status."
                ),
                expected_output="A brief update on the stock price and trend.",
                agent=stock_agent
            )

            crew = Crew(
                agents=[stock_agent],
                tasks=[task],
                verbose=True
            )

            result = crew.kickoff()
            print(f"\n✅ Agent Output:\n{result}\n")

        else:
            print(f"[{now_est}] Outside market hours (8:30am–4:01pm EST).")

    else:
        print(f"[{now_est}] Weekend. Market closed.")


# =========================================================
# 5. SCHEDULE TASK
# =========================================================

schedule.every(15).minutes.do(run_agentic_update)

print("🚀 Stock Manager Agent is active. Waiting for scheduled window...")

while True:
    schedule.run_pending()
    time.sleep(60)