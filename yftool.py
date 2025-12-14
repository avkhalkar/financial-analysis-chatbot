import yfinance as yf
from langchain_core.tools import tool
from typing import Literal
from langchain_huggingface import HuggingFaceEndpoint, ChatHuggingFace
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub
from dotenv import load_dotenv
import json
import os

load_dotenv()

@tool
def get_stock_info(
    ticker: str, 
    report_type: Literal["price", "income_stmt", "balance_sheet", "cash_flow", "info"] = "price"
) -> str:
    """
    Fetches financial data for a public company.
    
    Args:
        ticker (str): The stock ticker symbol (e.g., 'AAPL').
        report_type (str): The report to fetch: 'price', 'income_stmt', 'balance_sheet', 'cash_flow', or 'info'.
    """
    # --- FIX 1: JSON Input Parsing Hack ---
    # ReAct agents often pass a single JSON string for multiple args. We manually unpack it here.
    if ticker.startswith("{"):
        try:
            args = json.loads(ticker)
            ticker = args.get("ticker", ticker)
            # If report_type was passed in the JSON, update it. 
            # Note: We prioritize the JSON value over the function argument if it exists.
            report_type = args.get("report_type", report_type)
        except json.JSONDecodeError:
            pass # Use original values if parsing fails
    # ---------------------------------------

    try:
        # Clean ticker just in case (remove spaces/newlines)
        ticker = ticker.strip()
        stock = yf.Ticker(ticker)
        
        if report_type == "price":
            hist = stock.history(period="5d")
            if hist.empty:
                return f"No price data found for {ticker}."
            return f"Recent stock price history for {ticker}:\n{hist[['Close', 'Volume']].to_markdown()}"

        elif report_type == "income_stmt":
            return f"Income Statement for {ticker}:\n{stock.financials.iloc[:, :3].T.to_markdown()}"
        
        elif report_type == "balance_sheet":
            return f"Balance Sheet for {ticker}:\n{stock.balance_sheet.iloc[:, :3].T.to_markdown()}"
        
        elif report_type == "cash_flow":
            return f"Cash Flow for {ticker}:\n{stock.cashflow.iloc[:, :3].T.to_markdown()}"

        elif report_type == "info":
            info = stock.info
            keys_to_keep = ['longName', 'sector', 'industry', 'marketCap', 'forwardPE', 'dividendYield']
            filtered_info = {k: info.get(k, 'N/A') for k in keys_to_keep}
            return f"Company Info for {ticker}:\n{filtered_info}"
            
        else:
            return f"Error: '{report_type}' is not a valid option."
            
    except Exception as e:
        return f"Error fetching data for {ticker}: {str(e)}"

prompt = hub.pull("hwchase17/react")

llm = HuggingFaceEndpoint(
    repo_id='openai/gpt-oss-120b', 
    task='text-generation',
    max_new_tokens=512,
    temperature=0.1
)#type:ignore

model = ChatHuggingFace(llm=llm)

agent = create_react_agent(
    llm=model,
    tools=[get_stock_info],
    prompt=prompt
)

agent_executor = AgentExecutor(
    agent=agent,
    tools=[get_stock_info],
    verbose=True,
    handle_parsing_errors=True 
)

# --- Test ---
if __name__ == "__main__":
    try:
        response = agent_executor.invoke({
            "input": "What is the current stock price of Apple (AAPL) and what was their revenue in the last income statement?"
        })
        print(f"\n✅ Result: {response['output']}")
    except Exception as e:
        print(f"Error: {e}")