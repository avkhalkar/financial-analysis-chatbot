# Financial Analysis Chatbot

A RAG-powered financial analysis chatbot that combines real-time market data with deep document analysis through a conversational interface.

## Architecture

The system follows a **two-plane architecture**:

```
                         CONTROL PLANE (Manager Layer)
    ┌─────────────────────────────────────────────────────────────┐
    │  Inputs: Ticker, CIK/Scrip Code, Data Checklist            │
    │                                                             │
    │  ┌──────────────┐   ┌──────────────┐   ┌────────────────┐  │
    │  │  Existence   │──>│  Staleness   │──>│ Fetch/Embed/   │  │
    │  │    Check     │   │    Check     │   │ Upsert         │  │
    │  └──────────────┘   └──────────────┘   └────────────────┘  │
    │                                                             │
    │  Invariant: Pinecone = exact mirror of disk                │
    └─────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                         INFERENCE PLANE (Reader Layer)
    ┌─────────────────────────────────────────────────────────────┐
    │  READ-ONLY: Never fetches, modifies, or upserts            │
    │                                                             │
    │  ┌──────────────┐   ┌──────────────┐   ┌────────────────┐  │
    │  │ Embed Query  │──>│Query Pinecone│──>│ Return Context │  │
    │  └──────────────┘   └──────────────┘   └────────────────┘  │
    └─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
financial-analysis-chatbot/
├── agents/                         # ReAct agent for real-time queries
│   ├── agents.py                   # LangChain agent implementation
│   └── finance_tools.py            # Stock price, news tools
│
├── RAG/
│   └── src/
│       ├── control_plane/          # Data lifecycle management
│       │   ├── config.py           # Freshness policies, constants
│       │   ├── company_registry.py # Ticker → company info mapping
│       │   ├── freshness.py        # Staleness checking
│       │   └── manager.py          # ControlPlaneManager class
│       │
│       ├── inference_plane/        # Read-only retrieval
│       │   └── reader.py           # InferenceReader class
│       │
│       ├── orchestrate.py          # Unified entry point
│       │
│       ├── embeddings/             # Vector embedding generation
│       ├── indexing/               # Document chunking & Pinecone upsert
│       ├── retrieval/              # Query retrieval
│       ├── structured/             # yfinance data fetching
│       └── unstructured_data/      # SEC & BSE filing ingestion
│
└── data/                           # Local data storage (source of truth)
    └── {TICKER}/
        ├── structured/             # Financial statements (parquet/json)
        └── unstructured/           # SEC 10-K or BSE filings
```

## Installation

```bash
# Clone the repository
git clone https://github.com/your-repo/financial-analysis-chatbot.git
cd financial-analysis-chatbot

# Install RAG dependencies (requires Python 3.10-3.12)
cd RAG
pip install -r requirements.txt

# Install agent dependencies
cd ../agents
pip install -r requirements.txt
```

**Note:** Python 3.14 is not yet supported due to dependency compatibility issues. Use Python 3.10-3.12.

## Environment Variables

Create a `.env` file in the project root:

```env
PINECONE_API_KEY=your_pinecone_api_key
HUGGING_FACE_API_KEY=your_huggingface_api_key
NEWSAPI_KEY=your_newsapi_key
```

## Usage

### Full Pipeline (Recommended)

```python
from src.orchestrate import orchestrate

# US Company - checks freshness, fetches if stale, indexes, retrieves
result = orchestrate("AAPL", "What are Apple's risk factors?")

# Indian Company
result = orchestrate("TCS", "Quarterly results", scrip_code="532540")

# Force refresh data
result = orchestrate("MSFT", "Revenue trends", force_refresh=True)

# Access results
print(result.retrieval_context)      # Context for LLM
print(result.components_updated)     # What was refetched
print(result.retrieval_matches)      # Raw matches with scores
```

### CLI Usage

```bash
cd RAG

# Basic query
python -m src.orchestrate AAPL "What are Apple's risk factors?"

# With scrip code for Indian company
python -m src.orchestrate TCS "Quarterly results" --scrip 532540

# Force refresh
python -m src.orchestrate MSFT "Revenue trends" --force
```

### Control Plane Only

```python
from src.orchestrate import control_only
from src.control_plane.manager import DataChecklist

# Just ensure data is fresh (no query)
result = control_only("AAPL")

# Custom checklist
result = control_only(
    "MSFT",
    checklist=DataChecklist(structured=["price", "income_stmt"], unstructured=True)
)
```

### Inference Plane Only

```python
from src.orchestrate import retrieve_only

# Query without checking freshness (assumes data is ready)
result = retrieve_only("AAPL", "What is Apple's revenue?")
```

## Supported Companies

### Pre-registered (no CIK/scrip needed)

**US Companies:** AAPL, MSFT, GOOGL, AMZN, META, NVDA, TSLA, JPM, V, JNJ

**Indian Companies:** TCS, RELIANCE, INFY, HDFCBANK, ICICIBANK, HINDUNILVR, ITC, SBIN, BHARTIARTL, KOTAKBANK

### Other Companies

Provide the identifier:
- **US:** CIK from SEC EDGAR (e.g., `cik="0001045810"` for NVIDIA)
- **India:** BSE scrip code (e.g., `scrip_code="500209"` for Infosys)

## Data Freshness Policies

| Component | Max Age | Description |
|-----------|---------|-------------|
| price | 24 hours | Daily stock prices |
| income_stmt | 90 days | Quarterly income statements |
| balance_sheet | 90 days | Quarterly balance sheets |
| cash_flow | 90 days | Quarterly cash flow |
| info | 7 days | Company info (market cap, etc.) |
| unstructured | 365 days | SEC 10-K filings |

## Data Sources

- **Structured Data:** [yfinance](https://github.com/ranaroussi/yfinance) (Yahoo Finance)
- **US Filings:** [SEC EDGAR](https://www.sec.gov/edgar/searchedgar/companysearch)
- **Indian Filings:** [BSE India](https://www.bseindia.com/)
- **Vector Store:** [Pinecone](https://www.pinecone.io/)
- **Embeddings:** [sentence-transformers/all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2)

## Key Invariants

1. **Disk is source of truth** - Pinecone always mirrors local data
2. **Stable vector IDs** - Same content = same ID, enabling overwrites not duplicates
3. **Inference is read-only** - InferenceReader never modifies data
4. **Component isolation** - Each data component is checked/updated independently

## License

MIT
