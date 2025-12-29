"""
Constants for domain_status_graph package.

Centralizes magic numbers and configuration defaults.
"""

# Batch sizes for Neo4j operations
BATCH_SIZE_SMALL = 1000  # For node creation
BATCH_SIZE_LARGE = 5000  # For relationship creation
BATCH_SIZE_DELETE = 10000  # For relationship deletion

# GDS algorithm defaults
DEFAULT_TOP_K = 50
DEFAULT_SIMILARITY_CUTOFF = 0.1
DEFAULT_SIMILARITY_THRESHOLD = 0.6
DEFAULT_JACCARD_THRESHOLD = 0.3

# Minimum description length for similarity computation
# Filters out meaningless short descriptions (e.g., "N/A", very short text)
# that cause false exact matches (1.0 similarity)
MIN_DESCRIPTION_LENGTH_FOR_SIMILARITY = 200  # characters

# PageRank defaults
DEFAULT_MAX_ITERATIONS = 20
DEFAULT_DAMPING_FACTOR = 0.85

# Embedding defaults
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536

# Rate limiting
MIN_REQUEST_INTERVAL = 0.1  # seconds between API calls (general)
# OpenAI embeddings allow higher rates (100 req/sec)
EMBEDDING_REQUEST_INTERVAL = 0.01  # seconds between embedding API calls

# API rate limits (requests per second)
SEC_EDGAR_RATE_LIMIT = 10.0  # SEC EDGAR official limit: 10 req/sec
SEC_EDGAR_LONG_DURATION_LIMIT = 5.0  # SEC EDGAR long-duration limit: 5 req/sec average
FINVIZ_RATE_LIMIT = 5.0  # Finviz: No official API, web scraping. 5 req/sec is safe.
FINNHUB_RATE_LIMIT = 1.0  # Finnhub free tier: 60 req/min = 1 req/sec
YFINANCE_RATE_LIMIT = 0.0  # yfinance: No explicit limit, library handles throttling

# Cache TTL (Time To Live) in days
CACHE_TTL_COMPANY_DOMAINS = 30  # Company domain data cache TTL
CACHE_TTL_COMPANY_PROPERTIES = 30  # Company properties cache TTL
CACHE_TTL_10K_EXTRACTED = 365  # 10-K extracted data cache TTL (long-lived)
CACHE_TTL_NEGATIVE_RESULT = 7  # Negative results (not found) cache TTL (shorter)

# Parallel processing defaults
DEFAULT_WORKERS = 8  # Default number of parallel workers
DEFAULT_WORKERS_WITH_API = 16  # Default workers when API key is available (faster)
