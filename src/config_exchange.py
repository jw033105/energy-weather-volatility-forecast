# src/config_exchange.py

# Actual Load endpoint from "Try it" (no key here!)
MISO_DEX_ACTUAL_LOAD_URL_TEMPLATE = (
    "https://apim.misoenergy.org/lgi/v1/real-time/{date}/demand/actual"
)

# Header name shown in "Try it"
MISO_DEX_SUBSCRIPTION_HEADER = "Ocp-Apim-Subscription-Key"

# Store key in an env var (never commit secrets)
MISO_DEX_KEY_ENV = "MISO_DEX_KEY"