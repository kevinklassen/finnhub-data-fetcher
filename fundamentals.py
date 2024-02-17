# Standard library imports

# Third party imports

# Local application imports
from fetch_finnhub import fetch_data_for_endpoint

# Define endpoints
endpoints = [
    "profile",
    "financials",
    "recommendation",
    "price-target",
    "revenue-estimate",
    "ebitda-estimate",
    "ebit-estimate",
    "eps-estimate",
]

sub_endpoints = {
    "financials": [
        "bs_annual",
        "bs_quarterly",
        "cf_annual",
        "cf_quarterly",
        "ic_annual",
        "ic_quarterly",
    ]
}

# Fetch data for each endpoint and sub-endpoint if applicable
for endpoint in endpoints:
    if endpoint in sub_endpoints:
        for sub_endpoint in sub_endpoints[endpoint]:
            fetch_data_for_endpoint(
                api_key="your API key",
                endpoint=endpoint,
                sub_endpoint=sub_endpoint,
            )
    else:
        fetch_data_for_endpoint(
            api_key="your API key",
            endpoint=endpoint,
        )
