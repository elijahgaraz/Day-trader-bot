import requests
from typing import Dict, Any, Optional

def get_advice(
    snapshot: Dict[str, Any],
    advisor_url: str,
    advisor_token: str,
    timeout_ms: int = 800
) -> Optional[Dict[str, Any]]:
    """
    Gets trading advice from the external AI advisor service.

    Args:
        snapshot: A dictionary containing the market data for the advisor.
        advisor_url: The URL of the AI advisor service.
        advisor_token: The shared secret token for authentication.
        timeout_ms: The request timeout in milliseconds.

    Returns:
        A dictionary containing the advisor's response, or None if an error occurs.
    """
    if not advisor_url:
        return None

    headers = {"Content-Type": "application/json"}
    if advisor_token:
        headers["X-Advisor-Token"] = advisor_token

    try:
        response = requests.post(
            advisor_url,
            json=snapshot,
            headers=headers,
            timeout=timeout_ms / 1000.0
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Advisor request failed: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred when calling the advisor: {e}")
        return None
