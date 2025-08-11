from __future__ import annotations
import threading
import webbrowser
import requests
import random
import time
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAGetTrendbarsReq
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod
from typing import List, Any, Optional, Tuple, Dict
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAReconcileReq,
    ProtoOAReconcileRes,
    ProtoOAClosePositionReq
)
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASpotEvent
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoMessage
from twisted.internet import reactor
from twisted.internet.defer import TimeoutError
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
import queue
import sys
import traceback
import pandas as pd
from datetime import datetime, timezone

TREND_BAR_PERIOD_SECONDS = {
    ProtoOATrendbarPeriod.M1: 60,
    ProtoOATrendbarPeriod.M5: 300,
    ProtoOATrendbarPeriod.M15: 900,
    ProtoOATrendbarPeriod.M30: 1800,
    ProtoOATrendbarPeriod.H1: 3600,
    ProtoOATrendbarPeriod.H4: 14400,
    ProtoOATrendbarPeriod.D1: 86400,
    ProtoOATrendbarPeriod.W1: 604800,
}

# Conditional import for Twisted reactor for GUI integration
_reactor_installed = False
try:
    from twisted.internet import reactor, tksupport
    _reactor_installed = True
except ImportError:
    print("Twisted reactor or GUI support not found. GUI integration with Twisted might require manual setup.")


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, auth_code_queue: queue.Queue, **kwargs):
        self.auth_code_queue = auth_code_queue
        super().__init__(*args, **kwargs)

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == "/callback":
            query_components = urllib.parse.parse_qs(parsed_path.query)
            auth_code = query_components.get("code", [None])[0]

            if auth_code:
                self.auth_code_queue.put(auth_code)
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authentication Successful!</h1>")
                self.wfile.write(b"<p>You can close this browser tab and return to the application.</p></body></html>")
                print(f"OAuth callback handled, code extracted: {auth_code[:20]}...")
            else:
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authentication Failed</h1><p>No authorization code found in callback.</p></body></html>")
                print("OAuth callback error: No authorization code found.")
                self.auth_code_queue.put(None) # Signal failure or no code
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Not Found</h1></body></html>")

    def log_message(self, format, *args):
        # Suppress most log messages from the HTTP server for cleaner console
        # You might want to enable some for debugging.
        # Example: only log errors or specific messages
        if "400" in args[0] or "404" in args[0] or "code 200" in args[0]: # Log errors and successful callback
             super().log_message(format, *args)
        # else:
        #    pass
   
import json # For token persistence
    
# Imports from ctrader-open-api
try:
    from ctrader_open_api import Client, TcpProtocol, EndPoints, Protobuf
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
        ProtoHeartbeatEvent,
        ProtoErrorRes,
        ProtoMessage
        # ProtoPayloadType / ProtoOAPayloadType not here
    )
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
        ProtoOATraderReq, ProtoOATraderRes,
        ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes,
        ProtoOASpotEvent, ProtoOATraderUpdatedEvent,
        ProtoOANewOrderReq, ProtoOAExecutionEvent,
        ProtoOAErrorRes,
        # Specific message types for deserialization
        ProtoOAGetCtidProfileByTokenRes,
        ProtoOAGetCtidProfileByTokenReq,
        ProtoOASymbolsListReq, ProtoOASymbolsListRes, # For fetching symbol list (light symbols)
        ProtoOASymbolByIdReq, ProtoOASymbolByIdRes,    # For fetching full symbol details
        ProtoOAGetTrendbarsReq, # Added for historical data
        ProtoOAGetTrendbarsRes  # Added for historical data
    )
    # ProtoOALightSymbol is implicitly used by ProtoOASymbolsListRes
    # ProtoOASymbol is used by ProtoOASymbolByIdRes and for our symbol_details_map value type
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
        ProtoOATrader, ProtoOASymbol,
        ProtoOAOrderType,      # Moved for place_market_order
        ProtoOATradeSide,      # Moved for place_market_order
        ProtoOAExecutionType,  # Moved for _handle_execution_event
        ProtoOAOrderStatus,     # Moved for _handle_execution_event
        ProtoOATrendbarPeriod  # Added for historical data
    )
    USE_OPENAPI_LIB = True
except ImportError as e:
    print(f"ctrader-open-api import failed ({e}); running in mock mode.")
    USE_OPENAPI_LIB = False

TOKEN_FILE_PATH = "tokens.json"

     # ─── Crypto helper ────────────────────────────────────────────────────
CRYPTO_PREFIXES = {"BTCUSD", "ETH", "LTC", "XRP", "DOGE"}  # extend as needed

def _is_crypto_symbol(symbol_name: str) -> bool:
    return any(symbol_name.startswith(pref) for pref in CRYPTO_PREFIXES)
class Trader:
    def __init__(self, settings, history_size: int = 100):
        
        self.settings = settings
        self.is_connected: bool = False
        self._is_client_connected: bool = False
        self._last_error: str = ""
        self.price_history: List[float] = [] # Stores recent bid prices for the default symbol (tick data)
        self.history_size = history_size # Max length for self.price_history
        self.latest_prices: Dict[str, float] = {}
        # OHLC Data Storage for default symbol
        self.timeframes_seconds = {
            '15s': 15,
            '1m': 60,
            '5m': 300,
            '15m': 900
        }
        self.current_bars = {} # Stores the currently forming bar for each timeframe
        self.ohlc_history = {} # Stores history of completed bars for each timeframe
        self.max_ohlc_history_len = 500 # Max number of OHLC bars to keep per timeframe

        for tf_str in self.timeframes_seconds.keys():
            self.current_bars[tf_str] = {
                'timestamp': None, # Start time of the bar (datetime object)
                'open': None,
                'high': None,
                'low': None,
                'close': None,
                'volume': 0 # Using tick count as volume for now
            }
            self.ohlc_history[tf_str] = pd.DataFrame(
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )


        # Initialize token fields before loading
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None
        self._load_tokens_from_file() # Load tokens on initialization

        # Account details
        self.ctid_trader_account_id: Optional[int] = settings.openapi.default_ctid_trader_account_id
        self.account_id: Optional[str] = None # Will be string representation of ctidTraderAccountId
        self.balance: Optional[float] = None
        self.equity: Optional[float] = None
        self.currency: Optional[str] = None
        self.used_margin: Optional[float] = None # For margin used

        # Symbol data
        self.symbols_map: Dict[str, int] = {} # Map from symbol name to symbolId
        self.symbol_details_map: Dict[int, Any] = {} # Map from symbolId to ProtoOASymbol
        self.default_symbol_id: Optional[int] = None # Symbol ID for the default_symbol from settings
        self.subscribed_spot_symbol_ids: set[int] = set()


        self._client: Optional[Client] = None
        self._message_id_counter: int = 1
        self._reactor_thread: Optional[threading.Thread] = None
        self._auth_code: Optional[str] = None # To store the auth code from OAuth flow
        self._account_auth_initiated: bool = False # Flag to prevent duplicate account auth attempts

        if USE_OPENAPI_LIB:
            host = (
                EndPoints.PROTOBUF_LIVE_HOST
                if settings.openapi.host_type == "live"
                else EndPoints.PROTOBUF_DEMO_HOST
            )
            port = EndPoints.PROTOBUF_PORT
            self._client = Client(host, port, TcpProtocol)
            self._client.setConnectedCallback(self._on_client_connected)
            self._client.setDisconnectedCallback(self._on_client_disconnected)
            self._client.setMessageReceivedCallback(self._on_message_received)
        else:
            print("Trader initialized in MOCK mode.")

        self._auth_code_queue = queue.Queue() # Queue to pass auth_code from HTTP server thread
        self._http_server_thread: Optional[threading.Thread] = None
        self._http_server: Optional[HTTPServer] = None

    def _save_tokens_to_file(self):
        """Saves the current OAuth access token, refresh token, and expiry time to TOKEN_FILE_PATH."""
        tokens = {
            "access_token": self._access_token,
            "refresh_token": self._refresh_token,
            "token_expires_at": self._token_expires_at,
        }
        try:
            with open(TOKEN_FILE_PATH, "w") as f:
                json.dump(tokens, f)
            print(f"Tokens saved to {TOKEN_FILE_PATH}")
        except IOError as e:
            print(f"Error saving tokens to {TOKEN_FILE_PATH}: {e}")

    def _load_tokens_from_file(self):
        """Loads OAuth tokens from a local file."""
        try:
            with open(TOKEN_FILE_PATH, "r") as f:
                tokens = json.load(f)
            self._access_token = tokens.get("access_token")
            self._refresh_token = tokens.get("refresh_token")
            self._token_expires_at = tokens.get("token_expires_at")
            if self._access_token:
                print(f"Tokens loaded from {TOKEN_FILE_PATH}. Access token: {self._access_token[:20]}...")
            else:
                print(f"{TOKEN_FILE_PATH} not found or no access token in it. Will need OAuth.")
        except FileNotFoundError:
            print(f"Token file {TOKEN_FILE_PATH} not found. New OAuth flow will be required.")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading tokens from {TOKEN_FILE_PATH}: {e}. Will need OAuth flow.")
            # In case of corrupted file, good to try to remove it or back it up
            try:
                import os
                os.remove(TOKEN_FILE_PATH)
                print(f"Removed corrupted token file: {TOKEN_FILE_PATH}")
            except OSError as rm_err:
                print(f"Error removing corrupted token file: {rm_err}")

    def _next_message_id(self) -> str:
        mid = str(self._message_id_counter)
        self._message_id_counter += 1
        return mid

    # Twisted callbacks
    def _on_client_connected(self, client: Client) -> None:
        print("OpenAPI Client Connected.")
        self._is_client_connected = True
        self._last_error = ""
        req = ProtoOAApplicationAuthReq()
        req.clientId = self.settings.openapi.client_id or ""
        req.clientSecret = self.settings.openapi.client_secret or ""
        if not req.clientId or not req.clientSecret:
            print("Missing OpenAPI credentials.")
            client.stopService()
            return
        print(f"Sending ProtoOAApplicationAuthReq: {req}")
        d = client.send(req)
        d.addCallbacks(self._handle_app_auth_response, self._handle_send_error)

    def _on_client_disconnected(self, client: Client, reason: Any) -> None:
        print(f"OpenAPI Client Disconnected: {reason}")
        self.is_connected = False
        self._is_client_connected = False
        self._account_auth_initiated = False # Reset flag

    def _on_message_received(self, client: Client, message: Any) -> None:
        print(f"Original message received (type: {type(message)}): {message}")

        # Attempt to extract and deserialize using Protobuf.extract
        try:
            actual_message = Protobuf.extract(message)
            print(f"Message extracted via Protobuf.extract (type: {type(actual_message)}): {actual_message}")
        except Exception as e:
            print(f"Error using Protobuf.extract: {e}. Falling back to manual deserialization if possible.")
            actual_message = message # Fallback to original message for manual processing attempt
            # Log additional details about the original message if it's a ProtoMessage
            if isinstance(message, ProtoMessage):
                 print(f"  Fallback: Original ProtoMessage PayloadType: {message.payloadType}, Payload Bytes: {message.payload[:64]}...") # Log first 64 bytes

        # If Protobuf.extract returned the original ProtoMessage wrapper, it means it couldn't deserialize it.
        # Or if an error occurred and we fell back.
        # We can attempt manual deserialization as before, but it's better if Protobuf.extract handles it.
        # For now, the dispatch logic below will use the result of Protobuf.extract.
        # If actual_message is still ProtoMessage, the specific isinstance checks will fail,
        # which is the correct behavior if it couldn't be properly deserialized.

        # We need to get payload_type for logging in case it's an unhandled ProtoMessage
        payload_type = 0
        if isinstance(actual_message, ProtoMessage): # If still a wrapper after extract attempt
            payload_type = actual_message.payloadType
            print(f"  Protobuf.extract did not fully deserialize. Message is still ProtoMessage wrapper with PayloadType: {payload_type}")
        elif isinstance(message, ProtoMessage) and actual_message is message: # Fallback case where actual_message was reset to original
            payload_type = message.payloadType
        # Ensure payload_type is defined for the final log message if it's an unhandled ProtoMessage
        final_payload_type_for_log = payload_type if isinstance(actual_message, ProtoMessage) else getattr(actual_message, 'payloadType', 'N/A')


        # Dispatch by type using the (potentially deserialized) actual_message
        if isinstance(actual_message, ProtoOAApplicationAuthRes):
            print("  Dispatching to _handle_app_auth_response")
            self._handle_app_auth_response(actual_message)
        elif isinstance(actual_message, ProtoOAAccountAuthRes):
            print("  Dispatching to _handle_account_auth_response")
            self._handle_account_auth_response(actual_message)
        elif isinstance(actual_message, ProtoOAGetCtidProfileByTokenRes):
            print("  Dispatching to _handle_get_ctid_profile_response")
            self._handle_get_ctid_profile_response(actual_message)
        elif isinstance(actual_message, ProtoOAGetAccountListByAccessTokenRes):
            print("  Dispatching to _handle_get_account_list_response")
            self._handle_get_account_list_response(actual_message)
        elif isinstance(actual_message, ProtoOASymbolsListRes):
            print("  Dispatching to _handle_symbols_list_response")
            self._handle_symbols_list_response(actual_message)
        elif isinstance(actual_message, ProtoOASymbolByIdRes):
            print("  Dispatching to _handle_symbol_details_response")
            self._handle_symbol_details_response(actual_message)
        elif isinstance(actual_message, ProtoOASubscribeSpotsRes):
            # This is usually handled by the callback in _send_subscribe_spots_request directly,
            # but good to have a dispatch log if it comes through _on_message_received.
            print("  Received ProtoOASubscribeSpotsRes (typically handled by send callback).")
            # self._handle_subscribe_spots_response(actual_message, []) # Might need context if called here
        elif isinstance(actual_message, ProtoOATraderRes):
            print("  Dispatching to _handle_trader_response")
            self._handle_trader_response(actual_message)
        elif isinstance(actual_message, ProtoOATraderUpdatedEvent):
            print("  Dispatching to _handle_trader_updated_event")
            self._handle_trader_updated_event(actual_message)
        elif isinstance(actual_message, ProtoOASpotEvent):
            self._handle_spot_event(actual_message) # Potentially noisy
            # print("  Received ProtoOASpotEvent (handler commented out).")
        elif isinstance(actual_message, ProtoOAExecutionEvent):
            self._handle_execution_event(actual_message)
            # print("  Received ProtoOAExecutionEvent (handler commented out).")
        elif isinstance(actual_message, ProtoOAGetTrendbarsRes):
            print("  Dispatching to _handle_get_trendbars_response")
            self._handle_get_trendbars_response(actual_message)
        elif isinstance(actual_message, ProtoHeartbeatEvent):
            print("  Received heartbeat.")
        elif isinstance(actual_message, ProtoOAErrorRes): # Specific OA error
            print(f"  Dispatching to ProtoOAErrorRes handler. Error code: {actual_message.errorCode}, Description: {actual_message.description}")
            self._last_error = f"{actual_message.errorCode}: {actual_message.description}"
            if "NOT_AUTHENTICATED" in actual_message.errorCode:
                self._last_error += ". Please reconnect."
                self.disconnect()
        elif isinstance(actual_message, ProtoErrorRes): # Common error
            print(f"  Dispatching to ProtoErrorRes (common) handler. Error code: {actual_message.errorCode}, Description: {actual_message.description}")
            self._last_error = f"Common Error {actual_message.errorCode}: {actual_message.description}"
            if "NOT_AUTHENTICATED" in actual_message.errorCode:
                self._last_error += ". Please reconnect."
                self.disconnect()
        # Check if it's still the ProtoMessage wrapper (meaning Protobuf.extract didn't deserialize it further)
        elif isinstance(actual_message, ProtoMessage): # Covers actual_message is message (if message was ProtoMessage)
                                                       # and actual_message is the result of extract but still a wrapper.
            print(f"  ProtoMessage with PayloadType {actual_message.payloadType} was not handled by specific type checks.")
        elif actual_message is message and not isinstance(message, ProtoMessage): # Original message was not ProtoMessage and not handled
             print(f"  Unhandled non-ProtoMessage type in _on_message_received: {type(actual_message)}")
        else: # Should ideally not be reached if all cases are handled
            print(f"  Message of type {type(actual_message)} (PayloadType {final_payload_type_for_log}) fell through all handlers.")

    # Handlers
    def _handle_app_auth_response(self, response: ProtoOAApplicationAuthRes) -> None:
        print("ApplicationAuth response received.")

        if self._account_auth_initiated:
            print("Account authentication process already initiated, skipping duplicate _handle_app_auth_response.")
            return

        # The access token from ProtoOAApplicationAuthRes is for the application's session.
        # We have a user-specific OAuth access token in self._access_token (if OAuth flow completed).
        # We should not overwrite self._access_token here if it was set by OAuth.
        # For ProtoOAAccountAuthReq, we must use the user's OAuth token.

        # Let's see if the response contains an access token, though we might not use it directly
        # if our main OAuth token is already present.
        app_session_token = getattr(response, 'accessToken', None)
        if app_session_token:
            print(f"ApplicationAuthRes provided an app session token: {app_session_token[:20]}...")
            # If self._access_token (OAuth user token) is NOT set,
            # this could be a fallback or an alternative flow not fully explored.
            # For now, we prioritize the OAuth token set by exchange_code_for_token.
            if not self._access_token:
                print("Warning: OAuth access token not found, but AppAuthRes provided one. This scenario needs review.")
                # self._access_token = app_session_token # Potentially use if no OAuth token? Risky.

        if not self._access_token:
            self._last_error = "Critical: OAuth access token not available for subsequent account operations."
            print(self._last_error)
            # Potentially stop the client or signal a critical failure here
            if self._client:
                self._client.stopService()
            return

        # Proceed to account authentication or discovery, using the OAuth access token (self._access_token)
        if self.ctid_trader_account_id and self._access_token:
            # If a ctidTraderAccountId is known (e.g., from settings) and we have an OAuth access token,
            # proceed directly with ProtoOAAccountAuthReq as per standard Spotware flow.
            print(f"Known ctidTraderAccountId: {self.ctid_trader_account_id}. Attempting ProtoOAAccountAuthReq.")
            self._account_auth_initiated = True # Set flag before sending
            self._send_account_auth_request(self.ctid_trader_account_id)
        elif self._access_token:
            # If ctidTraderAccountId is not known, but we have an access token,
            # first try to get the account list associated with this token.
            # ProtoOAGetAccountListByAccessTokenReq is preferred over ProtoOAGetCtidProfileByTokenReq
            # if the goal is to find trading accounts. Profile is more about user details.
            print("No default ctidTraderAccountId. Attempting to get account list by access token.")
            self._account_auth_initiated = True # Set flag before sending
            self._send_get_account_list_request()
        else:
            # This case should ideally be prevented by earlier checks in the connect() flow.
            self._last_error = "Critical: Cannot proceed with account auth/discovery. Missing ctidTraderAccountId or access token after app auth."
            print(self._last_error)
            if self._client:
                self._client.stopService()


    def _handle_get_ctid_profile_response(self, response: ProtoOAGetCtidProfileByTokenRes) -> None:
        
        print(f"Received ProtoOAGetCtidProfileByTokenRes. Content: {response}")

        pass

    def _handle_subscribe_spots_response(self, response_wrapper: Any, subscribed_symbol_ids: List[int]) -> None:
        """Handles the response from a ProtoOASubscribeSpotsReq."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_subscribe_spots_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASubscribeSpotsRes):
            print(f"_handle_subscribe_spots_response: Expected ProtoOASubscribeSpotsRes, got {type(actual_message)}. Message: {actual_message}")
            # Potentially set an error or log failure for specific symbols if the response structure allowed it.
            # For ProtoOASubscribeSpotsRes, it's usually an empty message on success.
            # Errors would typically come as ProtoOAErrorRes or via _handle_send_error.
            self._last_error = f"Spot subscription response was not ProtoOASubscribeSpotsRes for symbols {subscribed_symbol_ids}."
            return

        # ProtoOASubscribeSpotsRes is an empty message. Its reception confirms the subscription request was processed.
        # Actual spot data will come via ProtoOASpotEvent.
        print(f"Successfully processed spot subscription request for ctidTraderAccountId: {self.ctid_trader_account_id} and symbol IDs: {subscribed_symbol_ids}.")
        # No specific action needed here other than logging, errors are usually separate messages.

    def _send_get_symbol_details_request(self, symbol_ids: List[int]) -> None:
        """Sends a ProtoOASymbolByIdReq to get full details for specific symbol IDs."""
        if not self._ensure_valid_token():
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot get symbol details: Client not connected."
            print(self._last_error)
            return
        if not self.ctid_trader_account_id: # ctidTraderAccountId is not part of ProtoOASymbolByIdReq
            pass # but good to ensure we have it generally for consistency
        if not symbol_ids:
            self._last_error = "Cannot get symbol details: No symbol_ids provided."
            print(self._last_error)
            return

        print(f"Requesting full symbol details for IDs: {symbol_ids}")
        req = ProtoOASymbolByIdReq()
        req.symbolId.extend(symbol_ids)
        if not self.ctid_trader_account_id:
            self._last_error = "Cannot get symbol details: ctidTraderAccountId is not set."
            print(self._last_error)
            return
        req.ctidTraderAccountId = self.ctid_trader_account_id

        print(f"Sending ProtoOASymbolByIdReq: {req}")
        try:
            d = self._client.send(req)
            # The callback will handle ProtoOASymbolByIdRes
            d.addCallbacks(self._handle_symbol_details_response, self._handle_send_error)
            print("Added callbacks to ProtoOASymbolByIdReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_get_symbol_details_request send command: {e}")
            self._last_error = f"Exception sending symbol details request: {e}"

    def _send_get_symbols_list_request(self) -> None:
        """Sends a ProtoOASymbolsListReq to get all symbols for the authenticated account."""
        if not self._ensure_valid_token(): # Should not be strictly necessary if called after successful auth, but good practice
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot get symbols list: Client not connected."
            print(self._last_error)
            return
        if not self.ctid_trader_account_id:
            self._last_error = "Cannot get symbols list: ctidTraderAccountId is not set."
            print(self._last_error)
            return

        print(f"Requesting symbols list for account {self.ctid_trader_account_id}")
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        # req.includeArchivedSymbols = False # Optional: to include archived symbols

        print(f"Sending ProtoOASymbolsListReq: {req}")
        try:
            d = self._client.send(req)
            d.addCallbacks(self._handle_symbols_list_response, self._handle_send_error)
            print("Added callbacks to ProtoOASymbolsListReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_get_symbols_list_request send command: {e}")
            self._last_error = f"Exception sending symbols list request: {e}"

    def _handle_symbols_list_response(self, response_wrapper: Any) -> None:
        """Handles the response from a ProtoOASymbolsListReq."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_symbols_list_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASymbolsListRes):
            print(f"_handle_symbols_list_response: Expected ProtoOASymbolsListRes, got {type(actual_message)}. Message: {actual_message}")
            self._last_error = "Symbols list response was not ProtoOASymbolsListRes."
            return

        print(f"Received ProtoOASymbolsListRes with {len(actual_message.symbol)} symbols.")
        self.symbols_map.clear()
        self.default_symbol_id = None

        # Populate light symbols map
        for light_symbol in actual_message.symbol:
            self.symbols_map[light_symbol.symbolName] = light_symbol.symbolId
            if light_symbol.symbolName == self.settings.general.default_symbol:
                self.default_symbol_id = light_symbol.symbolId

        if not self.symbols_map:
            print("Warning: No symbols found.")
            return

        # 🔥 NEW: Request full details for *all* symbols (not just the default)
        symbol_ids = list(self.symbols_map.values())
        print(f"Requesting full details for all symbols: {symbol_ids}")
        self._send_get_symbol_details_request(symbol_ids)


    def _handle_symbol_details_response(self, response_wrapper: Any) -> None:
        """Handles the response from a ProtoOASymbolByIdReq, containing full symbol details."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_symbol_details_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASymbolByIdRes):
            print(f"_handle_symbol_details_response: Expected ProtoOASymbolByIdRes, got {type(actual_message)}. Message: {actual_message}")
            self._last_error = "Symbol details response was not ProtoOASymbolByIdRes."
            # Potentially try to re-request or handle error for specific symbols if needed.
            return

        print(f"Received ProtoOASymbolByIdRes with details for {len(actual_message.symbol)} symbol(s).")

        for detailed_symbol_proto in actual_message.symbol: # These are full ProtoOASymbol objects
            # ProtoOASymbol does not have symbolName, get it from symbols_map
            symbol_name_for_logging = "Unknown"
            for name, id_val in self.symbols_map.items():
                if id_val == detailed_symbol_proto.symbolId:
                    symbol_name_for_logging = name
                    break

            self.symbol_details_map[detailed_symbol_proto.symbolId] = detailed_symbol_proto
            print(f"  Stored full details for Symbol ID: {detailed_symbol_proto.symbolId} ({symbol_name_for_logging}), Digits: {detailed_symbol_proto.digits}, PipPosition: {detailed_symbol_proto.pipPosition}")

        # After updating the details map, check if we have details for the default symbol
        # and if so, proceed to subscribe for its spot prices.
        if self.default_symbol_id is not None and self.default_symbol_id in self.symbol_details_map:
            # Get the default symbol's name from symbols_map for logging
            default_symbol_name_for_logging = "Unknown"
            for name, id_val in self.symbols_map.items():
                if id_val == self.default_symbol_id:
                    default_symbol_name_for_logging = name
                    break

            print(f"Full details for default symbol '{default_symbol_name_for_logging}' (ID: {self.default_symbol_id}) received. Subscribing to spots.")

            # Ensure ctidTraderAccountId is available before subscribing
            if self.ctid_trader_account_id is not None:
                # Fetch historical data for 1m timeframe for the default symbol
                # Assuming M1 is ProtoOATrendbarPeriod.M1
                # Fetch max_ohlc_history_len bars (e.g., 200)
                print(f"Fetching initial historical 1m trendbars for default symbol {default_symbol_name_for_logging} (ID: {self.default_symbol_id}).")
                self._send_get_trendbars_request(
                    symbol_id=self.default_symbol_id,
                    period=ProtoOATrendbarPeriod.M1, # Assuming M1 is the desired period
                    count=self.max_ohlc_history_len
                )
                self._send_get_trendbars_request(
                    symbol_id=self.default_symbol_id,
                    period=ProtoOATrendbarPeriod.M15,
                    count=self.max_ohlc_history_len
                )
                # Note: We might want to fetch for other timeframes too if strategies use them.
                # For now, focusing on '1m'.

                # After requesting historical data, subscribe to live spots
                self._send_subscribe_spots_request(self.ctid_trader_account_id, list(self.symbols_map.values()))
                self.subscribed_spot_symbol_ids.add(self.default_symbol_id)
            else:
                print(f"Error: ctidTraderAccountId not set. Cannot subscribe to spots or fetch history for {default_symbol_name_for_logging}.")
                self._last_error = "ctidTraderAccountId not available for spot subscription after getting symbol details."
        elif self.default_symbol_id is not None:
            # This case should ideally not be hit if ProtoOASymbolByIdReq was successful for default_symbol_id
            print(f"Warning: Full details for default symbol ID {self.default_symbol_id} not found in response, cannot subscribe to its spots yet.")


    def _handle_account_auth_response(self, response: ProtoOAAccountAuthRes) -> None:
        print(f"Received ProtoOAAccountAuthRes: {response}")
        # The response contains the ctidTraderAccountId that was authenticated.
        # We should verify it matches the one we intended to authenticate.
        if response.ctidTraderAccountId == self.ctid_trader_account_id:
            print(f"Successfully authenticated account {self.ctid_trader_account_id}.")
            self.is_connected = True # Mark as connected for this specific account
            self._last_error = ""     # Clear any previous errors

            # After successful account auth, fetch initial trader details (balance, equity)
            self._send_get_trader_request(self.ctid_trader_account_id)

            # TODO: Subscribe to spots, etc., as needed by the application
            # self._send_subscribe_spots_request(symbol_id) # Example

            # After successful account auth, fetch symbol list to find default_symbol_id
            print("Account authenticated. Requesting symbols list...")
            self._send_get_symbols_list_request()

        else:
            print(f"AccountAuth failed. Expected ctidTraderAccountId {self.ctid_trader_account_id}, "
                  f"but response was for {response.ctidTraderAccountId if hasattr(response, 'ctidTraderAccountId') else 'unknown'}.")
            self._last_error = "Account authentication failed (ID mismatch or error)."
            self.is_connected = False
            # Consider stopping the client if account auth fails critically
            if self._client:
                self._client.stopService()

    def _handle_get_account_list_response(self, response: ProtoOAGetAccountListByAccessTokenRes) -> None:
        print("Account list response.")
        accounts = getattr(response, 'ctidTraderAccount', [])
        if not accounts:
            print("No accounts available for this access token.")
            self._last_error = "No trading accounts found for this access token."
            # Potentially disconnect or signal error more formally if no accounts mean connection cannot proceed.
            if self._client and self._is_client_connected:
                self._client.stopService() # Or some other error state
            return

        # TODO: If multiple accounts, allow user selection. For now, using the first.
        selected_account = accounts[0] # Assuming ctidTraderAccount is a list of ProtoOACtidTraderAccount
        if not selected_account.ctidTraderAccountId:
            print("Error: Account in list has no ctidTraderAccountId.")
            self._last_error = "Account found but missing ID."
            return

        self.ctid_trader_account_id = selected_account.ctidTraderAccountId
        print(f"Selected ctidTraderAccountId from list: {self.ctid_trader_account_id}")
        # Optionally save to settings if this discovery should update the default
        # self.settings.openapi.default_ctid_trader_account_id = self.ctid_trader_account_id

        # Now that we have a ctidTraderAccountId, authenticate this account
        self._send_account_auth_request(self.ctid_trader_account_id)

    def _handle_trader_response(self, response_wrapper: Any) -> None:
        # If this is called directly by a Deferred, response_wrapper might be ProtoMessage
        # If called after global _on_message_received, it's already extracted.
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_trader_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper # Assume it's already the specific message type

        if not isinstance(actual_message, ProtoOATraderRes):
            print(f"_handle_trader_response: Expected ProtoOATraderRes, got {type(actual_message)}. Message: {actual_message}")
            return

        # Now actual_message is definitely ProtoOATraderRes
        trader_object = actual_message.trader # Access the nested ProtoOATrader object
        
        trader_details_updated = self._update_trader_details(
            "Trader details response.", trader_object
        )

        if trader_details_updated and hasattr(trader_object, 'ctidTraderAccountId'):
            current_ctid = getattr(trader_object, 'ctidTraderAccountId')
            print(f"Value of trader_object.ctidTraderAccountId before assignment: {current_ctid}, type: {type(current_ctid)}")
            self.account_id = str(current_ctid)
            print(f"self.account_id set to: {self.account_id}")
        elif trader_details_updated:
            print(f"Trader details updated, but ctidTraderAccountId missing from trader_object. trader_object: {trader_object}")
        else:
            print("_handle_trader_response: _update_trader_details did not return updated details or trader_object was None.")


    def _handle_trader_updated_event(self, event_wrapper: Any) -> None:
        if isinstance(event_wrapper, ProtoMessage):
            actual_event = Protobuf.extract(event_wrapper)
            print(f"_handle_trader_updated_event: Extracted {type(actual_event)} from ProtoMessage wrapper.")
        else:
            actual_event = event_wrapper

        if not isinstance(actual_event, ProtoOATraderUpdatedEvent):
            print(f"_handle_trader_updated_event: Expected ProtoOATraderUpdatedEvent, got {type(actual_event)}. Message: {actual_event}")
            return
            
        self._update_trader_details(
            "Trader updated event.", actual_event.trader # Access nested ProtoOATrader
        )
        # Note: TraderUpdatedEvent might not always update self.account_id if it's already set,
        # but it will refresh balance, equity, margin if present in actual_event.trader.

    def _update_trader_details(self, log_message: str, trader_proto: ProtoOATrader):
        """Helper to update trader balance and equity from a ProtoOATrader object."""
        print(log_message)
        if trader_proto:
            print(f"Full ProtoOATrader object received in _update_trader_details: {trader_proto}")

            # Safely get ctidTraderAccountId for logging, though it's not set here directly
            logged_ctid = getattr(trader_proto, 'ctidTraderAccountId', 'N/A')

            balance_val = getattr(trader_proto, 'balance', None)
            if balance_val is not None:
                self.balance = balance_val / 100.0
                print(f"  Updated balance for {logged_ctid}: {self.balance}")
            else:
                print(f"  Balance not found in ProtoOATrader for {logged_ctid}")

            equity_val = getattr(trader_proto, 'equity', None)
            if equity_val is not None:
                self.equity = equity_val / 100.0
                print(f"  Updated equity for {logged_ctid}: {self.equity}")
            else:
                # self.equity remains as its previous value (or None if first time)
                print(f"  Equity not found in ProtoOATrader for {logged_ctid}. self.equity remains: {self.equity}")
            
            currency_val = getattr(trader_proto, 'depositAssetId', None) # depositAssetId is often used for currency ID
            # TODO: Convert depositAssetId to currency string if mapping is available
            # For now, just store the ID if it exists, or keep self.currency as is.
            if currency_val is not None:
                 # self.currency = str(currency_val) # Or map to symbol
                 print(f"  depositAssetId (currency ID) for {logged_ctid}: {currency_val}")

            # Placeholder for margin - we need to see what fields are available from logs
           
            return trader_proto
        else:
            print("_update_trader_details received empty trader_proto.")
        return None

        symbol_id = event.symbolId
        symbol_name = next(
            (name for name, sid in self.symbols_map.items() if sid == symbol_id),
            None
        )
        
        if symbol_name:
            self.latest_prices[symbol_name] = current_price
            
        if self.default_symbol_id is not None and symbol_id == self.default_symbol_id:
           
            bid_for_log = event.bid if hasattr(event, 'bid') else "N/A (field missing)" # More robust check
            print(f"DEBUG Trader: SpotEvent for default symbol {symbol_id}. Timestamp={event.timestamp}, Bid={bid_for_log}")
            # --- END DETAILED TIMESTAMP LOGGING ---

            if event.timestamp == 0: # A timestamp of 0 is highly unlikely for a valid event
                print(f"Spot Event for default symbol {symbol_id} has timestamp 0. Skipping OHLC update.")
                return

            # Scale the price
            raw_bid_price = event.bid # Directly access, as HasField was problematic. Assumed present if timestamp > 0.
            price_scale_factor = 100000.0 # Default
            if symbol_id in self.symbol_details_map:
                digits = self.symbol_details_map[symbol_id].digits
                price_scale_factor = float(10**digits)

            current_price = raw_bid_price / price_scale_factor

            # Update simple price history (for immediate price checks, GUI, etc.)
            self.price_history.append(current_price)
            if len(self.price_history) > self.history_size:
                self.price_history.pop(0)

            # OHLC Aggregation Logic
            event_dt = datetime.fromtimestamp(event.timestamp / 1000, tz=timezone.utc)

            for tf_str, tf_seconds in self.timeframes_seconds.items():
                current_tf_bar = self.current_bars[tf_str]

                if current_tf_bar['timestamp'] is None: # First tick for this timeframe or after a reset
                    current_tf_bar['timestamp'] = event_dt.replace(second= (event_dt.second // tf_seconds) * tf_seconds, microsecond=0)
                    current_tf_bar['open'] = current_price
                    current_tf_bar['high'] = current_price
                    current_tf_bar['low'] = current_price
                    current_tf_bar['close'] = current_price
                    current_tf_bar['volume'] = 1 # Tick count
                else:
                    # Check if current tick falls into a new bar interval
                    bar_end_time = current_tf_bar['timestamp'] + pd.Timedelta(seconds=tf_seconds)

                    if event_dt >= bar_end_time:

                        # Add completed bar to history (if it has data)
                        if current_tf_bar['open'] is not None:
                            completed_bar_data = {
                                'timestamp': current_tf_bar['timestamp'], # Start time of the completed bar
                                'open': current_tf_bar['open'],
                                'high': current_tf_bar['high'],
                                'low': current_tf_bar['low'],
                                'close': current_tf_bar['close'], # Close of the *previous* bar
                                'volume': current_tf_bar['volume']
                            }
                            # Use pd.concat instead of append for DataFrames
                            self.ohlc_history[tf_str] = pd.concat([
                                self.ohlc_history[tf_str],
                                pd.DataFrame([completed_bar_data])
                            ], ignore_index=True)
                            
                            df = self.ohlc_history[tf_str]
                            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
                            df.set_index('timestamp', inplace=True)
                            self.ohlc_history[tf_str] = df

                            # Keep history to max_ohlc_history_len
                            if len(self.ohlc_history[tf_str]) > self.max_ohlc_history_len:
                                self.ohlc_history[tf_str] = self.ohlc_history[tf_str].iloc[-self.max_ohlc_history_len:]

                            # Optional: Log completed bar
                            # print(f"Completed {tf_str} bar: O={completed_bar_data['open']:.5f} H={completed_bar_data['high']:.5f} L={completed_bar_data['low']:.5f} C={completed_bar_data['close']:.5f} V={completed_bar_data['volume']}")

                        # Start a new bar
                        current_tf_bar['timestamp'] = event_dt.replace(second=(event_dt.second // tf_seconds) * tf_seconds, microsecond=0)
                        current_tf_bar['open'] = current_price
                        current_tf_bar['high'] = current_price
                        current_tf_bar['low'] = current_price
                        current_tf_bar['close'] = current_price
                        current_tf_bar['volume'] = 1
                    else:
                        # Update current (still forming) bar
                        current_tf_bar['high'] = max(current_tf_bar['high'], current_price)
                        current_tf_bar['low'] = min(current_tf_bar['low'], current_price)
                        current_tf_bar['close'] = current_price
                        current_tf_bar['volume'] += 1

    def _handle_spot_event(self, event: ProtoOASpotEvent) -> None:
        """Handles incoming spot events (price updates) for all subscribed symbols."""
        # 1) Get event timestamp
        if event.timestamp == 0:
            event_dt = datetime.now(timezone.utc)
            print(f"WARNING: SpotEvent has no timestamp; falling back to local time {event_dt.isoformat()}")
        else:
            event_dt = datetime.fromtimestamp(event.timestamp / 1000, tz=timezone.utc)

        symbol_id = event.symbolId
        raw_bid   = event.bid

        # 2) Scale the price using the symbol’s digits (default to 5 decimal places)
        price_scale = 100000.0
        if symbol_id in self.symbol_details_map:
            digits = self.symbol_details_map[symbol_id].digits
            price_scale = float(10 ** digits)
        current_price = raw_bid / price_scale

        # 3) Record this tick for the symbol name, so get_market_price() works
        symbol_name = next(
            (name for name, sid in self.symbols_map.items() if sid == symbol_id),
            None
        )
        if symbol_name:
            self.latest_prices[symbol_name] = current_price

        # 4) If this is the default symbol, update your existing price_history & OHLC logic
        if self.default_symbol_id is not None and symbol_id == self.default_symbol_id:
            # — simple price history —
            self.price_history.append(current_price)
            if len(self.price_history) > self.history_size:
                self.price_history.pop(0)

            # — OHLC aggregation —
            for tf_str, tf_seconds in self.timeframes_seconds.items():
                bar = self.current_bars[tf_str]
                # initialize new bar?
                if bar['timestamp'] is None:
                    start_ts = event_dt.replace(
                        second=(event_dt.second // tf_seconds) * tf_seconds,
                        microsecond=0
                    )
                    bar.update({
                        'timestamp': start_ts,
                        'open': current_price,
                        'high': current_price,
                        'low': current_price,
                        'close': current_price,
                        'volume': 1
                    })
                else:
                    bar_end = bar['timestamp'] + pd.Timedelta(seconds=tf_seconds)
                    if event_dt >= bar_end:
                        # finalize old bar
                        completed = {
                            'timestamp': bar['timestamp'],
                            'open': bar['open'],
                            'high': bar['high'],
                            'low': bar['low'],
                            'close': bar['close'],
                            'volume': bar['volume']
                        }
                        self.ohlc_history[tf_str] = pd.concat([
                            self.ohlc_history[tf_str],
                            pd.DataFrame([completed])
                        ], ignore_index=True)
                        # trim
                        if len(self.ohlc_history[tf_str]) > self.max_ohlc_history_len:
                            self.ohlc_history[tf_str] = self.ohlc_history[tf_str].iloc[-self.max_ohlc_history_len:]
                        # start new bar
                        start_ts = event_dt.replace(
                            second=(event_dt.second // tf_seconds) * tf_seconds,
                            microsecond=0
                        )
                        bar.update({
                            'timestamp': start_ts,
                            'open': current_price,
                            'high': current_price,
                            'low': current_price,
                            'close': current_price,
                            'volume': 1
                        })
                    else:
                        # update ongoing bar
                        bar['high']   = max(bar['high'], current_price)
                        bar['low']    = min(bar['low'], current_price)
                        bar['close']  = current_price
                        bar['volume'] += 1
    
    def get_available_symbol_names(self) -> List[str]:
        """Returns a sorted list of symbol name strings available from the API."""
        if not self.symbols_map:
            return []
        return sorted(list(self.symbols_map.keys()))

    def _handle_execution_event(self, event: ProtoOAExecutionEvent) -> None:
        # TODO: handle executions
        pass

    def _handle_send_error(self, failure: Any) -> None:
        """
        Centralized error handler for any send() failures.
        Ignores “outside trading hours” errors on crypto symbols so you can trade 24/7.
        """
        # Extract the error message
        err = failure.getErrorMessage()

        # If this was a crypto symbol and the only problem is OUTSIDE_TRADING_HOURS, swallow it
        current_symbol = getattr(self, "_last_order_symbol", None)
        if current_symbol and _is_crypto_symbol(current_symbol) and "OUTSIDE_TRADING_HOURS" in err:
            print(f"Ignoring outside‑trading‑hours for crypto {current_symbol}")
            return

        # Otherwise, log it normally and store for GUI/status checks
        print(f"Send error: {err}")
        if hasattr(failure, "printTraceback"):
            print("Traceback for send error:")
            failure.printTraceback(file=sys.stderr)
        else:
            print("Failure object does not have printTraceback method. Full failure object:")
            print(failure)

        # Preserve the last error for get_connection_status() etc.
        self._last_error = err


    # Sending methods
    def _send_account_auth_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return # Token refresh failed or no token, error set by _ensure_valid_token

        print(f"Requesting AccountAuth for {ctid} with token: {self._access_token[:20]}...") # Log token used
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = ctid
        req.accessToken = self._access_token or "" # Should be valid now

        print(f"Sending ProtoOAAccountAuthReq for ctid {ctid}: {req}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for ProtoOAAccountAuthReq: {d}")

            def success_callback(response_msg):
                # This callback is mostly for confirming the Deferred fired successfully.
                # Normal processing will happen in _on_message_received if message is dispatched.
                print(f"AccountAuthReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")
                # Note: We don't directly process response_msg here as _on_message_received should get it.

            def error_callback(failure_reason):
                print(f"AccountAuthReq error_callback triggered. Failure:")
                # Print a summary of the failure, and the full traceback if it's an exception
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'printTraceback'):
                    print(f"  Traceback for AccountAuthReq error:")
                    failure_reason.printTraceback(file=sys.stderr)
                else:
                    print(f"  Failure object (no printTraceback): {failure_reason}")
                self._handle_send_error(failure_reason) # Ensure our existing error handler is called

            d.addCallbacks(success_callback, errback=error_callback)
            print("Added callbacks to AccountAuthReq Deferred.")

        except Exception as e:
            print(f"Exception during _send_account_auth_request send command: {e}")
            self._last_error = f"Exception sending AccountAuth: {e}"
            # Potentially stop client if send itself fails critically
            if self._client and self._is_client_connected:
                self._client.stopService()
                self.is_connected = False # Ensure state reflects this

    def _send_get_account_list_request(self) -> None:
        if not self._ensure_valid_token():
            return

        print("Requesting account list.")
        req = ProtoOAGetAccountListByAccessTokenReq()
        if not self._access_token: # Should have been caught by _ensure_valid_token, but double check for safety
            self._last_error = "Critical: OAuth access token not available for GetAccountList request."
            print(self._last_error)
            if self._client:
                self._client.stopService()
            return
        req.accessToken = self._access_token
        print(f"Sending ProtoOAGetAccountListByAccessTokenReq: {req}")
        d = self._client.send(req)
        d.addCallbacks(self._handle_get_account_list_response, self._handle_send_error)

    def _send_get_trader_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return

        print(f"Requesting Trader details for {ctid}")
        req = ProtoOATraderReq()
        req.ctidTraderAccountId = ctid
        # Note: ProtoOATraderReq does not directly take an access token in its fields.
        # The authentication is expected to be session-based after AccountAuth.
        # If a token were needed here, the message definition would include it.
        print(f"Sending ProtoOATraderReq for ctid {ctid}: {req}")
        d = self._client.send(req)
        d.addCallbacks(self._handle_trader_response, self._handle_send_error)

    def _send_get_ctid_profile_request(self) -> None:
        """Sends a ProtoOAGetCtidProfileByTokenReq using the current OAuth access token."""
        if not self._ensure_valid_token(): # Ensure token is valid before using it
            return

        if not self._access_token:
            self._last_error = "Critical: OAuth access token not available for GetCtidProfile request."
            print(self._last_error)
            if self._client and self._is_client_connected:
                self._client.stopService()
            return

        print("Sending ProtoOAGetCtidProfileByTokenReq...")
        req = ProtoOAGetCtidProfileByTokenReq()
        req.accessToken = self._access_token

        print(f"Sending ProtoOAGetCtidProfileByTokenReq: {req}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for ProtoOAGetCtidProfileByTokenReq: {d}")

            # Adding specific callbacks for this request to see its fate
            def profile_req_success_callback(response_msg):
                print(f"GetCtidProfileByTokenReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")

            def profile_req_error_callback(failure_reason):
                print(f"GetCtidProfileByTokenReq error_callback triggered. Failure:")
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'printTraceback'): # May be verbose
                    print(f"  Traceback for GetCtidProfileByTokenReq error:")
                    failure_reason.printTraceback(file=sys.stderr)
                else:
                    print(f"  Failure object (no printTraceback): {failure_reason}")
                self._handle_send_error(failure_reason)

            d.addCallbacks(profile_req_success_callback, errback=profile_req_error_callback)
            print("Added callbacks to GetCtidProfileByTokenReq Deferred.")

        except Exception as e:
            print(f"Exception during _send_get_ctid_profile_request send command: {e}")
            self._last_error = f"Exception sending GetCtidProfile: {e}"
            if self._client and self._is_client_connected:
                self._client.stopService()
                self.is_connected = False

    def _send_subscribe_spots_request(self, ctid_trader_account_id: int, symbol_ids: List[int]) -> None:
        """Sends a ProtoOASubscribeSpotsReq to subscribe to spot prices for given symbol IDs."""
        if not self._ensure_valid_token():
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot subscribe to spots: Client not connected."
            print(self._last_error)
            return
        if not ctid_trader_account_id:
            self._last_error = "Cannot subscribe to spots: ctidTraderAccountId is not set."
            print(self._last_error)
            return
        if not symbol_ids:
            self._last_error = "Cannot subscribe to spots: No symbol_ids provided."
            print(self._last_error)
            return

        print(f"Requesting spot subscription for account {ctid_trader_account_id} and symbols {symbol_ids}")
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = ctid_trader_account_id
        req.symbolId.extend(symbol_ids) # symbolId is a repeated field

        # clientMsgId can be set if needed, but for subscriptions, the server pushes updates
        # req.clientMsgId = self._next_message_id()

        print(f"Sending ProtoOASubscribeSpotsReq: {req}")
        try:
            d = self._client.send(req)
            # Add callbacks: one for the direct response to the subscription request,
            # and one for handling errors during sending.
            # Spot events themselves will be handled by _on_message_received -> _handle_spot_event.
            d.addCallbacks(
                lambda response: self._handle_subscribe_spots_response(response, symbol_ids),
                self._handle_send_error
            )
            print("Added callbacks to ProtoOASubscribeSpotsReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_subscribe_spots_request send command: {e}")
            self._last_error = f"Exception sending spot subscription: {e}"


    # Public API
    def connect(self) -> bool:
        """
        Establishes a connection to the trading service.
        Handles OAuth2 flow (token loading, refresh, or full browser authentication)
        and then starts the underlying OpenAPI client service.

        Returns:
            True if connection setup (including successful client service start) is successful,
            False otherwise.
        """
        if not USE_OPENAPI_LIB:
            print("Mock mode: OpenAPI library unavailable.")
            self._last_error = "OpenAPI library not available (mock mode)."
            return False

        # 1. Check if loaded token is valid and not expired
        self._last_error = "Checking saved tokens..." # For GUI
        if self._access_token and not self._is_token_expired():
            print("Using previously saved, valid access token.")
            self._last_error = "Attempting to connect with saved token..." # For GUI
            if self._start_openapi_client_service():
                return True # Proceed with this token
            else:
                # Problem starting client service even with a seemingly valid token
                # self._last_error is set by _start_openapi_client_service
                print(f"Failed to start client service with saved token: {self._last_error}")
                # Fall through to try refresh or full OAuth
                self._access_token = None # Invalidate to ensure we don't retry this path immediately

        # 2. If token is expired or (now) no valid token, try to refresh if possible
        if self._refresh_token: # Check if refresh is even possible
            if self._access_token and self._is_token_expired(): # If previous step invalidated or was originally expired
                print("Saved access token is (or became) invalid/expired, attempting refresh...")
            elif not self._access_token: # No access token from file, but refresh token exists
                 print("No saved access token, but refresh token found. Attempting refresh...")

            self._last_error = "Attempting to refresh token..." # For GUI
            if self.refresh_access_token(): # This also saves the new token
                print("Access token refreshed successfully using saved refresh token.")
                self._last_error = "Token refreshed. Attempting to connect..." # For GUI
                if self._start_openapi_client_service():
                    return True # Proceed with refreshed token
                else:
                    # self._last_error set by _start_openapi_client_service
                    print(f"Failed to start client service after token refresh: {self._last_error}")
                    return False # Explicitly fail here if client service fails after successful refresh
            else:
                print("Failed to refresh token. Proceeding to full OAuth flow.")
                # self._last_error set by refresh_access_token(), fall through to full OAuth
        else:
            print("No refresh token available. Proceeding to full OAuth if needed.")


        # 3. If no valid/refreshed token, proceed to full OAuth browser flow
        print("No valid saved token or refresh failed/unavailable. Initiating full OAuth2 flow...")
        self._last_error = "OAuth2: Redirecting to browser for authentication." # More specific initial status

        auth_url = self.settings.openapi.spotware_auth_url
        # token_url = self.settings.openapi.spotware_token_url # Not used in this part of connect()
        client_id = self.settings.openapi.client_id
        redirect_uri = self.settings.openapi.redirect_uri
        # Define scopes - this might need adjustment based on Spotware's requirements
        scopes = "trading" # Changed from "trading accounts" to just "accounts" based on OpenApiPy example hint

        # Construct the authorization URL using the new Spotware URL
        # Construct the authorization URL using the standard Spotware OAuth endpoint.
        params = {
            "response_type": "code", # Required for Authorization Code flow
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes
            # product="web" is removed as it's not part of standard OAuth params here
            # "state": "YOUR_UNIQUE_STATE_HERE" # Optional: for CSRF protection
        }
        auth_url_with_params = f"{auth_url}?{urllib.parse.urlencode(params)}"

        # Start local HTTP server
        if not self._start_local_http_server():
            self._last_error = "OAuth2 Error: Could not start local HTTP server for callback."
            print(self._last_error)
            return False # Indicate connection failed

        print(f"Redirecting to browser for authentication: {auth_url_with_params}")
        webbrowser.open(auth_url_with_params)
        self._last_error = "OAuth2: Waiting for authorization code via local callback..." # Update status

        # Wait for the auth code from the HTTP server (with a timeout)
        try:
            auth_code = self._auth_code_queue.get(timeout=120) # 2 minutes timeout
            print("Authorization code received from local server.")
        except queue.Empty:
            print("OAuth2 Error: Timeout waiting for authorization code from callback.")
            self._last_error = "OAuth2 Error: Timeout waiting for callback."
            self._stop_local_http_server()
            return False

        self._stop_local_http_server()

        if auth_code:
            return self.exchange_code_for_token(auth_code)
        else: # Should not happen if queue contained None or empty string but good to check
            self._last_error = "OAuth2 Error: Invalid authorization code received."
            print(self._last_error)
            return False


    def _start_local_http_server(self) -> bool:
        """
        Starts a local HTTP server on a separate thread to listen for the OAuth callback.
        The server address is determined by self.settings.openapi.redirect_uri.

        Returns:
            True if the server started successfully, False otherwise.
        """
        try:
            # Ensure any previous server is stopped
            if self._http_server_thread and self._http_server_thread.is_alive():
                self._stop_local_http_server()

            # Use localhost and port from redirect_uri
            parsed_uri = urllib.parse.urlparse(self.settings.openapi.redirect_uri)
            host = parsed_uri.hostname
            port = parsed_uri.port

            if not host or not port:
                print(f"Invalid redirect_uri for local server: {self.settings.openapi.redirect_uri}")
                return False

            # Pass the queue to the handler
            def handler_factory(*args, **kwargs):
                return OAuthCallbackHandler(*args, auth_code_queue=self._auth_code_queue, **kwargs)

            self._http_server = HTTPServer((host, port), handler_factory)
            self._http_server_thread = threading.Thread(target=self._http_server.serve_forever, daemon=True)
            self._http_server_thread.start()
            print(f"Local HTTP server started on {host}:{port} for OAuth callback.")
            return True
        except Exception as e:
            print(f"Failed to start local HTTP server: {e}")
            self._last_error = f"Failed to start local HTTP server: {e}"
            return False

    def _stop_local_http_server(self):
        if self._http_server:
            print("Shutting down local HTTP server...")
            self._http_server.shutdown() # Signal server to stop serve_forever loop
            self._http_server.server_close() # Close the server socket
            self._http_server = None
        if self._http_server_thread and self._http_server_thread.is_alive():
            self._http_server_thread.join(timeout=5) # Wait for thread to finish
            if self._http_server_thread.is_alive():
                print("Warning: HTTP server thread did not terminate cleanly.")
        self._http_server_thread = None
        print("Local HTTP server stopped.")


    def exchange_code_for_token(self, auth_code: str) -> bool:
       
        print(f"Exchanging authorization code for token: {auth_code[:20]}...") # Log part of the code
        self._last_error = ""
        try:
            token_url = self.settings.openapi.spotware_token_url
            payload = {
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": self.settings.openapi.redirect_uri,
                "client_id": self.settings.openapi.client_id,
                "client_secret": self.settings.openapi.client_secret,
            }
            response = requests.post(token_url, data=payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

            token_data = response.json()

            if "access_token" not in token_data:
                self._last_error = "OAuth2 Error: access_token not in response from token endpoint."
                print(f"{self._last_error} Response: {token_data}")
                return False

            self._access_token = token_data["access_token"]
            self._refresh_token = token_data.get("refresh_token") # refresh_token might not always be present
            expires_in = token_data.get("expires_in")
            if expires_in:
                self._token_expires_at = time.time() + int(expires_in)
            else:
                self._token_expires_at = None # Or a very long time if not specified

            print(f"Access token obtained: {self._access_token[:20]}...")
            if self._refresh_token:
                print(f"Refresh token obtained: {self._refresh_token[:20]}...")
            print(f"Token expires in: {expires_in} seconds (at {self._token_expires_at})")

            # Now that we have the access token, we can start the actual OpenAPI client service
            self._save_tokens_to_file() # Save tokens after successful exchange
            if self._start_openapi_client_service():
                # Connection to TCP endpoint will now proceed, leading to ProtoOAApplicationAuthReq etc.
                # The _check_connection in GUI will handle the rest.
                return True
            else:
                # _start_openapi_client_service would have set _last_error
                return False

        except requests.exceptions.HTTPError as http_err:
            error_content = http_err.response.text
            self._last_error = f"OAuth2 HTTP Error: {http_err}. Response: {error_content}"
            print(self._last_error)
            return False
        except requests.exceptions.RequestException as req_err:
            self._last_error = f"OAuth2 Request Error: {req_err}"
            print(self._last_error)
            return False
        except Exception as e:
            self._last_error = f"OAuth2 Unexpected Error during token exchange: {e}"
            print(self._last_error)
            return False

    def _start_openapi_client_service(self):
        """
        Starts the underlying OpenAPI client service (TCP connection, reactor).
        This is called after successful OAuth token acquisition/validation.

        Returns:
            True if the client service started successfully, False otherwise.
        """
        if self.is_connected or (self._client and getattr(self._client, 'isConnected', False)):
            print("OpenAPI client service already running or connected.")
            return True

        print("Starting OpenAPI client service.")
        try:
            self._client.startService()
            if _reactor_installed and not reactor.running:
                self._reactor_thread = threading.Thread(target=lambda: reactor.run(installSignalHandlers=0), daemon=True)
                self._reactor_thread.start()
            return True
        except Exception as e:
            print(f"Error starting OpenAPI client service: {e}")
            self._last_error = f"OpenAPI client error: {e}"
            return False

    def refresh_access_token(self) -> bool:
        """
        Refreshes the OAuth access token using the stored refresh token.
        Saves the new tokens to file on success.

        Returns:
            True if the access token was refreshed successfully, False otherwise.
        """
        if not self._refresh_token:
            self._last_error = "OAuth2 Error: No refresh token available to refresh access token."
            print(self._last_error)
            return False

        print("Attempting to refresh access token...")
        self._last_error = ""
        try:
            token_url = self.settings.openapi.spotware_token_url
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self.settings.openapi.client_id,
                "client_secret": self.settings.openapi.client_secret, # Typically required for refresh
            }
            response = requests.post(token_url, data=payload)
            response.raise_for_status()

            token_data = response.json()

            if "access_token" not in token_data:
                self._last_error = "OAuth2 Error: access_token not in response from refresh token endpoint."
                print(f"{self._last_error} Response: {token_data}")
                # Potentially invalidate old tokens if refresh fails this way
                self.is_connected = False
                return False

            self._access_token = token_data["access_token"]
            # A new refresh token might be issued, or the old one might continue to be valid.
            # Standard practice: if a new one is issued, use it. Otherwise, keep the old one.
            if "refresh_token" in token_data:
                self._refresh_token = token_data["refresh_token"]
                print(f"New refresh token obtained: {self._refresh_token[:20]}...")

            expires_in = token_data.get("expires_in")
            if expires_in:
                self._token_expires_at = time.time() + int(expires_in)
            else:
                # If expires_in is not provided on refresh, it might mean the expiry doesn't change
                # or it's a non-expiring token (less common). For safety, clear old expiry.
                self._token_expires_at = None

            print(f"Access token refreshed successfully: {self._access_token[:20]}...")
            print(f"New expiry: {self._token_expires_at}")
            self._save_tokens_to_file() # Save tokens after successful refresh
            return True

        except requests.exceptions.HTTPError as http_err:
            error_content = http_err.response.text
            self._last_error = f"OAuth2 HTTP Error during token refresh: {http_err}. Response: {error_content}"
            print(self._last_error)
            self.is_connected = False # Assume connection is lost if refresh fails
            return False
        except requests.exceptions.RequestException as req_err:
            self._last_error = f"OAuth2 Request Error during token refresh: {req_err}"
            print(self._last_error)
            self.is_connected = False
            return False
        except Exception as e:
            self._last_error = f"OAuth2 Unexpected Error during token refresh: {e}"
            print(self._last_error)
            self.is_connected = False
            return False

    def _is_token_expired(self, buffer_seconds: int = 60) -> bool:
        """
        Checks if the current OAuth access token is expired or nearing expiry.

        Args:
            buffer_seconds: A buffer time in seconds. If the token expires within
                            this buffer, it's considered nearing expiry.

        Returns:
            True if the token is non-existent, expired, or nearing expiry, False otherwise.
        """
        if not self._access_token:
            return True # No token means it's effectively expired for use
        if self._token_expires_at is None:
            return False # Token that doesn't expire (or expiry unknown)
        return time.time() > (self._token_expires_at - buffer_seconds)

    def _ensure_valid_token(self) -> bool:
       
        if self._is_token_expired():
            print("Access token expired or nearing expiry. Attempting refresh.")
            if not self.refresh_access_token():
                print("Failed to refresh access token.")
                # self._last_error is set by refresh_access_token()
                if self._client and self._is_client_connected: # Check if client exists and was connected
                    self._client.stopService() # Stop service if token cannot be refreshed
                self.is_connected = False
                return False
        return True


    def disconnect(self) -> None:
        if self._client:
            self._client.stopService()
        if _reactor_installed and reactor.running:
            reactor.callFromThread(reactor.stop)
        self.is_connected = False
        self._is_client_connected = False

    def get_connection_status(self) -> Tuple[bool, str]:
        return self.is_connected, self._last_error

    def get_account_summary(self) -> Dict[str, Any]:
        if not USE_OPENAPI_LIB:
            return {"account_id": "MOCK", "balance": 0.0, "equity": 0.0, "margin": 0.0}

        return {
            "account_id": self.account_id if self.account_id else "connecting...",
            "balance": self.balance,
            "equity": self.equity,
            "margin": self.used_margin
        }

    def get_market_price(self, symbol: str) -> Optional[float]:
            """
            Returns the most recent tick price for any subscribed symbol,
            or None if we haven’t yet received a quote.
            """
            price = self.latest_prices.get(symbol)
            if price is None:
                print(f"Price for {symbol} not available yet.")
            return price

    def get_price_history(self) -> List[float]:
        return list(self.price_history)

    def get_ohlc_bar_counts(self) -> Dict[str, int]:
        """Returns a dictionary with the count of available OHLC bars for each timeframe."""
        counts = {}
        for tf_str, df_history in self.ohlc_history.items():
            # df_history is a DataFrame, len(df_history) gives the number of rows (bars)
            counts[tf_str] = len(df_history)
        return counts

    def close_all_positions(self) -> None:
        """Fetch all open positions (via reconcile) then close each one."""
        if not (self.is_connected and self._client and self._is_client_connected):
            print("Cannot close positions: not connected.")
            return

        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        try:
            d = self._client.send(req)
            d.addCallbacks(self._handle_reconcile_response, self._handle_send_error)
            print("Requested reconcile (positions + orders).")
        except Exception as e:
            print(f"Error sending reconcile request: {e}")

    def _handle_reconcile_response(self, response_wrapper: Any) -> None:
        """Called when ProtoOAReconcileRes arrives—iterate open positions and close them."""
        # Unwrap if wrapped in ProtoMessage
        if isinstance(response_wrapper, ProtoMessage):
            actual = Protobuf.extract(response_wrapper)
        else:
            actual = response_wrapper

        if not isinstance(actual, ProtoOAReconcileRes):
            print(f"Unexpected reconcile response: {type(actual)}")
            return

        if not actual.position:
            print("No open positions to close.")
            return

        for pos in actual.position:
            pid = pos.positionId
            print(f"Closing position {pid}…")

            # Build the close‐position request
            close_req = ProtoOAClosePositionReq()
            close_req.ctidTraderAccountId = self.ctid_trader_account_id
            close_req.positionId          = pid

            # ❗ Must supply the volume field or serialization fails:
            # Try pos.volume first, then pos.tradeData.volume if available
            if hasattr(pos, 'volume'):
                close_req.volume = pos.volume
            elif hasattr(pos, 'tradeData') and hasattr(pos.tradeData, 'volume'):
                close_req.volume = pos.tradeData.volume
            else:
                print(f"  ⚠️ Cannot determine volume for position {pid}, skipping.")
                continue

            try:
                # Send and give up to 30s before timing out
                d2 = self._client.send(close_req)
                d2.addTimeout(30, reactor)
                d2.addCallbacks(
                    # on success
                    lambda resp, pid=pid: print(f"  ✅ Position {pid} closed."),
                    # on failure (including TimeoutError)
                    lambda failure, pid=pid: print(
                        f"  ❌ Error closing {pid}: "
                        f"{failure.getErrorMessage() if hasattr(failure, 'getErrorMessage') else failure}"
                    )
                )
            except Exception as e:
                print(f"Exception sending close for {pid}: {e}")


    def place_market_order(
        self,
        symbol_name: str,
        side: str,                     
        volume_lots: float,           # ← now *required*, user’s lots
        stop_loss_pips: float = 2.0,
        take_profit_pips: Optional[float] = None,
        client_msg_id: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Place a market order for `symbol_name` with exactly `volume_lots` lots,
        plus SL/TP in pips.
        """
        if not (self.is_connected and self._client and self._is_client_connected):
            return False, "Not connected to the trading platform."

        self._last_order_symbol = symbol_name

        if not self.ctid_trader_account_id:
            return False, "Account information not available."

        # 1) Look up symbol
        symbol_id = self.symbols_map.get(symbol_name)
        if not symbol_id:
            return False, f"Symbol '{symbol_name}' not found."
        symbol_details = self.symbol_details_map.get(symbol_id)
        if not symbol_details:
            return False, f"Symbol details for '{symbol_name}' not loaded."

        # 2) Convert user’s lots → “units” the API expects
        #    (e.g. 1 lot = symbol_details.lotSize units)
        volume_in_units = int(volume_lots * symbol_details.lotSize)

        # 3) Enforce min/max/step constraints
        min_v, max_v, step_v = (
            symbol_details.minVolume,
            symbol_details.maxVolume,
            symbol_details.stepVolume
        )
        # Round down to nearest step first:
        volume_in_units = (volume_in_units // step_v) * step_v
        # Then clamp:
        volume_in_units = max(min_v, min(volume_in_units, max_v))

        if volume_in_units <= 0:
            return False, "Computed volume is zero after applying broker constraints."

        # 4) Build & send the order
        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId  = self.ctid_trader_account_id
        req.symbolId             = symbol_id
        req.orderType            = ProtoOAOrderType.MARKET
        req.tradeSide            = (
            ProtoOATradeSide.BUY if side.upper() == "BUY"
            else ProtoOATradeSide.SELL
        )
        req.volume               = volume_in_units
        req.comment              = f"User‑specified lot size: {volume_lots}"
        req.relativeStopLoss     = int(stop_loss_pips * (10 ** symbol_details.pipPosition))
        if take_profit_pips is not None:
            req.relativeTakeProfit = int(take_profit_pips * (10 ** symbol_details.pipPosition))
        if client_msg_id:
            req.clientOrderId     = client_msg_id

        try:
            d = self._client.send(req)
            d.addCallbacks(
                lambda resp: print(f"Order sent successfully. Response: {resp}"),
                self._handle_send_error
            )
            return True, f"Order for {volume_in_units} units ({volume_lots} lots) of {symbol_name} placed."
        except Exception as e:
            return False, f"Exception placing order: {e}"


            
    def _handle_execution_event(self, event: ProtoOAExecutionEvent) -> None:
        # TODO: handle executions
        print(f"Received ProtoOAExecutionEvent: {event}")
 

        # Example: update internal order status, notify GUI, log P&L.
        order_info = event.order
        position_info = event.position # if position was opened/closed/modified

        log_msg = f"Execution Event: Type={ProtoOAExecutionType.Name(event.executionType)}"
        if hasattr(order_info, 'orderId') and order_info.orderId:
            log_msg += f", OrderID={order_info.orderId}"
        if hasattr(order_info, 'clientOrderId') and order_info.clientOrderId: # This is clientMsgId
             log_msg += f", ClientMsgID={order_info.clientOrderId}"

        if event.executionType == ProtoOAExecutionType.ORDER_FILLED:
            log_msg += (f", Status={ProtoOAOrderStatus.Name(order_info.orderStatus)}"
                        f", FilledVol={order_info.executedVolume}/{order_info.tradeData.volume}"
                        f" at {order_info.executionPrice}")
            # Here you would update P&L, trade counts, etc.
            # Potentially signal to the GUI that the trade was successful.

        elif event.executionType == ProtoOAExecutionType.ORDER_REJECTED:
            log_msg += f", Reason={event.errorCode if hasattr(event, 'errorCode') else 'Unknown'}" # Use .errorCode for rejection reason
            if hasattr(event, 'description') and event.description: # Check if description field exists and is not empty
                log_msg += f" - {event.description}"
            # Signal to GUI about rejection

        # Add more handling for other execution types:
        # ORDER_ACCEPTED, ORDER_CANCELLED, ORDER_EXPIRED, etc.

        print(log_msg)

        # If you have a way to notify the GUI (e.g., through a queue or callback):
        # self.gui_update_queue.put(("execution_update", log_msg)) # Example


    def _send_get_trendbars_request(
        self,
        symbol_id: int,
        period: ProtoOATrendbarPeriod,
        count: int
    ) -> None:
        if not self._ensure_valid_token():
            return
        if not self._client or not self._is_client_connected:
            self._last_error = "Cannot get trendbars: Client not connected."
            print(self._last_error)
            return
        if not self.ctid_trader_account_id:
            self._last_error = "Cannot get trendbars: ctidTraderAccountId is not set."
            print(self._last_error)
            return

        print(f"Requesting {count} trendbars for symbol ID {symbol_id}, period {ProtoOATrendbarPeriod.Name(period)}")

        req = ProtoOAGetTrendbarsReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        req.symbolId = symbol_id
        req.period = period

        # Compute timestamps
        to_timestamp = int(time.time() * 1000)

        period_seconds = TREND_BAR_PERIOD_SECONDS.get(period)
        if period_seconds is None:
            self._last_error = f"Unsupported trendbar period: {period}"
            print(self._last_error)
            return

        from_timestamp = to_timestamp - (count * period_seconds * 1000)

        req.fromTimestamp = from_timestamp
        req.toTimestamp = to_timestamp
        req.count = count  # Optional, but safe to include

        print(
            f"Sending ProtoOAGetTrendbarsReq:\n"
            f"  fromTimestamp: {from_timestamp}\n"
            f"  toTimestamp:   {to_timestamp}\n"
            f"  period:        {ProtoOATrendbarPeriod.Name(period)}\n"
            f"  symbolId:      {symbol_id}\n"
            f"  ctidTraderAccountId: {self.ctid_trader_account_id}"
        )

        try:
            d = self._client.send(req)
            d.addErrback(self._handle_send_error)
            print("Added errback to ProtoOAGetTrendbarsReq Deferred.")
        except Exception as e:
            print(f"Exception during _send_get_trendbars_request send command: {e}")
            self._last_error = f"Exception sending trendbars request: {e}"


    def _handle_get_trendbars_response(self, response: ProtoOAGetTrendbarsRes) -> None:
        """Processes ProtoOAGetTrendbarsRes into OHLC DataFrame with a proper Timestamp index."""
        print(f"Received ProtoOAGetTrendbarsRes for symbol ID {response.symbolId}, "
            f"period {ProtoOATrendbarPeriod.Name(response.period)} with {len(response.trendbar)} bars.")

        symbol_id = response.symbolId
        # Map enum to your timeframe key
        tf_str_map = {
            ProtoOATrendbarPeriod.M1: '1m',
            ProtoOATrendbarPeriod.M5: '5m',
            # etc…
        }
        tf_str = tf_str_map.get(response.period)
        if not tf_str:
            print(f"Warning: Unmapped period {ProtoOATrendbarPeriod.Name(response.period)}.")
            return

        details = self.symbol_details_map.get(symbol_id)
        if not details or not hasattr(details, 'digits'):
            print(f"Warning: Symbol details missing for {symbol_id}. Cannot scale prices.")
            return

        scale = float(10 ** details.digits)
        bars = []
        for bar in response.trendbar:
            # bar.utcTimestampInMinutes is minutes since epoch
            ts = pd.to_datetime(bar.utcTimestampInMinutes * 60 * 1000, unit='ms', utc=True)
            low    = bar.low   / scale
            open_  = (bar.low + bar.deltaOpen)  / scale
            high   = (bar.low + bar.deltaHigh)  / scale
            close  = (bar.low + bar.deltaClose) / scale
            bars.append({
                'timestamp': ts,
                'open':  open_,
                'high':  high,
                'low':   low,
                'close': close,
                'volume': bar.volume
            })

        if not bars:
            print(f"No trendbars in response for {tf_str}.")
            return

        df = pd.DataFrame(bars)
        # 🔥 NEW: set index to the timestamp column
        df.set_index('timestamp', inplace=True)
        df.index.name = 'timestamp'

        # Replace your history and reset the live‐bar accumulator
        self.ohlc_history[tf_str] = df
        print(f"Populated {tf_str} history: {len(df)} bars. Last bar at {df.index[-1]}")
        self.current_bars[tf_str] = {
            'timestamp': None, 'open': None, 'high': None,
            'low': None, 'close': None, 'volume': 0
        }
