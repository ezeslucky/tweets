"""X (Twitter) OAuth 2.0 PKCE authentication engine.

Supports standard PKCE authorization code grant with SHA-256 challenges,
session token minting, and automated mock/developer consent for local verification.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import urllib.parse
from typing import Any, Dict, Optional, Tuple

import database

# Configuration from environment
X_AUTH_URL = "https://twitter.com/i/oauth2/authorize"
X_TOKEN_URL = "https://api.twitter.com/2/oauth2/token"
X_USERINFO_URL = "https://api.twitter.com/2/users/me"

USERNAME_REGEX = re.compile(r"^[a-zA-Z0-9_]{3,15}$")

def get_x_client_id() -> str:
    return os.getenv("X_CLIENT_ID", "").strip()

def get_x_client_secret() -> str:
    return os.getenv("X_CLIENT_SECRET", "").strip()

def get_default_redirect_uri() -> str:
    return os.getenv("X_REDIRECT_URI", "http://localhost:8000/api/auth/x/callback").strip()

def is_mock_mode() -> bool:
    explicit = os.getenv("X_MOCK_AUTH", "").lower()
    if explicit in ("1", "true", "yes"):
        return True
    client_id = get_x_client_id()
    # If client ID is empty, default placeholder, or contains 'placeholder', auto-enable mock mode
    return not client_id or "placeholder" in client_id.lower() or "test" in client_id.lower()

def generate_code_verifier(length: int = 64) -> str:
    """Generate high-entropy cryptographic random string for PKCE."""
    # 48 bytes urlsafe produces 64 base64 characters
    return secrets.token_urlsafe(48)[:length]

def generate_code_challenge(code_verifier: str) -> str:
    """Compute base64url-encoded SHA-256 digest of the code_verifier without padding."""
    digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")

def generate_state() -> str:
    """Generate cryptographically secure state token to prevent CSRF."""
    return secrets.token_urlsafe(24)

def validate_username_format(username: str) -> Tuple[bool, str]:
    """Validate username string against standard X/Twitter naming conventions."""
    clean = username.strip().lstrip("@")
    if not clean:
        return False, "Username cannot be empty."
    if len(clean) < 3:
        return False, "Username must be at least 3 characters."
    if len(clean) > 15:
        return False, "Username cannot exceed 15 characters."
    if not USERNAME_REGEX.match(clean):
        return False, "Username can only contain letters, numbers, and underscores."
    
    # Check reserved system words
    reserved = {"admin", "root", "system", "api", "support", "tweets", "login", "oauth"}
    if clean.lower() in reserved:
        return False, f"Username '@{clean}' is reserved. Please pick another."

    return True, ""

def create_authorization_request(
    redirect_uri: Optional[str] = None,
    mock: Optional[bool] = None,
) -> Tuple[str, str, str]:
    """Create OAuth 2.0 PKCE authorization request URL and persist single-use state in SQLite.
    
    Returns:
        (authorization_url, state, code_verifier)
    """
    effective_redirect_uri = redirect_uri or get_default_redirect_uri()
    state = generate_state()
    verifier = generate_code_verifier()
    challenge = generate_code_challenge(verifier)

    # Persist in SQLite
    database.save_oauth_state(state, verifier, effective_redirect_uri)

    use_mock = is_mock_mode() if mock is None else mock

    if use_mock:
        # Route to local interactive consent page for dev/test
        params = {
            "state": state,
            "code_challenge": challenge,
            "redirect_uri": effective_redirect_uri,
        }
        auth_url = f"/api/auth/x/mock-consent?{urllib.parse.urlencode(params)}"
    else:
        # Official X OAuth 2.0 PKCE URL
        params = {
            "response_type": "code",
            "client_id": get_x_client_id(),
            "redirect_uri": effective_redirect_uri,
            "scope": "tweet.read users.read offline.access",
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        auth_url = f"{X_AUTH_URL}?{urllib.parse.urlencode(params)}"

    return auth_url, state, verifier

async def exchange_code_for_user_profile(
    code: str,
    state: str,
) -> Dict[str, Any]:
    """Validate OAuth state, exchange authorization code using PKCE, and return X user profile.
    
    Returns:
        { "x_user_id": str, "x_username": str, "display_name": str }
    """
    # 1. Atomic state consumption
    stored_state = database.get_and_delete_oauth_state(state)
    if not stored_state:
        raise ValueError("Invalid or expired OAuth state parameter. Please try logging in again.")

    code_verifier = stored_state["code_verifier"]
    redirect_uri = stored_state["redirect_uri"]

    # 2. Check for mock code or mock mode
    if code.startswith("mock_") or is_mock_mode():
        # Deterministic mock user extraction from code suffix or fallback
        suffix = code.replace("mock_", "").strip() or "test"
        if "_" in suffix:
            parts = suffix.split("_", 1)
            raw_user = parts[0]
            raw_id = parts[1]
        else:
            raw_user = suffix
            raw_id = str(abs(hash(suffix)) % 10000000000)

        # Standard clean user profile
        x_user_id = f"x_{raw_id}"
        x_username = f"{raw_user}"
        display_name = raw_user.replace(".", " ").replace("-", " ").title()
        return {
            "x_user_id": x_user_id,
            "x_username": x_username,
            "display_name": display_name,
        }

    # 3. Live X OAuth 2.0 Token Exchange
    import urllib.request
    client_id = get_x_client_id()
    client_secret = get_x_client_secret()

    token_payload = {
        "code": code,
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_verifier": code_verifier,
    }
    encoded_data = urllib.parse.urlencode(token_payload).encode("utf-8")

    req = urllib.request.Request(
        X_TOKEN_URL,
        data=encoded_data,
        method="POST",
    )
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    if client_secret:
        # Basic auth with client_id:client_secret
        basic_creds = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {basic_creds}")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            token_res = json.loads(resp.read().decode("utf-8"))
            access_token = token_res.get("access_token")
    except Exception as e:
        raise ValueError(f"Failed to exchange OAuth code with X token endpoint: {e}")

    if not access_token:
        raise ValueError("No access token returned by X.")

    # 4. Fetch User Profile
    user_req = urllib.request.Request(
        f"{X_USERINFO_URL}?user.fields=id,name,username,profile_image_url",
        method="GET",
    )
    user_req.add_header("Authorization", f"Bearer {access_token}")

    try:
        with urllib.request.urlopen(user_req, timeout=15) as resp:
            user_res = json.loads(resp.read().decode("utf-8"))
            data = user_res.get("data", {})
    except Exception as e:
        raise ValueError(f"Failed to fetch authenticated X user profile: {e}")

    x_user_id = str(data.get("id"))
    x_username = str(data.get("username", "user"))
    display_name = data.get("name") or x_username

    return {
        "x_user_id": x_user_id,
        "x_username": x_username,
        "display_name": display_name,
    }
