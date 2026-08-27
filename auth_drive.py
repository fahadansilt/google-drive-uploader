"""One-time Google Drive consent -> writes token.json.

Headless VPS? Either run this on your laptop and scp token.json across, or
forward the port first:   ssh -L 8080:localhost:8080 user@vps
then run this on the VPS and open the printed URL in your local browser.
"""
from google_auth_oauthlib.flow import InstalledAppFlow

import config
from drive import save_credentials

CLIENT_CONFIG = {
    "installed": {
        "client_id": config.GOOGLE_CLIENT_ID,
        "client_secret": config.GOOGLE_CLIENT_SECRET,
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "redirect_uris": ["http://localhost"],
    }
}


def main():
    flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, config.SCOPES)
    creds = flow.run_local_server(
        port=8080,
        access_type="offline",
        prompt="consent",  # forces a refresh_token even on re-auth
        open_browser=False,
    )
    if not creds.refresh_token:
        raise SystemExit("No refresh token returned. Revoke the app's access and retry.")
    save_credentials(creds)
    print(f"Saved {config.GOOGLE_TOKEN_FILE}")


if __name__ == "__main__":
    main()
