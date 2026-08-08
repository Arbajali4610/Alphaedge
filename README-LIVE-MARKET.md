# AlphaEdge — Live Market Integration

This version keeps the existing AlphaEdge page and adds a protected server-side market-feed layer.

## Provider
Upstox Market Data Feed V3 is used for the live feed. The browser never receives the Upstox access token.

## Render
1. Upload this folder to your GitHub repository.
2. In Render, create a Web Service from that repository.
3. Build command: `npm install`
4. Start command: `npm start`
5. Add Environment Variable:
   - `UPSTOX_ACCESS_TOKEN` = your Upstox access token
6. Deploy.

## Important
The live feed will remain unavailable until `UPSTOX_ACCESS_TOKEN` is configured. The site does not show fake live values when the feed is unavailable.

The token is a secret and must be stored only in Render Environment Variables, never inside `index.html` or GitHub.
