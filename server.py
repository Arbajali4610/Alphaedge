from flask import Flask, jsonify, request, send_from_directory
import os
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=BASE_DIR)

# Delayed-market upstream used by AlphaEdge.
UPSTREAM = os.environ.get("MARKET_UPSTREAM", "http://65.0.104.9/stock/list")

@app.get("/")
def home():
    return send_from_directory(BASE_DIR, "index.html")

@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "AlphaEdge"})

@app.get("/api/stocks")
def stocks():
    symbols = request.args.get("symbols", "RELIANCE,TCS,HDFCBANK,INFY")
    try:
        response = requests.get(
            UPSTREAM,
            params={"symbols": symbols, "res": "num"},
            timeout=10,
        )
        response.raise_for_status()
        return jsonify(response.json())
    except requests.RequestException as exc:
        return jsonify({
            "status": "error",
            "message": "Market data provider is temporarily unavailable.",
            "detail": str(exc),
        }), 502
    except ValueError:
        return jsonify({
            "status": "error",
            "message": "Market data provider returned invalid JSON.",
        }), 502

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
