# AlphaEdge

AlphaEdge is a Flask web app with a delayed-market data proxy.

## Run locally

```bash
pip install -r requirements.txt
python server.py
```

Open `http://127.0.0.1:5000`.

## Deploy on Render

1. Create a GitHub repository named `alphaedge`.
2. Upload the project files from this folder.
3. In Render, choose **New > Web Service** and connect the GitHub repository.
4. Build command: `pip install -r requirements.txt`
5. Start command: `gunicorn server:app --bind 0.0.0.0:$PORT`
6. Health check path: `/health`
7. Deploy.

The included `render.yaml` can also be used as the service blueprint.

## Market data

The browser calls `/api/stocks`; the Flask server calls the configured `MARKET_UPSTREAM` provider. This keeps the provider endpoint out of browser code. The default provider is the delayed-market endpoint used by this project.


### Phone preview
Open `index.html` after extracting the ZIP to preview the AlphaEdge form. Market prices require the Flask/Render server; the form itself remains usable in preview mode.


### WhatsApp
The enquiry form now uses direct navigation to the AlphaEdge WhatsApp chat, which is more reliable on Android/local HTML previews.
