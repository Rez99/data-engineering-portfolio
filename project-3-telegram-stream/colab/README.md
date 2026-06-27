# Colab endpoint explorer

Upload `worldmonitor_endpoints_colab.ipynb` to [Google Colab](https://colab.research.google.com/) and run the cells top to bottom. The notebook uses independent cells so you can change a query, bounding box, or limit and rerun just that endpoint.

| Endpoint | Credentials | What it explores |
| --- | --- | --- |
| USGS Earthquake GeoJSON | None | Recent M4.5+ earthquakes |
| NASA EONET v3 | None | Open natural events and categories |
| GDELT DOC 2.0 | None | Recent news articles matching a query |
| NASA FIRMS | Free MAP_KEY | Satellite fire detections in a small bounding box |
| OpenSky states | Anonymous or token | Aircraft state vectors in a bounding box |

Never paste long-lived credentials into a notebook that you plan to share. In Colab, enter a NASA FIRMS map key through the `getpass` prompt, or use Colab Secrets for anything you want to retain privately.

The goal is source exploration, not a production connector. Each successful cell lets you inspect the response schema and choose a focused region/query before designing an ingestion adapter.

## Official YouTube Data API v3 explorer

`youtube_data_api_colab.ipynb` is a separate notebook for YouTube's official, read-only Data API endpoints: `videos.list`, `channels.list`, `playlistItems.list`, `search.list`, and `commentThreads.list`.

Before running it, create a Google Cloud project, enable **YouTube Data API v3**, and create an API key in Google Cloud Console. Enter the key through the notebook's hidden prompt; do not commit it or store it in a shared notebook. The official API can list caption tracks, but caption downloads require OAuth authorization from the video owner, so this notebook does not attempt to retrieve caption text.
