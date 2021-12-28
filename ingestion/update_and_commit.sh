. venv/bin/activate
git pull
python ingestion/sink.py
f="data_update_$(date +"%d%m%y%H%M%S")"
git commit ingestion/data -m f
git push
