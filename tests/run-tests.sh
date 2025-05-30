docker run -d \
  -v ./sample-data:/app/starlink/data \
  clarkzjw/leoviz:starlink \
  poetry run python3 plot.py --id 2025-04-15-01-31-27

sleep 120

stat ./sample-data/starlink-2025-04-15-01-31-27.mp4

if [ $? -ne 0 ]; then
  echo "Error: Video file not found or not created."
  exit 1
fi
