docker run -d --rm \
  -v ./sample-data:/app/starlink/data \
  leoviz:starlink \
  poetry run python3 plot.py --id 2025-05-29-23-20-13

sleep 120

stat ./sample-data/starlink-2025-05-29-23-20-13.mp4

if [ $? -ne 0 ]; then
  echo "Error: Video file not found or not created."
  exit 1
fi
