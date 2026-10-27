FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir torch==2.8.0 --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir "chronos-forecasting==2.3.2" "transformers>=4.49,<5" einops safetensors numpy pandas
RUN pip install --no-cache-dir "timesfm @ git+https://github.com/google-research/timesfm@e51928e27119cb17bebc005be2696b75e0a9e688"
