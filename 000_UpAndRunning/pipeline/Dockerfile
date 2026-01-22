# 1. Use an existing image as a base (e.g., Python, Node, PHP)
FROM python:3.13.11-slim
# FROM python:3.13.11-slim

#include uv as a dependency - via from variable
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# 2. Set the working directory inside the container
# WORKDIR /app
WORKDIR /code

# # Optionally, create and activate a virtual environment - if active, would not need to use uv to run
ENV PATH="/code/.venv/bin:$PATH"

# 3. Copy your dependency file first (optimizes building)
# COPY requirements.txt .
COPY pyproject.toml .python-version uv.lock ./

# 4. Install dependencies - note: will not install dev dependencies
# RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install pandas pyarrow
RUN uv sync --locked

# 5. Copy the rest of your project code - if desired
# COPY . ./
# COPY pipeline.py ./ - moved to ingest_data to create table and insert data
COPY ingest_data.py .

# 6. Tell the container which port to let traffic through

# 7. The command to run your app
# CMD []

# 8. Specify the entry point for the container - command that runs when container starts
# ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
# ENTRYPOINT ["python", "pipeline.py"]
ENTRYPOINT ["python", "ingest_data.py"]