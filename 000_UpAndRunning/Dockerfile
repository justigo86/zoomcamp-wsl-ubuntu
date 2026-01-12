# 1. Use an existing image as a base (e.g., Python, Node, PHP)
FROM python:slim

# 2. Set the working directory inside the container
# WORKDIR /app

# 3. Copy your dependency file first (optimizes building)
COPY requirements.txt .

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the rest of your project code
COPY . .

# 6. Tell the container which port to let traffic through

# 7. The command to run your app
# CMD []