FROM public.ecr.aws/lambda/python:3.9

# Install gcc and python development files needed for building some packages
RUN yum install -y gcc python3-devel

WORKDIR /lambda-layer

COPY requirements.txt .

# Install numpy and pandas first, separately with specific flags
RUN mkdir -p python && \
    pip install \
    --platform manylinux2014_x86_64 \
    --target python \
    --implementation cp \
    --python-version 3.9 \
    --only-binary=:all: \
    numpy==1.22.4 \
    pandas==1.4.4

# Then install other dependencies
RUN pip install \
    --target python \
    -r requirements.txt

# Debug: List contents
# RUN ls -la python/

# Make sure files are readable
RUN chmod -R 755 python/

# Copy the entire python directory
CMD cp -r python/. /output/ 