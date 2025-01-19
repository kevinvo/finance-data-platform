FROM public.ecr.aws/lambda/python:3.9

# Install gcc and python development files needed for building some packages
RUN yum install -y gcc python3-devel

WORKDIR /lambda-layer

# Copy requirements file
COPY lambda/requirements.txt requirements.txt

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

# Clean up unnecessary files to reduce size
RUN cd python/ && \
    find . -type d -name "tests" -exec rm -rf {} + && \
    find . -type d -name "__pycache__" -exec rm -rf {} + && \
    find . -name "*.pyc" -delete && \
    find . -name "*.pyo" -delete && \
    find . -name "*.pyd" -delete && \
    find . -name "*.so" -delete && \
    find . -name "*.dist-info" -exec rm -rf {} + && \
    # Remove specific large packages/files that aren't needed
    rm -rf numpy/tests && \
    rm -rf pandas/tests && \
    rm -rf *.dist-info && \
    rm -rf __pycache__

# Make sure files are readable
RUN chmod -R 755 python/

# Copy the entire python directory
CMD cp -r python/. /output/ 