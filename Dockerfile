# Docker file heavily inspired (basically copied) by
# https://sourcery.ai/blog/python-docker/

FROM python:3.12-slim as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1


FROM base AS python-deps

# Install pipenv
RUN pip install pipenv

# Install python dependencies in /.venv
COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy
#RUN PIPENV_VENV_IN_PROJECT=0 pipenv sync


FROM base AS runtime

# Copy virtual env from python-deps stage
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Create and switch to a new user
RUN useradd --create-home appuser
WORKDIR .
USER appuser

# Install application into container
COPY . .

# Run the application
# ENTRYPOINT python "entrypoint.py"

# docker run takeaway-challenge python src/entrypoint.py --task process_raw_reviews_data_without_timestamps
# docker run takeaway-challenge python src/entrypoint.py --task process_raw_metadata_without_timestamps
# docker run takeaway-challenge python src/entrypoint.py --task check_successful_completion
