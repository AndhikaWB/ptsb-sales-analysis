# Use Google Cloud CLI image as base
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:stable
# Copy our data to working directory
WORKDIR /work
COPY docker/data .
# Copy also ~/.bigqueryrc and ~/.config/gcloud
# To skip auth process if we already have the credentials
# See also https://cloud.google.com/docs/authentication/gcloud
COPY docker/root /root