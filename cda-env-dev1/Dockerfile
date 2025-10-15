FROM python:3.11.9 AS build_package_whl

ENV PROJECT_ID="1209"
ARG GITLAB_API_TOKEN

COPY ./glue_scripts ./glue_scripts
RUN mkdir scripts
RUN curl -sS --insecure --header "PRIVATE-TOKEN: $GITLAB_API_TOKEN" \
"https://git.mmi.mig.corp/api/v4/projects/$PROJECT_ID/repository/files/scripts%2Fbuild_package_whl.py/raw?ref=main" \
-o "scripts/build_package_whl.py"
RUN curl -sS --insecure --header "PRIVATE-TOKEN: $GITLAB_API_TOKEN" \
"https://git.mmi.mig.corp/api/v4/projects/$PROJECT_ID/repository/files/scripts%2Frequirements.txt/raw?ref=main" \
-o "scripts/requirements.txt"
RUN pip install -r scripts/requirements.txt
RUN python scripts/build_package_whl.py build-all-package-whls

FROM nexus3.mmi.mig.corp:5000/encovatrivy:latest AS tf_install

# Terraform version is parsed from source code by Jenkins. Assumption is made that the required_version tag is defined within a backend.tf file in the repository root
ARG TF_BINARY_VERSION

# Install official apt binaries. Setup for this exists within the enconvatrivy image
# https://developer.hashicorp.com/terraform/cli/install/apt#installing-a-specific-version-of-terraform
RUN set -eux; \
  apt-get update; \
  apt-get install --no-install-recommends -qy terraform=${TF_BINARY_VERSION}-*; \
  apt-get -qy clean; \
  rm -rf /var/lib/apt/lists/*; \
  terraform version;

# Initialize the project to s3 backend and run validate for syntax issues
FROM tf_install AS tf_plan

# This value should always map to the tfvars filename, workspace name, and backend config filename we use in a deployment
ARG WORKLOAD_ENV
ARG DEPLOYMENT_ROLE

WORKDIR /home/migadmin

ENV \
  AWS_SHARED_CREDENTIALS_FILE=/run/secrets/credentials \
  AWS_REGION="us-east-1" \
  TF_IN_AUTOMATION=true \
  #TF_CLI_ARGS="-no-color" \
  TF_VAR_cda_deployment_role=${DEPLOYMENT_ROLE}

COPY . .
COPY --from=build_package_whl dist dist

RUN --mount=type=secret,id=credentials \
  terraform init --backend-config=backends/${WORKLOAD_ENV}.s3.tfbackend; \
  terraform validate; \
  terraform plan -var-file=env/${WORKLOAD_ENV}.tfvars -out tfplan

FROM tf_plan AS tf_scan

ARG VERSION
ARG GIT_COMMIT
ARG BUILD_DATE

LABEL com.encova.rm.image.version=$VERSION \
      com.encova.rm.image.revision=$GIT_COMMIT \
      com.encova.rm.image.created=$BUILD_DATE \
      com.encova.rm.image.source="https://git.mmi.mig.corp/aws/data-services/cda.git"

USER migadmin

RUN trivy fs \
  --scanners secret,misconfig \
  --offline-scan \
  --format table \
  --exit-code 0 \
  ./ | tee -a ./tfscan
