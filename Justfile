set shell := ["bash", "-c"]

@help:
  just --list --unsorted

#
# Dev setup.
#

@fmt:
  black .
  isort .

@lint:
  black --check .

@clean:
  find . | grep -E "(__pycache__|\.pyc|\.pyo)" | xargs rm -rf

@piptools:
  pip install -U pip-tools

@setup-dev: piptools
  pip-sync dev-requirements.txt


#
# Flyte integration.
#

workflow_version := `cat latch/version`
client_id := "flytepropeller"
secret_path := "/root/client_secret.txt"

app_name := "guideseq_python2"
docker_registry := "812206152185.dkr.ecr.us-west-2.amazonaws.com"
docker_image_version := `inp=$(cat latch/version); echo "${inp//+/_}"`
docker_image_prefix := docker_registry + "/" + app_name
docker_image_full := docker_image_prefix + ":" + docker_image_version

def_environment := "latch.bio"
def_project := "50"
def_endpoint :=  "admin.flyte." + def_environment + ":81"
def_nucleus_endpoint := "https://nucleus." + def_environment

@docker-login:
  aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin {{docker_registry}}

@docker-build:
  docker build --build-arg tag={{docker_image_full}} --build-arg nucleus_endpoint={{def_nucleus_endpoint}} -t {{docker_image_full}} .

@docker-push:
  docker push {{docker_image_full}}

@dbnp: docker-login docker-build docker-push

#
# Entrypoint.
#

register project=def_project endpoint=def_endpoint: dbnp
  docker run -i --rm \
    -e REGISTRY={{docker_registry}} \
    -e PROJECT={{project}} \
    -e VERSION={{workflow_version}} \
    -e FLYTE_ADMIN_ENDPOINT={{endpoint}} \
    -e FLYTE_CLIENT_ID={{client_id}} \
    -e FLYTE_SECRET_PATH={{secret_path}} \
    {{docker_image_full}} make register

test project=def_project endpoint=def_endpoint:
  docker run -i --rm \
    -e REGISTRY={{docker_registry}} \
    -e PROJECT={{project}} \
    -e VERSION={{workflow_version}} \
    -e FLYTE_ADMIN_ENDPOINT={{endpoint}} \
    -e FLYTE_CLIENT_ID={{client_id}} \
    -e FLYTE_SECRET_PATH={{secret_path}} \
    -e AWS_ACCESS_KEY_ID={{env_var("AWS_ACCESS_KEY_ID")}} \
    -e AWS_SECRET_ACCESS_KEY={{env_var("AWS_SECRET_ACCESS_KEY")}} \
    {{docker_image_full}} make test
