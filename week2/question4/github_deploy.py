from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub

github_block = GitHub.load("github-block")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="github-flow",
    infrastructure=github_block,
)


if __name__ == "__main__":
    docker_dep.apply()