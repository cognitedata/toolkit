import os


def get_cicd_environment() -> str:
    if "CI" in os.environ and os.getenv("GITHUB_ACTIONS"):
        return "github"
    if os.getenv("GITLAB_CI"):
        return "gitlab"
    if "CI" in os.environ and "BITBUCKET_BUILD_NUMBER" in os.environ:
        return "bitbucket"
    if os.getenv("CIRCLECI"):
        return "circleci"
    if os.getenv("TRAVIS"):
        return "travis"
    if "TF_BUILD" in os.environ:
        return "azure"
    if "BUILD_ID" in os.environ:
        return "google-cloud-build"

    return "local"
