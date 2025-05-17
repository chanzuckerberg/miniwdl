# miniwdl/tools_image

This subdirectory is the recipe for a Docker image bundling tools that miniwdl uses to provide certain functionality. End-users shouldn't deal with this image; it's used internally by miniwdl and does *not* include miniwdl itself.

For example, the image bundles [aria2c](https://aria2.github.io/), which miniwdl uses to download large input files supplied as URLs (without requiring end-users to install extra OS packages). Miniwdl synthesizes a WDL task with this image, which inputs the URL and outputs the desired file. The image is served publicly from GitHub Container Registry, referenced in the miniwdl configuration defaults (where it can be overridden if necessary).

This image doesn't change often, so we build it manually. First, authenticate your local `docker` CLI to GitHub Container Registry ([instructions](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-to-the-container-registry)) using a Personal Access Token with appropriate scope for the [miniwdl-ext organization](https://github.com/miniwdl-ext/). [Enable docker build --squash](https://stackoverflow.com/a/44346323/13393076) and,

```
docker pull ubuntu:20.04
docker build --no-cache --squash -t miniwdl-tools:latest tools_image/
TAG=$(docker inspect miniwdl-tools:latest | jq -r .[0].Id | tr ':' '_' \
      | xargs printf 'ghcr.io/miniwdl-ext/miniwdl-tools:Id_%s')
docker tag miniwdl-tools:latest $TAG
docker push $TAG
echo $TAG
```

This tags the image based on its content-digest "Image ID" to help pulling the exact intended image. (The Image ID is *not* the "Repo Digest", another pull handle which however depends on the registry as well as the image content.)

Lastly, update references to this image in [default.cfg](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/config_templates/default.cfg) and run the test suite.
