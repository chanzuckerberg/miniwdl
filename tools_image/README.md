# miniwdl/tools_image

This subdirectory is the recipe for a Docker image bundling tools that miniwdl uses to provide certain functionality. End-users shouldn't deal with this image; it's used internally by miniwdl and does *not* include miniwdl itself.

For example, the image bundles [aria2c](https://aria2.github.io/), which miniwdl uses to download large input files supplied as URLs (without requiring end-users to install extra OS packages). Miniwdl synthesizes a WDL task with this image, which inputs the URL and outputs the desired file. The image is served publicly from GitHub Container Registry, referenced in the miniwdl configuration defaults (where it can be overridden if necessary).

This image doesn't change often, so we build it manually. First, authenticate your local `docker` CLI to GitHub Container Registry ([instructions](https://docs.github.com/en/packages/guides/pushing-and-pulling-docker-images#authenticating-to-github-container-registry)) using a Personal Access Token *with SSO enabled* for the `chanzuckerberg` organization. Then,

```
docker build --no-cache -t miniwdl_tools:latest tools_image/
TAG=$(docker inspect miniwdl_tools:latest | jq -r .[0].Id | tr ':' '_' \
      | xargs printf 'ghcr.io/chanzuckerberg/miniwdl_tools:Id_%s')
docker tag miniwdl_tools:latest $TAG
docker push $TAG
echo $TAG
```

This tags the image based on its content-digest "Image ID" to help pulling the exact intended image. (The Image ID is *not* the "Repo Digest", which is another way of achieving that.)

Lastly, update references to this image in [default.cfg](https://github.com/chanzuckerberg/miniwdl/blob/main/WDL/runtime/config_templates/default.cfg).
