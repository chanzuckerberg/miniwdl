# `miniwdl run` container runtimes

miniwdl's default Docker task runtime is recommended for production use. Users unable to employ the Docker daemon may configure miniwdl to use [Podman](https://podman.io/), [Singularity](https://sylabs.io/singularity/), or [udocker](https://indigo-dc.github.io/udocker/) instead (locally on Linux hosts only). Podman is the most compatible with Docker, but also requires root privileges. Singularity and udocker don't need to run as root, but impose a few other limitations.

## Podman (BETA)

Podman closely resembles Docker, to the extent that it's usually operated as root. If miniwdl is configured to use Podman (and isn't running as root itself), then it attempts to use `sudo podman` internally, which therefore must not require interactive password entry.

To configure miniwdl to use Podman instead of Docker:

1. Allow relevant user(s) to `sudo podman` without password entry using [this procedure](https://minikube.sigs.k8s.io/docs/drivers/podman/#known-issues).
2. Run the following command to sanity-check Podman: `sudo -kn podman run ubuntu:20.04 cat /etc/issue`
3. Set the environment variable `MINIWDL__SCHEDULER__CONTAINER_BACKEND=podman` or the equivalent [configuration file](https://miniwdl.readthedocs.io/en/latest/runner_reference.html#configuration) option `[scheduler] container_backend=podman`
4. Test the configuration with `miniwdl run_self_test`

Limitations:

* Without the aforementioned procedure to enable passwordless `sudo podman`, typical defaults require password entry after a five-minute timeout since the last `sudo` operation. This can cause confusion when test runs succeed because they turn over containers rapidly, but lengthy workflows fail in the middle due to this timeout.
  * Alternatives to the passwordless configuration include [increasing the timeout](https://unix.stackexchange.com/questions/382060/change-default-sudo-password-timeout), using a background script to [refresh it temporarily](https://serverfault.com/a/702019), or running miniwdl itself as root.
* Unlike with the Docker daemon, separate `miniwdl run` processes using the Podman runtime don't coordinate their CPU/memory reservations for container scheduling. Running multiple resource-intensive workflows concurrently (with separate `miniwdl run` invocations) is liable to overload the host.

## Singularity (BETA)

Once a system administrator [installs Singularity](https://sylabs.io/guides/master/admin-guide/installation.html), it's usually operated without root privileges. To configure miniwdl to use Singularity instead of Docker:

1. Run the following command to sanity-check Singularity: `singularity run --fakeroot docker://ubuntu:20.04 cat /etc/issue`
2. Set the environment variable `MINIWDL__SCHEDULER__CONTAINER_BACKEND=singularity` or the equivalent [configuration file](https://miniwdl.readthedocs.io/en/latest/runner_reference.html#configuration) option `[scheduler] container_backend=singularity`
3. Test the configuration with `miniwdl run_self_test`

Limitations:

* Inside a Singularity container, only the working directory and /tmp are writable, while the rest of the file system is read-only. Task commands that attempt to write elsewhere (e.g. installing software/libraries at runtime) will fail.
* Tasks' `runtime.docker` declarations are used as Docker (OCI) image tags with [Singularity's Docker image import features](https://sylabs.io/guides/2.6/user-guide/singularity_and_docker.html); SIF image files cannot yet be used directly.
* Task containers aren't actually restricted to use only their declared `runtime.cpu` and `runtime.memory` resources once they start, although those reservations are considered for parallel scheduling.
  * If parallel container scheduling causes problems, then it can be disabled (serializing the workflow) by setting the environment variable `MINIWDL__SCHEDULER__CALL_CONCURRENCY=1` or the equivalent configuration file option `[scheduler] call_concurrency=1`.
* Unlike with the Docker daemon, separate `miniwdl run` processes using the Singularity runtime don't coordinate their CPU/memory reservations for container scheduling. Running multiple resource-intensive workflows concurrently (with separate `miniwdl run` invocations) is liable to overload the host.

## udocker (BETA)

udocker typically doesn't require root privileges to operate nor even [install](https://indigo-dc.github.io/udocker/installation_manual.html), but it affects execution speed and provides less isolation between containers.

To configure miniwdl to use udocker:

1. Run the following command to sanity-check udocker: `udocker run ubuntu:20.04 cat /etc/issue`
2. Set the environment variable `MINIWDL__SCHEDULER__CONTAINER_BACKEND=udocker` or the equivalent [configuration file](https://miniwdl.readthedocs.io/en/latest/runner_reference.html#configuration) option `[scheduler] container_backend=udocker`
3. Test the configuration with `miniwdl run_self_test`

Limitations:

* Tasks running in udocker are able to overwrite their input files (unlike Docker which mounts them read-only). Doing so may have undefined effects on other tasks and the workflow as a whole.
* Task containers aren't actually restricted to use only their declared `runtime.cpu` and `runtime.memory` resources once they start, although those reservations are considered for parallel scheduling.
  * If parallel container scheduling causes problems, then it can be disabled (serializing the workflow) by setting the environment variable `MINIWDL__SCHEDULER__CALL_CONCURRENCY=1` or the equivalent configuration file option `[scheduler] call_concurrency=1`.
* Unlike with the Docker daemon, separate `miniwdl run` processes using the udocker runtime don't coordinate their CPU/memory reservations for container scheduling. Running multiple resource-intensive workflows concurrently (with separate `miniwdl run` invocations) is liable to overload the host.

