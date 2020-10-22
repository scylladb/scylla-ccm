# Docker support for CCM - 2020 dev-summit hackathon

## Goals/Reasons
* Be able to run ccm ontop of docker images from:
  * https://hub.docker.com/r/scylladb/scylla/
  * https://hub.docker.com/r/scylladb/scylla-nightly/
* Relocatable package/s is available, but 
  * Tend to have issues around changes in installation process (install.sh, directory structure changes) 
  * Lots of games and tricks to get paths correctly
  * Lots of handling of resources allocation, like running instances after a faulty run.
* remove dependencies from the test running machines - i.e. java version as example
* Handling networking resources, running tests in parallel without giving them "ids"
* depend only on scylla control of CPU/memory via --smp and such. 
  we can use docker facilities to distribute the CPUs shares

## What we want to support
* Using it via command line:
```bash
ccm create scylla-docker-cluster -n 3 --scylla --docker scylladb/scylla:4.1.8
```

* Using it from dtest:
```bash
SCYLLA_DOCKER_IMAGE=scylladb/scylla:latest
nosetest ...
```
* Including calling tools like cassandra-stress or scyllatop (should be part of the image)
* Running manager agents, and server
* Integration tests - to keep it from breaking
* Running CI for PRs - which we don't have now

## Open questions

Yaron, Fabio, Israel 
## Cluster/Node implementation 
* should we use docker command line ? or using a python package for that ?
* how we change configuration ? mount files ? in place changes/replaces ?
* from where do we collect logs ? from specific files ? using docker logs ?
* how we collect all the relevant docker instances, to cleanup the test ?

Alex.B, Shlomo
### Test
* unittest - 
* integration test - pull a docker, like latest, crate

Oren,
### CI 
* which CI we should use - our own jenkins ? travis/circle-ci/github actions ?

### other/extra tools
* cassandra-stress - is it part of the official docker ? should we run it in a separate docker ? 
* where we install run manager server ? do use the official docker of manager ?
* how we support manager-agent in the docker nodes.

## How we would work

* We'll start a branch called `docker-ccm` on scylla-ccm (open-source), 
  and open PRs into that branch.
