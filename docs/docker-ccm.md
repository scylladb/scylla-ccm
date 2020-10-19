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
* should we use docker command line ? or using a python package for that ?
* where we install run manager server ?
* how we change configuration ? mount files ? in place changes/replaces ?
* how we collect all the relevant docker instances, to cleanup the test  
* from where do we collect logs ? from specific files ? using docker logs ?
* which CI we should use - our own jenkins ? travis/circle-ci/github actions ?

## How we would work

* We'll start a branch called `docker-ccm` on scylla-ccm (open-source), 
  and open PRs into that branch.
