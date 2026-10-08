# Shop workspace

The demo system of a course about working with Claude Code: a small shop split into services, each in its own repository. This repository ties them together. It holds the Kafka setup, the scripts that run everything, and the end-to-end tests.

You need git, [uv](https://docs.astral.sh/uv/) and Docker.

## Set up

Clone this repository, then from its directory:

    SHOP_GIT_BASE=<address shared by the shop repositories> ./setup.sh
    ./up.sh
    ./run-all.sh
    uv run pytest e2e

`SHOP_GIT_BASE` is the address of the service repositories without the repository name. `setup.sh` clones `contracts`, `orders-service`, `inventory-service` and `payments-service` into this directory.

Stop everything with `./stop-all.sh` and `docker compose down`.

## Follow a lesson

    ./checkout-lesson.sh 03

puts every repository on a branch `lesson-03` that starts where lesson 3 starts. It refuses if you have uncommitted changes. `./checkout-lesson.sh 03 --discard` throws them away instead.

What runs where is in `CLAUDE.md`.
