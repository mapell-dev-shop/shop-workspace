# Shop workspace

The demo system of a course about working with Claude Code: a small shop split into services, each in its own repository. This repository ties them together. It holds the Kafka setup, the scripts that run everything, and the end-to-end tests.

You need git, [uv](https://docs.astral.sh/uv/) and Docker.

## Set up

    git clone https://github.com/mapell-dev-shop/shop-workspace.git
    cd shop-workspace
    ./setup.sh
    ./up.sh
    ./run-all.sh
    uv run pytest e2e

`setup.sh` clones `contracts`, `orders-service`, `inventory-service` and `payments-service` from github.com/mapell-dev-shop into this directory. To work on your own forks, set `SHOP_GIT_BASE` to your account's address, for example `SHOP_GIT_BASE=https://github.com/your-account ./setup.sh`.

Stop everything with `./stop-all.sh` and `docker compose down`.

## Follow a lesson

    ./checkout-lesson.sh 03

puts every repository on a branch `lesson-03` that starts where lesson 3 starts. It refuses if you have uncommitted changes. `./checkout-lesson.sh 03 --discard` throws them away instead. `./checkout-lesson.sh 03 --end` goes to where lesson 3 ends, for when you want to skip ahead or a demo went wrong.

Every checkout also deletes the services' SQLite databases: an earlier lesson's code cannot open a database that a later lesson migrated. `./run-all.sh` creates them again.

## Claude Code plugin

`plugins/shop-team` holds the team's shared Claude Code setup: the `new-consumer` skill, the `reviewer` agent, and hooks that run ruff format on every edited Python file, mypy when Claude stops, and block destructive commands. This repository is its marketplace (`.claude-plugin/marketplace.json`). Install it with

    /plugin install shop-team --marketplace mapell-dev-shop/shop-workspace

or, from a local clone, `/plugin marketplace add ./` and then `/plugin install shop-team@shop-workspace`.

The hooks work in a session started here or in any service. A file is formatted with the ruff of the repository it belongs to, and mypy checks the session's repository plus, from here, every service with uncommitted changes under `src`. A repository without ruff, mypy or `src` is skipped.

What runs where is in `CLAUDE.md`.
