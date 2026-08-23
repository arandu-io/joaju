# Skills

Procedures an assistant follows when working with this server.

They live in `.agents/skills/<name>/SKILL.md`, which is the path the coding
assistants read from — Cursor, Codex, Cline, Copilot, Gemini CLI, Amp, OpenCode,
Warp, Zed and the rest all look there. It is one directory rather than a file
per vendor, so a skill written once is read by whatever this project is being
written with.

Each file opens with frontmatter carrying a `name` and a `description`. The
description is what a tool reads to decide whether the skill is relevant, so it
names the situation you are in rather than the topic it covers.

There are two audiences here, and they want different things. Someone building a
feature mounts this server and writes policies for it; someone changing the
server writes the transport it stands on. The skills are split along that line.

| skill | when it fires |
| --- | --- |
| `joaju-server` | mounting the server in an application, or running the process, and publishing an event to whoever is listening |
| `joaju-channel-policy` | deciding who may hold a socket and who may hear a channel — the two Grants, and the reads that have no exemption |
| `joaju-browser-client` | the page half: serving the script, connecting, subscribing, and the endpoint that authorizes a private channel |
| `joaju-transport` | changing anything under `ws/` — frames, the handshake, close codes, UTF-8, conformance |

## Why these exist

A WebSocket server is a shape a model already has an answer for, and the answer
is somebody else's library, a Node gateway, an npm client and a middleware that
authorizes. None of those is here, and each was refused for a reason that is
written down. `AGENTS.md` at the root lists what each of them maps to.

The rest of the answer is that this repository is built to be checked rather
than trusted. The gates are four commands, CI adds the ones a laptop cannot run
— a dependency in the graph, an attribution file under `ws/`, a `package.json`
anywhere — and the conformance report says on its first line whether it still
describes the code. An assistant that runs those is not guessing.

## Adding your own

A skill in this directory travels with the repository. Keep it a procedure
rather than a description: a file that says "read the documentation" never
changes what anybody does. Every command in one has to run, and every number in
one has to have been measured.
